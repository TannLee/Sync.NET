import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

assert.ok(process.argv[2], 'Pass the extracted KineDB package directory.');
const packageDir = resolve(process.argv[2]);
const manifest = JSON.parse(await readFile(join(packageDir, 'package.json'), 'utf8'));
assert.equal(manifest.name, '@kinedb/embedded');
assert.equal(manifest.version, '0.0.0-g3c2ace4d5485');
const wasm = await readFile(join(packageDir, 'src/kinedb.wasm'));
assert.equal(createHash('sha256').update(wasm).digest('hex'),
    'a8a30778d607fd524a0af4893ca100062af173f37c0707f0341ceb62b117bf81');
const endpoint = 'http://127.0.0.1:4820';
const password = process.env.KINEDB_ROOT_PASSWORD;
assert.ok(password, 'Set KINEDB_ROOT_PASSWORD before running the probe.');

const health = await fetch(endpoint + '/health');
assert.ok(health.ok);
assert.equal((await health.json()).revision, '3c2ace4d54857df44bf160fb71ddb2cb484d1009');
const login = await fetch(endpoint + '/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: 'root', password })
});
assert.ok(login.ok, 'Server login failed.');
const { token } = await login.json();
const session = { url: 'ws://127.0.0.1:4820/ws', token };
const pull = { ...session, live: false, pack: false };
let dropNextReply = false;
let droppedReplies = 0;

globalThis.WebSocket = class FaultInjectingWebSocket extends WebSocket {
    set onmessage(handler) {
        super.onmessage = event => {
            if (dropNextReply) {
                dropNextReply = false;
                droppedReplies++;
                this.close();
                return;
            }
            handler(event);
        };
    }
};

const { KineDB } = await import(pathToFileURL(join(packageDir, 'src/index.js')));
const table = 'phase0_' + randomBytes(8).toString('hex');
const query = `SELECT id, payload FROM ${table} ORDER BY id`;
const directory = await mkdtemp(join(tmpdir(), 'syncnet-replicas-'));
const open = name => new KineDB(name, { dir: directory, durability: 'commit', reconnect: false });

async function server(sql) {
    const response = await fetch(endpoint + '/sql', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` },
        body: sql
    });
    const body = await response.json();
    assert.ok(response.ok && body.type !== 'error', JSON.stringify(body));
    return body;
}

async function local(db, expected) {
    assert.deepEqual((await db.sql(query)).data, expected);
}

let a = open('a');
let b = open('b');
try {
    await server(`CREATE TABLE ${table} (id INT PRIMARY KEY, payload TEXT)`);
    await server(`INSERT INTO ${table} (id, payload) VALUES (1, 'base'), (2, 'delete_me')`);
    await a.sync(table, pull);
    await b.sync(table, pull);
    await local(a, [[1, 'base'], [2, 'delete_me']]);
    await local(b, [[1, 'base'], [2, 'delete_me']]);
    await a.close();
    a = open('a');
    await a.sql(`UPDATE ${table} SET payload = 'updated' WHERE id = 1`);
    await a.sql(`DELETE FROM ${table} WHERE id = 2`);
    await a.sql(`INSERT INTO ${table} (id, payload) VALUES (3, 'new')`);
    const expected = [[1, 'updated'], [3, 'new']];
    await local(a, expected);
    assert.deepEqual((await server(query)).data, [[1, 'base'], [2, 'delete_me']]);
    await a.close();
    a = open('a');
    await local(a, expected);
    const pushed = await a.push(table, session);
    assert.equal(pushed.applied, 3, JSON.stringify(pushed));
    assert.deepEqual((await server(query)).data, expected);
    const delta = await b.sync(table, pull);
    await local(b, expected);
    assert.equal(delta.full, false);
    assert.ok(delta.resumedFrom);
    console.log('PASS: initial pull, offline CUD, pending writes after reopen, push and delta pull', JSON.stringify(delta));

    await a.sync(table, pull);
    await a.close();
    await b.close();
    a = open('a');
    b = open('b');
    await a.sql(`UPDATE ${table} SET payload = 'winner' WHERE id = 1`);
    await b.sql(`UPDATE ${table} SET payload = 'stale' WHERE id = 1`);
    assert.equal((await a.push(table, session)).applied, 1);
    const stale = await b.push(table, session);
    assert.equal(stale.conflicts, 1, JSON.stringify(stale));
    assert.equal(stale.outcomes[0]?.status, 'conflict', JSON.stringify(stale));
    assert.deepEqual((await server(query)).data, [[1, 'winner'], [3, 'new']]);
    const refused = await b.refused(table);
    assert.ok(refused.some(item => item.code === 'conflict'), JSON.stringify(refused));
    console.log('PASS: stale write reports conflict and server preserves winning write', JSON.stringify(stale.outcomes));

    // Establish the sync socket before arming the fault. With live pulls off,
    // the next incoming frame on this socket follows the single queued write.
    assert.equal((await a.push(table, session)).sent, 0);
    await a.sql(`INSERT INTO ${table} (id, payload) VALUES (4, 'lost_ack')`);
    dropNextReply = true;
    await assert.rejects(a.push(table, session), /session .* closed/);
    assert.equal(droppedReplies, 1);
    const committed = [[1, 'winner'], [3, 'new'], [4, 'lost_ack']];
    assert.deepEqual((await server(query)).data, committed);
    const beforeRetry = await b.sync(table, pull);
    await local(b, committed);
    await a.close();
    a = open('a');
    const retry = await a.push(table, session);
    assert.equal(retry.sent, 1, JSON.stringify(retry));
    assert.equal(retry.outcomes[0]?.status, 'applied', JSON.stringify(retry));
    assert.equal(retry.outcomes[0]?.replayed, true, JSON.stringify(retry));
    const afterRetry = await b.sync(table, pull);
    assert.equal(afterRetry.resumedFrom, beforeRetry.token);
    assert.equal(afterRetry.upserts, 0);
    assert.equal(afterRetry.removes, 0);
    assert.deepEqual((await server(query)).data, committed);
    await local(b, committed);
    console.log('PASS: lost ACK, committed row before retry, replayed outcome after reopen and no additional delta',
        JSON.stringify(retry.outcomes));
} finally {
    await a.close();
    await b.close();
    await server(`DROP TABLE IF EXISTS ${table}`);
    await rm(directory, { recursive: true, force: true });
}
