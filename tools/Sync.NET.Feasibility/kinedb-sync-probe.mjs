import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

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
const [mode, childDirectory, childTable] = process.argv.slice(3);
assert.ok(!mode || (mode === 'lose-ack' && childDirectory && /^phase0_[a-f0-9]{16}$/.test(childTable)),
    'Invalid replay worker arguments.');
const table = childTable ?? ('phase0_' + randomBytes(8).toString('hex'));
const query = `SELECT id, payload FROM ${table} ORDER BY id`;
const directory = childDirectory ?? await mkdtemp(join(tmpdir(), 'syncnet-replicas-'));
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

async function loseAcknowledgment() {
    const replica = open('a');
    try {
        assert.equal((await replica.push(table, session)).sent, 0);
        await replica.sql(`INSERT INTO ${table} (id, payload) VALUES (4, 'lost_ack')`);
        // The push session is established and has no live subscriptions.
        dropNextReply = true;
        await assert.rejects(replica.push(table, session), /session .* closed/);
        assert.equal(droppedReplies, 1);
        assert.ok(process.send, 'The replay worker needs an IPC channel.');
        process.send('ack-dropped');
        // Keep the database open until the parent terminates this process.
        await once(process, 'disconnect');
    } finally {
        await replica.close();
    }
}

async function runProbe() {
    const related = table + '_related';
    const relatedQuery = `SELECT id, payload FROM ${related} ORDER BY id`;
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
        await b.sql(`INSERT INTO ${table} (id, payload) VALUES (5, 'independent')`);
        assert.equal((await a.push(table, session)).applied, 1);
        const mixed = await b.push(table, session);
        assert.equal(mixed.sent, 2, JSON.stringify(mixed));
        assert.equal(mixed.applied, 1, JSON.stringify(mixed));
        assert.equal(mixed.conflicts, 1, JSON.stringify(mixed));
        assert.deepEqual(mixed.outcomes.map(outcome => outcome.status).sort(), ['applied', 'conflict']);
        assert.deepEqual((await server(query)).data, [[1, 'winner'], [3, 'new'], [5, 'independent']]);

        const refused = await b.refused(table);
        assert.equal(refused.length, 1, JSON.stringify(refused));
        assert.equal(refused[0].code, 'conflict');
        await b.close();
        b = open('b');
        assert.deepEqual(await b.refused(table, session), refused);
        assert.equal((await b.push(table, session)).sent, 0, 'No applied or refused operation should be resent.');
        console.log('PASS: mixed push applies the independent insert and preserves the refusal after reopen',
            JSON.stringify(mixed.outcomes));

        await a.close();
        const worker = fork(fileURLToPath(import.meta.url),
            [packageDir, 'lose-ack', directory, table],
            { timeout: 30_000, killSignal: 'SIGKILL' });
        let killedAfterAckDrop = false;
        worker.once('message', message => {
            if (message === 'ack-dropped')
                killedAfterAckDrop = worker.kill('SIGKILL');
        });
        const [exitCode, signal] = await once(worker, 'exit');
        assert.ok(killedAfterAckDrop, `Worker exited before confirming the dropped ACK: ${exitCode}, ${signal}`);
        assert.equal(signal, 'SIGKILL');
        const committed = [[1, 'winner'], [3, 'new'], [4, 'lost_ack'], [5, 'independent']];
        assert.deepEqual((await server(query)).data, committed);
        const beforeRetry = await b.sync(table, pull);
        await local(b, committed);
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
        console.log('PASS: lost ACK, committed row before retry, replayed outcome after client process kill and no additional delta',
            JSON.stringify(retry.outcomes));

        await a.sync(table, pull);
        const refusalsBeforeGroup = await b.refused(table, session);
        await b.close();
        b = open('b');
        await b.transaction([
            `INSERT INTO ${table} (id, payload) VALUES (6, 'must_roll_back')`,
            `UPDATE ${table} SET payload = 'group_stale' WHERE id = 1`
        ]);
        await local(b, [[1, 'group_stale'], ...committed.slice(1), [6, 'must_roll_back']]);
        await a.sql(`UPDATE ${table} SET payload = 'group_winner' WHERE id = 1`);
        assert.equal((await a.push(table, session)).applied, 1);

        const rejectedGroup = await b.push(table, session);
        assert.equal(rejectedGroup.sent, 2, JSON.stringify(rejectedGroup));
        assert.equal(rejectedGroup.applied, 0, JSON.stringify(rejectedGroup));
        assert.deepEqual(rejectedGroup.outcomes.map(outcome => outcome.status).sort(), ['conflict', 'rejected']);
        const groupWinner = [[1, 'group_winner'], ...committed.slice(1)];
        assert.deepEqual((await server(query)).data, groupWinner);
        const refusalsAfterGroup = await b.refused(table, session);
        assert.equal(refusalsAfterGroup.length, refusalsBeforeGroup.length + 2, JSON.stringify(refusalsAfterGroup));
        console.log('PASS: conflicted transaction applies neither member', JSON.stringify(rejectedGroup.outcomes));

        await a.transaction([
            `INSERT INTO ${table} (id, payload) VALUES (7, 'group_first')`,
            `INSERT INTO ${table} (id, payload) VALUES (8, 'group_second')`
        ]);
        const acceptedGroup = await a.push(table, session);
        assert.equal(acceptedGroup.sent, 2, JSON.stringify(acceptedGroup));
        assert.equal(acceptedGroup.applied, 2, JSON.stringify(acceptedGroup));
        assert.equal(acceptedGroup.outcomes.length, 2);
        assert.ok(acceptedGroup.outcomes[0].version);
        assert.equal(acceptedGroup.outcomes[0].version, acceptedGroup.outcomes[1].version);
        assert.deepEqual((await server(query)).data, [...groupWinner, [7, 'group_first'], [8, 'group_second']]);
        console.log('PASS: valid transaction applies both members', JSON.stringify(acceptedGroup.outcomes));

        await server(`CREATE TABLE ${related} (id INT PRIMARY KEY, payload TEXT)`);
        await a.sync(related, pull);
        await b.sync(related, pull);
        await b.sync(table, pull);
        await b.close();
        b = open('b');
        await b.transaction([
            `INSERT INTO ${related} (id, payload) VALUES (1, 'cross_table_stale')`,
            `UPDATE ${table} SET payload = 'cross_table_stale' WHERE id = 1`
        ]);
        await a.sql(`UPDATE ${table} SET payload = 'cross_table_winner' WHERE id = 1`);
        assert.equal((await a.push(table, session)).applied, 1);
        const crossRejected = await b.push(table, session);
        assert.equal(crossRejected.sent, 2, JSON.stringify(crossRejected));
        assert.equal(crossRejected.applied, 0, JSON.stringify(crossRejected));
        assert.deepEqual(crossRejected.outcomes.map(outcome => outcome.status).sort(), ['conflict', 'rejected']);
        assert.deepEqual((await server(relatedQuery)).data, []);
        assert.deepEqual((await server(query)).data[0], [1, 'cross_table_winner']);
        assert.ok((await b.refused(related, session)).some(entry => entry.code === 'rejected'));
        assert.equal((await b.push(related, session)).sent, 0);
        console.log('PASS: cross-table conflict refuses the whole group', JSON.stringify(crossRejected.outcomes));

        await a.transaction([
            `INSERT INTO ${related} (id, payload) VALUES (2, 'cross_table_ok')`,
            `INSERT INTO ${table} (id, payload) VALUES (9, 'cross_table_ok')`
        ]);
        const crossAccepted = await a.push(table, session);
        assert.equal(crossAccepted.sent, 2, JSON.stringify(crossAccepted));
        assert.equal(crossAccepted.applied, 2, JSON.stringify(crossAccepted));
        assert.equal(crossAccepted.outcomes.length, 2);
        assert.ok(crossAccepted.outcomes[0].version);
        assert.equal(crossAccepted.outcomes[0].version, crossAccepted.outcomes[1].version);
        assert.deepEqual((await server(relatedQuery)).data, [[2, 'cross_table_ok']]);
        assert.deepEqual((await server(query)).data.at(-1), [9, 'cross_table_ok']);
        assert.equal((await a.push(related, session)).sent, 0);
        console.log('PASS: one push applies both tables at the same version', JSON.stringify(crossAccepted.outcomes));
    } finally {
        await a.close();
        await b.close();
        await server(`DROP TABLE IF EXISTS ${related}`);
        await server(`DROP TABLE IF EXISTS ${table}`);
        await rm(directory, { recursive: true, force: true });
    }
}

if (mode === 'lose-ack') await loseAcknowledgment();
else await runProbe();
