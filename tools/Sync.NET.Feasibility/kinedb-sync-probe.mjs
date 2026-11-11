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
const port = Number(process.env.SYNCNET_KINEDB_PORT ?? 4820);
assert.ok(Number.isInteger(port) && port > 0 && port < 65536, 'Invalid local server port.');
const endpoint = `http://127.0.0.1:${port}`;
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
const session = { url: `ws://127.0.0.1:${port}/ws`, token };
const pull = { ...session, live: false, pack: false };
let dropNextReply = false;
let dropReplyContaining;
let matchingRepliesToDeliver = 0;
let droppedReplies = 0;

globalThis.WebSocket = class FaultInjectingWebSocket extends WebSocket {
    set onmessage(handler) {
        super.onmessage = event => {
            const containsMarker = dropReplyContaining && event.data instanceof ArrayBuffer &&
                Buffer.from(event.data).includes(dropReplyContaining);
            if (containsMarker && matchingRepliesToDeliver > 0) {
                matchingRepliesToDeliver--;
                handler(event);
                return;
            }
            if (dropNextReply || containsMarker) {
                dropReplyContaining = undefined;
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
assert.ok(!mode || mode === 'retention' || mode === 'paged' || (['lose-ack', 'restart-prepare', 'restart-verify'].includes(mode) && childDirectory && /^phase0_[a-f0-9]{16}$/.test(childTable)),
    'Invalid replay worker arguments.');
const table = childTable ?? ('phase0_' + randomBytes(8).toString('hex'));
const query = `SELECT id, payload FROM ${table} ORDER BY id`;
const directory = childDirectory ?? await mkdtemp(join(tmpdir(), 'syncnet-replicas-'));
const open = name => new KineDB(name, { dir: directory, durability: 'commit', reconnect: false });

async function requestSql(sql) {
    const response = await fetch(endpoint + '/sql', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` },
        body: sql,
        signal: AbortSignal.timeout(15_000)
    });
    const body = await response.json();
    assert.ok(response.ok || body.type === 'error', `HTTP ${response.status}`);
    return body;
}

async function server(sql) {
    const body = await requestSql(sql);
    assert.ok(body.type !== 'error', JSON.stringify(body));
    return body;
}

async function local(db, expected) {
    assert.deepEqual((await db.sql(query)).data, expected);
}

async function waitFor(predicate) {
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
        if (await predicate()) return;
        await new Promise(resolve => setTimeout(resolve, 25));
    }
    assert.fail('Timed out waiting for the live subscription.');
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
    const relatedQuery = `SELECT id, parent_id, payload FROM ${related} ORDER BY id`;
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
        await killReplayWorker();
        const committed = [[1, 'winner'], [3, 'new'], [4, 'lost_ack'], [5, 'independent']];
        assert.deepEqual((await server(query)).data, committed);
        const beforeRetry = await b.sync(table, pull);
        await local(b, committed);
        a = open('a');
        const replayDelay = Number(process.env.SYNCNET_KINEDB_REPLAY_DELAY_SECONDS ?? 0);
        assert.ok(Number.isInteger(replayDelay) && replayDelay >= 0 && replayDelay <= 3600);
        if (replayDelay) await new Promise(resolve => setTimeout(resolve, replayDelay * 1_000));
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
            JSON.stringify({ delaySeconds: replayDelay, outcomes: retry.outcomes }));

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

        await server(`CREATE TABLE ${related} (id INT PRIMARY KEY, parent_id INT REFERENCES ${table}(id), payload TEXT)`);
        await a.sync(related, pull);
        await b.sync(related, pull);
        await b.sync(table, pull);
        await b.close();
        b = open('b');
        await b.transaction([
            `INSERT INTO ${related} (id, parent_id, payload) VALUES (1, 1, 'cross_table_stale')`,
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
            `INSERT INTO ${table} (id, payload) VALUES (9, 'cross_table_ok')`,
            `INSERT INTO ${related} (id, parent_id, payload) VALUES (2, 9, 'cross_table_ok')`
        ]);
        const crossAccepted = await a.push(related, session);
        assert.equal(crossAccepted.sent, 2, JSON.stringify(crossAccepted));
        assert.equal(crossAccepted.applied, 2, JSON.stringify(crossAccepted));
        assert.equal(crossAccepted.outcomes.length, 2);
        assert.ok(crossAccepted.outcomes[0].version);
        assert.equal(crossAccepted.outcomes[0].version, crossAccepted.outcomes[1].version);
        assert.deepEqual((await server(relatedQuery)).data, [[2, 9, 'cross_table_ok']]);
        assert.deepEqual((await server(query)).data.at(-1), [9, 'cross_table_ok']);
        assert.equal((await a.push(table, session)).sent, 0);
        console.log('PASS: one push applies both tables at the same version', JSON.stringify(crossAccepted.outcomes));

        await b.sync(table, pull);
        await b.sync(related, pull);
        await b.close();
        b = open('b');
        await b.transaction([
            `INSERT INTO ${table} (id, payload) VALUES (10, 'must_roll_back')`,
            `INSERT INTO ${related} (id, parent_id, payload) VALUES (3, 3, 'parent_deleted')`
        ]);
        assert.deepEqual((await b.sql(relatedQuery)).data.at(-1), [3, 3, 'parent_deleted']);
        await server(`DELETE FROM ${table} WHERE id = 3`);
        const missingParent = await b.push(table, session);
        assert.equal(missingParent.sent, 2, JSON.stringify(missingParent));
        assert.equal(missingParent.applied, 0, JSON.stringify(missingParent));
        assert.deepEqual(missingParent.outcomes.map(outcome => outcome.status), ['rejected', 'rejected']);
        assert.ok((await b.refused(related, session)).some(entry => entry.code === 'fk_parent_missing'),
            JSON.stringify(missingParent.outcomes));
        assert.deepEqual((await server(relatedQuery)).data, [[2, 9, 'cross_table_ok']]);
        assert.deepEqual((await server(query)).data.filter(([id]) => id === 3 || id === 10), []);
        console.log('PASS: missing remote parent refuses the entire group', JSON.stringify(missingParent.outcomes));

        await a.sync(table, pull);
        const checkpoint = await a.freshness(table, session);
        const rowsBeforePull = (await a.sql(query)).data;
        const pullMarker = 'interrupted_pull_' + randomBytes(8).toString('hex');
        await server(`UPDATE ${table} SET payload = '${pullMarker}' WHERE id = 1`);
        await server(`DELETE FROM ${table} WHERE id = 7`);
        const rowsAfterPull = (await server(query)).data;

        const dropsBeforePull = droppedReplies;
        dropReplyContaining = Buffer.from(pullMarker);
        await assert.rejects(a.sync(table, pull), /session .* closed/);
        assert.equal(droppedReplies, dropsBeforePull + 1);
        await a.close();
        a = open('a');
        assert.equal((await a.freshness(table, session)).token, checkpoint.token);
        await local(a, rowsBeforePull);

        const resumedPull = await a.sync(table, pull);
        assert.equal(resumedPull.resumedFrom, checkpoint.token);
        assert.equal(resumedPull.full, false);
        assert.equal(resumedPull.gap, null);
        assert.equal(resumedPull.upserts, 1);
        assert.equal(resumedPull.removes, 1);
        await local(a, rowsAfterPull);
        await a.close();
        a = open('a');
        assert.equal((await a.freshness(table, session)).token, resumedPull.token);
        await local(a, rowsAfterPull);
        console.log('PASS: interrupted pull preserves the old rows and checkpoint, then resumes the delta',
            JSON.stringify(resumedPull));

        // Keep one subscription open across two server commits. Deliver the
        // first live frame and drop the next before it reaches the engine.
        await a.sync(table, { ...pull, live: true });
        const firstMarker = 'live_first_' + randomBytes(8).toString('hex');
        const secondMarker = 'live_second_' + randomBytes(8).toString('hex');
        await server(`UPDATE ${table} SET payload = '${firstMarker}' WHERE id = 1`);
        await waitFor(async () => (await a.sql(query)).data[0][1] === firstMarker);
        const liveCheckpoint = await a.freshness(table, session);
        const rowsAfterFirstFrame = (await a.sql(query)).data;
        assert.notEqual(liveCheckpoint.token, resumedPull.token);
        const dropsBeforeLive = droppedReplies;
        dropReplyContaining = Buffer.from(secondMarker);
        await server(`UPDATE ${table} SET payload = '${secondMarker}' WHERE id = 1`);
        await waitFor(() => droppedReplies === dropsBeforeLive + 1);
        await a.close();
        a = open('a');
        assert.equal((await a.freshness(table, session)).token, liveCheckpoint.token);
        await local(a, rowsAfterFirstFrame);
        const resumedLive = await a.sync(table, pull);
        assert.equal(resumedLive.resumedFrom, liveCheckpoint.token);
        assert.equal(resumedLive.full, false);
        assert.equal(resumedLive.upserts, 1);
        assert.deepEqual((await a.sql(query)).data, (await server(query)).data);
        console.log('PASS: live frame applied before disconnect survives reopen; next delta resumes from its token');

        await server(`UPDATE ${table} SET payload = 'in_scope' WHERE id = 1`);
        await server(`UPDATE ${table} SET payload = 'out_of_scope' WHERE id = 4`);
        let filtered = open('filtered');
        try {
            const scoped = { ...pull, predicate: "payload = 'in_scope'" };
            await filtered.sync(table, scoped);
            await local(filtered, [[1, 'in_scope']]);
            await filtered.close();
            filtered = open('filtered');
            const scopeCheckpoint = await filtered.freshness(table, session);
            await server(`UPDATE ${table} SET payload = 'out_of_scope' WHERE id = 1`);
            await server(`UPDATE ${table} SET payload = 'in_scope' WHERE id = 4`);
            const scopeDelta = await filtered.sync(table, scoped);
            assert.equal(scopeDelta.resumedFrom, scopeCheckpoint.token);
            assert.equal(scopeDelta.full, false);
            assert.equal(scopeDelta.upserts, 1);
            assert.equal(scopeDelta.removes, 1);
            await local(filtered, [[4, 'in_scope']]);
            await filtered.close();
            filtered = open('filtered');
            await local(filtered, [[4, 'in_scope']]);
            console.log('PASS: filtered delta adds the row entering scope and removes the row leaving scope');
        } finally {
            await filtered.close();
        }
    } finally {
        await a.close();
        await b.close();
        await server(`DROP TABLE IF EXISTS ${related}`);
        await server(`DROP TABLE IF EXISTS ${table}`);
    }
}

async function prepareServerRestart() {
    const replica = open('a');
    try {
        await server(`CREATE TABLE ${table} (id INT PRIMARY KEY, payload TEXT)`);
        await replica.sync(table, pull);
    } finally {
        await replica.close();
    }
    await killReplayWorker();
    await localServerRestartRows();
    console.log('PASS: server has committed the row and the client exited without receiving its ACK');
}

async function localServerRestartRows() {
    assert.deepEqual((await server(query)).data, [[4, 'lost_ack']]);
}

async function verifyServerRestart() {
    const replica = open('a');
    try {
        await localServerRestartRows();
        const replay = await replica.push(table, session);
        assert.equal(replay.sent, 1, JSON.stringify(replay));
        assert.equal(replay.outcomes.length, 1);
        assert.equal(replay.outcomes[0].status, 'applied');
        assert.equal(replay.outcomes[0].replayed, true);
        await localServerRestartRows();
        assert.equal((await replica.push(table, session)).sent, 0);
        console.log('PASS: replay survives server restart', JSON.stringify(replay.outcomes));
    } finally {
        await replica.close();
        await server(`DROP TABLE IF EXISTS ${table}`);
    }
}

async function killReplayWorker() {
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
}

async function runPagedPull() {
    const replica = open('paged');
    let reopened;
    const marker = 'paged_' + randomBytes(8).toString('hex');
    const payload = marker.padEnd(8192, 'x');
    const expected = Array.from({ length: 1200 }, (_, i) => [i + 1, payload]);
    try {
        await server(`CREATE TABLE ${table} (id INT PRIMARY KEY, payload TEXT)`);
        for (let offset = 0; offset < expected.length; offset += 16) {
            const values = expected.slice(offset, offset + 16).map(([id, value]) => `(${id}, '${value}')`);
            await server(`INSERT INTO ${table} (id, payload) VALUES ${values.join(', ')}`);
        }

        const before = droppedReplies;
        dropReplyContaining = Buffer.from(marker);
        matchingRepliesToDeliver = 1;
        await assert.rejects(replica.sync(table, pull), /session .* closed/);
        assert.equal(droppedReplies, before + 1, 'The second data page must be intercepted.');
        await replica.close();

        reopened = open('paged');
        const saved = await reopened.freshness(table, session);
        const resumed = await reopened.sync(table, pull);
        assert.equal(resumed.gap, null);
        if (saved.token) assert.equal(resumed.resumedFrom, saved.token);
        const actual = (await reopened.sql(query)).data;
        assert.equal(actual.length, expected.length);
        for (const [index, [id, value]] of actual.entries()) {
            assert.ok(id === index + 1 && value === payload, `Unexpected row at index ${index}`);
        }
        await reopened.close();
        reopened = open('paged');
        assert.equal((await reopened.freshness(table, session)).token, resumed.token);
        assert.equal((await reopened.sql(`SELECT COUNT(*) FROM ${table}`)).data[0][0], expected.length);
        console.log('PASS: one-shot pull interrupted on its second data page recovers every row and saves the final checkpoint');
    } finally {
        await (reopened ?? replica).close();
        await server(`DROP TABLE IF EXISTS ${table}`);
    }
}

async function runRetention() {
    let replica = open('retention');
    try {
        await server(`CREATE TABLE ${table} (id INT PRIMARY KEY, payload TEXT)`);
        await server(`INSERT INTO ${table} (id, payload) VALUES (1, 'old')`);
        const initial = await replica.sync(table, pull);
        await local(replica, [[1, 'old']]);
        assert.match(initial.token, /^[0-9a-f]{64}$/i);
        const historicalQuery = `SELECT id FROM ${table} AS OF '${initial.token}'`;
        assert.deepEqual((await server(historicalQuery)).data, [[1]]);
        await replica.close();

        await server(`INSERT INTO ${table} (id, payload) VALUES (2, 'current')`);
        await server(`DELETE FROM ${table} WHERE id = 1`);
        const expiresBy = Date.now() + 120_000;
        while (true) {
            const history = await requestSql(historicalQuery);
            if (history.type === 'error') {
                assert.match(history.message, /beyond retention horizon|retention floor/i);
                break;
            }
            assert.ok(Date.now() < expiresBy, 'The server still accepts the old checkpoint after 120 seconds.');
            await new Promise(resolve => setTimeout(resolve, 1_000));
        }

        replica = open('retention');
        assert.equal((await replica.freshness(table, session)).token, initial.token);
        const resumed = await replica.sync(table, pull);
        if (resumed.gap) {
            assert.equal(resumed.gap.reason, 'retention');
            const refill = await replica.sync(table, { ...pull, full: true });
            assert.equal(refill.gap, null);
        }
        await local(replica, [[2, 'current']]);
        const recovered = await replica.freshness(table, session);
        assert.notEqual(recovered.token, initial.token);
        await replica.close();
        replica = open('retention');
        assert.equal((await replica.freshness(table, session)).token, recovered.token);
        await local(replica, [[2, 'current']]);
        console.log('PASS: expired server checkpoint recovers the current rows and removes stale cached data');
    } finally {
        await replica.close();
        await server(`DROP TABLE IF EXISTS ${table}`);
    }
}

try {
    if (mode === 'lose-ack') await loseAcknowledgment();
    else if (mode === 'restart-prepare') await prepareServerRestart();
    else if (mode === 'restart-verify') await verifyServerRestart();
    else if (mode === 'retention') await runRetention();
    else if (mode === 'paged') await runPagedPull();
    else await runProbe();
} finally {
    if (!['lose-ack', 'restart-prepare'].includes(mode)) await rm(directory, { recursive: true, force: true });
}
