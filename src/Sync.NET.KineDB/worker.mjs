import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createInterface } from 'node:readline';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [packageDirectory, directory, name, schemaId, endpoint, user, scopeKey] = process.argv.slice(2);
if (scopeKey !== undefined && !/^[A-Za-z0-9_-]{1,128}$/.test(scopeKey)) {
    throw new Error('Invalid replica scope key.');
}
const predicate = scopeKey === undefined ? undefined : `id = '${scopeKey}'`;
const revision = '3c2ace4d54857df44bf160fb71ddb2cb484d1009';
const table = /^[a-z][a-z0-9_]{0,62}$/.test(name) && !name.startsWith('syncnet_') && !name.startsWith('kinedb_')
    ? '"' + name + '"' : null;
if (!table) throw new Error('Invalid collection name.');
const manifest = JSON.parse(await readFile(join(packageDirectory, 'package.json'), 'utf8'));
const wasm = await readFile(join(packageDirectory, 'src', 'kinedb.wasm'));
if (manifest.name !== '@kinedb/embedded' || manifest.version !== '0.0.0-g3c2ace4d5485' ||
    createHash('sha256').update(wasm).digest('hex') !== 'a8a30778d607fd524a0af4893ca100062af173f37c0707f0341ceb62b117bf81') {
    throw new Error('The KineDB package does not match the Phase 0 pin.');
}

const { KineDB } = await import(pathToFileURL(join(packageDirectory, 'src', 'index.js')));
const db = new KineDB('replica', { dir: directory, wasm, durability: 'commit', reconnect: false });
const identityParts = [name, schemaId, endpoint, user];
if (scopeKey !== undefined) identityParts.push(scopeKey);
const identity = JSON.stringify(identityParts);
const metadata = 'syncnet_replica';
await db.sql(`CREATE TABLE IF NOT EXISTS ${metadata} (id INT PRIMARY KEY, identity TEXT, ready INT)`);
const binding = (await db.sql(`SELECT identity, ready FROM ${metadata} WHERE id = 1`)).data[0];
if (binding && binding[0] !== identity) {
    throw new Error('This replica directory is bound to a different table, schema, server, or user.');
}
if (!binding) await db.sql(`INSERT INTO ${metadata} (id, identity, ready) VALUES (1, $1, 0)`, [identity]);
let ready = binding?.[1] === 1;

function requireReady() {
    if (!ready) {
        throw new Error('Synchronize once before reading or editing a new replica.');
    }
}

async function execute({ command, key, payload, password }) {
    const session = { url: endpoint, user, password };
    switch (command) {
        case 'check': {
            const health = new URL('/health', endpoint);
            health.protocol = health.protocol === 'wss:' ? 'https:' : 'http:';
            const response = await fetch(health, { signal: AbortSignal.timeout(10_000), redirect: 'error' });
            if (response.status === 429 || response.status >= 500) {
                throw Object.assign(new Error('The KineDB server is temporarily unavailable.'), { retryable: true });
            }
            if (!response.ok || (await response.json()).revision !== revision) {
                throw new Error('The server revision does not match the KineDB pin.');
            }
            return ready;
        }
        case 'get':
            requireReady();
            if (scopeKey !== undefined && key !== scopeKey) return [];
            return (await db.sql(`SELECT id, payload FROM ${table} WHERE id = $1`, [key])).data;
        case 'schema': {
            const connection = await db.connect(endpoint, { user, password });
            try {
                const description = await connection.describe(name);
                const { columns, primary_key: keyColumns } = description;
                const id = columns.find(column => column.name === 'id');
                const data = columns.find(column => column.name === 'payload');
                if (columns.length !== 2 || keyColumns.length !== 1 || keyColumns[0] !== 'id' ||
                    id?.type.toLowerCase() !== 'text' || data?.type.toLowerCase() !== 'text' || !data.not_null) {
                    throw new Error('The KineDB table must have a TEXT id primary key and a non-null TEXT payload.');
                }
                return null;
            } finally {
                await connection.close();
            }
        }
        case 'list':
            requireReady();
            if (scopeKey !== undefined)
                return (await db.sql(`SELECT id, payload FROM ${table} WHERE id = $1`, [scopeKey])).data;
            return (await db.sql(`SELECT id, payload FROM ${table} ORDER BY id`)).data;
        case 'put':
            requireReady();
            await db.sql(`INSERT INTO ${table} (id, payload) VALUES ($1, $2)`, [key, payload]);
            return null;
        case 'delete': {
            requireReady();
            const result = await db.sql(`DELETE FROM ${table} WHERE id = $1`, [key]);
            return result.count > 0;
        }
        case 'push':
            requireReady();
            return await db.push(name, session);
        case 'refused':
            requireReady();
            return await db.refused(name, session);
        case 'pull': {
            const result = await db.sync(name, { ...session, predicate, live: false, pack: false });
            if (!ready && !result.gap) {
                await db.sql(`SELECT id, payload FROM ${table} LIMIT 1`);
                await db.sql(`UPDATE ${metadata} SET ready = 1 WHERE id = 1`);
                ready = true;
            }
            return result;
        }
        default:
            throw new Error('Unknown worker command.');
    }
}

async function reply(request) {
    let response;
    try {
        response = { id: request.id, value: await execute(request) };
    } catch (error) {
        // Avoid returning database errors that can contain credentials or row contents.
        const configuration = error.message === 'The server revision does not match the KineDB pin.' ||
            error.message === 'The KineDB table must have a TEXT id primary key and a non-null TEXT payload.';
        const notReady = error.message === 'Synchronize once before reading or editing a new replica.';
        response = { id: request.id, error: configuration || notReady ? error.message : 'KineDB operation failed.',
            retryable: error.retryable === true || error.name === 'TimeoutError' || error.cause?.code === 'ECONNREFUSED',
            outcomeUnknown: error.outcomeUnknown === true, configuration };
    }
    await new Promise((resolve, reject) => {
        process.stdout.write(JSON.stringify(response) + '\n', error => error ? reject(error) : resolve());
    });
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
const active = new Set();
for await (const line of lines) {
    const request = JSON.parse(line);
    const pending = reply(request);
    active.add(pending);
    pending.then(() => active.delete(pending), () => process.exit(1));
}
await Promise.all(active);
await db.close();
