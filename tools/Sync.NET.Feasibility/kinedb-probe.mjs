import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [packageDirectory, dataDirectory, sql] = process.argv.slice(2);
const entry = pathToFileURL(join(packageDirectory, 'src', 'index.js'));
const { KineDB } = await import(entry.href);
const db = new KineDB('replica', { dir: dataDirectory, durability: 'commit' });
const results = [];

try {
    for (const statement of JSON.parse(sql)) {
        const result = await db.sql(statement);
        results.push(result.type === 'rows' ? result.data : null);
    }
} finally {
    await db.close();
}

console.log(JSON.stringify(results));
