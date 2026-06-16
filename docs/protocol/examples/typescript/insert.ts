/**
 * Minimal example: insert an outbox row from TypeScript (node-postgres).
 *
 * Demonstrates the SQL-only integration path documented in
 * docs/protocol/insert.sql. No outbox-specific SDK; node-postgres + a
 * UUIDv7 library are the only runtime dependencies.
 *
 * Run against the lib's test compose stack:
 *
 *   DATABASE_URL='postgres://outbox:outbox@localhost:5434/outbox' \
 *     ts-node insert.ts
 *
 * Dependencies:
 *   npm i pg uuid
 *   npm i -D @types/pg @types/node
 *
 * Requires `uuid` >= 9.0.0 (which exports `v7`).
 */
import { Pool, PoolClient } from 'pg';
import { v7 as uuidv7 } from 'uuid';

const INSERT_SQL = `
  INSERT INTO outbox.messages
    (event_id, address, data, headers, ordering_key, retry_limit)
  VALUES
    ($1, $2, $3, $4, $5, $6)
`;

interface EmitOptions {
    address: string;
    payload: Buffer;
    orderingKey?: string;
    headers?: Record<string, string>;
    retryLimit?: number;
}

/**
 * Insert an outbox row inside the caller's transaction.
 *
 * The caller is responsible for the transaction boundary. Typical usage:
 *
 *   const client = await pool.connect();
 *   try {
 *     await client.query('BEGIN');
 *     await client.query('UPDATE payments SET ...');
 *     await emitEvent(client, { address: 'payments.completed.v1', payload: ... });
 *     await client.query('COMMIT');
 *   } catch (err) {
 *     await client.query('ROLLBACK');
 *     throw err;
 *   } finally {
 *     client.release();
 *   }
 *
 * Returns the event_id (UUIDv7) so the caller can log/correlate it.
 */
export async function emitEvent(
    client: PoolClient,
    opts: EmitOptions,
): Promise<string> {
    const eventId = uuidv7();
    await client.query(INSERT_SQL, [
        eventId,
        opts.address,
        opts.payload,
        JSON.stringify(opts.headers ?? {}),
        opts.orderingKey ?? '',
        opts.retryLimit ?? 5,
    ]);
    return eventId;
}

async function main() {
    const pool = new Pool({ connectionString: process.env.DATABASE_URL });
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // In a real service, your domain write goes here in the same tx:
        //   await client.query("UPDATE payments SET status = 'completed' WHERE id = $1", [paymentId]);

        const eventId = await emitEvent(client, {
            address: 'example.smoke.v1',
            payload: Buffer.from(JSON.stringify({ hello: 'world' })),
            orderingKey: 'example-key',
            headers: { 'content-type': 'application/json' },
        });
        await client.query('COMMIT');
        console.log(`emitted event_id=${eventId}`);
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
        await pool.end();
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
