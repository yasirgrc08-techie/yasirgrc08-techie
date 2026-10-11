import { test } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import fsp from 'node:fs/promises';
import path from 'node:path';
import {
  CHUNK,
  tempDir,
  startServer,
  randomBytes,
  sha256,
  json,
  createUpload,
  putChunk,
  chunkOf,
} from './helpers.js';

async function uploadAll(base, id, data, skip = new Set()) {
  const total = Math.ceil(data.length / CHUNK);
  for (let i = 0; i < total; i++) {
    if (skip.has(i)) continue;
    const r = await putChunk(base, id, i, chunkOf(data, i));
    assert.ok([200, 201].includes(r.status), `chunk ${i}: ${JSON.stringify(r.body)}`);
  }
}

test('chunked upload, completion and ranged download', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(CHUNK * 5 + 123);

  const { status, body } = await createUpload(base, { fileSize: data.length });
  assert.equal(status, 201);
  assert.equal(body.totalChunks, 6);
  await uploadAll(base, body.uploadId, data);

  const done = await fetch(`${base}/api/uploads/${body.uploadId}/complete`, { method: 'POST' });
  assert.equal(done.status, 200);
  const file = await json(done);

  const dl = await fetch(base + file.url);
  assert.equal(dl.status, 200);
  assert.equal(sha256(Buffer.from(await dl.arrayBuffer())), sha256(data));

  const part = await fetch(base + file.url, { headers: { Range: 'bytes=100-199' } });
  assert.equal(part.status, 206);
  assert.equal(part.headers.get('content-range'), `bytes 100-199/${data.length}`);
  assert.deepEqual(Buffer.from(await part.arrayBuffer()), data.subarray(100, 200));

  const list = await json(await fetch(`${base}/api/files`));
  assert.equal(list.files.length, 1);
});

test('duplicate create with the same idempotency key returns the same upload', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const [a, b] = await Promise.all([
    createUpload(base, { fileSize: CHUNK * 3, key: 'k1' }),
    createUpload(base, { fileSize: CHUNK * 3, key: 'k1' }),
  ]);
  assert.equal(a.body.uploadId, b.body.uploadId);
  assert.deepEqual([a.status, b.status].sort(), [200, 201]);

  const c = await createUpload(base, { fileSize: CHUNK * 4, key: 'k1' });
  assert.equal(c.status, 422, 'same key for a different file is rejected');
});

test('duplicate chunks are idempotent; conflicting or corrupted chunks are rejected', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(CHUNK * 2);
  const { body } = await createUpload(base, { fileSize: data.length });
  const id = body.uploadId;

  assert.equal((await putChunk(base, id, 0, chunkOf(data, 0))).status, 201);
  const dup = await putChunk(base, id, 0, chunkOf(data, 0));
  assert.equal(dup.status, 200);
  assert.equal(dup.body.duplicate, true);

  // Concurrent duplicates of a new chunk: exactly one write, both succeed.
  const both = await Promise.all([putChunk(base, id, 1, chunkOf(data, 1)), putChunk(base, id, 1, chunkOf(data, 1))]);
  assert.deepEqual(both.map((r) => r.status).sort(), [200, 201]);

  const other = randomBytes(CHUNK);
  assert.equal((await putChunk(base, id, 0, other)).status, 409, 'different content for a stored chunk');

  const { body: b2 } = await createUpload(base, { fileSize: data.length, fileName: 'b.bin' });
  const corrupted = await putChunk(base, b2.uploadId, 0, chunkOf(data, 0), sha256(other));
  assert.equal(corrupted.status, 422, 'checksum mismatch');

  const short = await putChunk(base, b2.uploadId, 0, chunkOf(data, 0).subarray(10));
  assert.equal(short.status, 400, 'wrong length');

  const bad = await putChunk(base, b2.uploadId, 99, chunkOf(data, 0));
  assert.equal(bad.status, 400, 'index out of range');

  const journal = await fsp.readFile(path.join(app.store.uploadsDir, id, 'journal.log'), 'utf8');
  assert.equal(journal.trim().split('\n').length, 2, 'each chunk journaled once');
});

test('completing an incomplete upload lists missing chunks; completing twice is idempotent', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(CHUNK * 4);
  const { body } = await createUpload(base, { fileSize: data.length });
  await uploadAll(base, body.uploadId, data, new Set([1, 3]));

  const res = await fetch(`${base}/api/uploads/${body.uploadId}/complete`, { method: 'POST' });
  assert.equal(res.status, 409);
  assert.deepEqual((await json(res)).missingChunks, [1, 3]);

  await putChunk(base, body.uploadId, 1, chunkOf(data, 1));
  await putChunk(base, body.uploadId, 3, chunkOf(data, 3));
  const [x, y] = await Promise.all([
    fetch(`${base}/api/uploads/${body.uploadId}/complete`, { method: 'POST' }).then(json),
    fetch(`${base}/api/uploads/${body.uploadId}/complete`, { method: 'POST' }).then(json),
  ]);
  assert.equal(x.id, y.id);
  const again = await json(await fetch(`${base}/api/uploads/${body.uploadId}/complete`, { method: 'POST' }));
  assert.equal(again.id, x.id);

  // A late duplicate chunk after completion is still acknowledged.
  assert.equal((await putChunk(base, body.uploadId, 0, chunkOf(data, 0))).status, 200);
});

test('server restart keeps acknowledged chunks', async () => {
  const dir = await tempDir();
  let { app, base, port } = await startServer(dir);
  const data = randomBytes(CHUNK * 6 + 7);
  const { body } = await createUpload(base, { fileSize: data.length, key: 'restart' });
  await uploadAll(base, body.uploadId, data, new Set([4, 5, 6]));
  await app.close();

  ({ app, base } = await startServer(dir, { port }));
  try {
    const status = await json(await fetch(`${base}/api/uploads/${body.uploadId}`));
    assert.deepEqual(status.receivedChunks, [0, 1, 2, 3]);
    const again = await createUpload(base, { fileSize: data.length, key: 'restart' });
    assert.equal(again.body.uploadId, body.uploadId, 'idempotency keys survive restart');

    for (const i of [4, 5, 6]) await putChunk(base, body.uploadId, i, chunkOf(data, i));
    const file = await json(await fetch(`${base}/api/uploads/${body.uploadId}/complete`, { method: 'POST' }));
    const dl = Buffer.from(await (await fetch(base + file.url)).arrayBuffer());
    assert.equal(sha256(dl), sha256(data));
  } finally {
    await app.close();
  }
});

test('crash during finalization and a torn journal line are recovered on startup', async () => {
  const dir = await tempDir();
  let { app, base } = await startServer(dir);
  const data = randomBytes(CHUNK * 2);
  const { body } = await createUpload(base, { fileSize: data.length });
  const id = body.uploadId;
  await uploadAll(base, id, data);
  await app.close();

  // Simulate: crash right after meta.json was switched to "completing",
  // plus a half-written journal line.
  const metaPath = path.join(dir, 'uploads', id, 'meta.json');
  const meta = JSON.parse(await fsp.readFile(metaPath, 'utf8'));
  meta.state = 'completing';
  await fsp.writeFile(metaPath, JSON.stringify(meta));
  await fsp.appendFile(path.join(dir, 'uploads', id, 'journal.log'), '1 abc');

  ({ app, base } = await startServer(dir));
  try {
    const status = await json(await fetch(`${base}/api/uploads/${id}`));
    assert.equal(status.state, 'complete');
    const dl = Buffer.from(await (await fetch(`${base}/files/${id}`)).arrayBuffer());
    assert.equal(sha256(dl), sha256(data));
  } finally {
    await app.close();
  }
});

test('client disconnecting mid-chunk leaves nothing recorded', async (t) => {
  const { app, base, port } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(CHUNK * 2);
  const { body } = await createUpload(base, { fileSize: data.length });

  await new Promise((resolve) => {
    const sock = net.connect(port, '127.0.0.1', () => {
      sock.write(
        `PUT /api/uploads/${body.uploadId}/chunks/0 HTTP/1.1\r\nHost: x\r\nContent-Length: ${CHUNK}\r\n\r\n`,
      );
      sock.write(chunkOf(data, 0).subarray(0, CHUNK / 2));
      setTimeout(() => {
        sock.destroy();
        resolve();
      }, 50);
    });
  });
  await new Promise((r) => setTimeout(r, 50));

  const status = await json(await fetch(`${base}/api/uploads/${body.uploadId}`));
  assert.deepEqual(status.receivedChunks, []);
  assert.equal((await putChunk(base, body.uploadId, 0, chunkOf(data, 0))).status, 201);
});

test('idle uploads expire', async (t) => {
  const { app, base } = await startServer(await tempDir(), { uploadTtlMs: 1000 });
  t.after(() => app.close());
  const { body } = await createUpload(base, { fileSize: CHUNK });
  await app.store.sweep(Date.now() + 5000);
  assert.equal((await fetch(`${base}/api/uploads/${body.uploadId}`)).status, 404);
  await assert.rejects(fsp.access(path.join(app.store.uploadsDir, body.uploadId)));
});

test('rejects files over the size limit', async (t) => {
  const { app, base } = await startServer(await tempDir(), { maxFileSize: CHUNK * 2 });
  t.after(() => app.close());
  const r = await createUpload(base, { fileSize: CHUNK * 3 });
  assert.equal(r.status, 413);
});
