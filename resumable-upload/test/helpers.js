import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { createApp } from '../server/app.js';

export const CHUNK = 64 * 1024;

export async function tempDir() {
  return fsp.mkdtemp(path.join(os.tmpdir(), 'resumable-upload-test-'));
}

export async function startServer(dataDir, opts = {}) {
  const app = await createApp({ dataDir, minChunkSize: 1024, ...opts });
  const port = await app.listen(opts.port ?? 0, '127.0.0.1');
  return { app, port, base: `http://127.0.0.1:${port}` };
}

export function randomBytes(n) {
  return crypto.randomBytes(n);
}

export function sha256(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

export function memorySource(buf, name = 'test.bin') {
  return {
    name,
    size: buf.length,
    lastModified: 1700000000000,
    type: 'application/octet-stream',
    async read(start, end) {
      return buf.subarray(start, end);
    },
  };
}

export async function json(res) {
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

export async function createUpload(base, { fileSize, chunkSize = CHUNK, fileName = 'a.bin', key } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (key) headers['Idempotency-Key'] = key;
  const res = await fetch(`${base}/api/uploads`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ fileName, fileSize, chunkSize }),
  });
  return { status: res.status, body: await json(res) };
}

export async function putChunk(base, id, index, bytes, hash = sha256(bytes)) {
  const headers = { 'Content-Type': 'application/octet-stream' };
  if (hash) headers['X-Chunk-Sha256'] = hash;
  const res = await fetch(`${base}/api/uploads/${id}/chunks/${index}`, { method: 'PUT', headers, body: bytes });
  return { status: res.status, body: await json(res) };
}

export function chunkOf(buf, index, chunkSize = CHUNK) {
  return buf.subarray(index * chunkSize, Math.min(buf.length, (index + 1) * chunkSize));
}
