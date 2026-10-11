// Node.js implementations of the uploader's injected dependencies.
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { TransferError } from '../public/uploader-core.js';

export const nodeHttp = {
  async request(method, url, { headers, body, timeoutMs = 30_000, signal } = {}) {
    const signals = [AbortSignal.timeout(timeoutMs)];
    if (signal) signals.push(signal);
    let res;
    try {
      res = await fetch(url, { method, headers, body, signal: AbortSignal.any(signals) });
      const text = await res.text();
      let parsed = null;
      try {
        parsed = text ? JSON.parse(text) : null;
      } catch {
        parsed = { error: text };
      }
      return { status: res.status, body: parsed, retryAfter: res.headers.get('retry-after') };
    } catch (err) {
      if (signal?.aborted) throw new TransferError('Aborted', { kind: 'aborted' });
      if (err.name === 'TimeoutError') throw new TransferError(`Request timed out after ${timeoutMs}ms`, { kind: 'timeout' });
      throw new TransferError(`Network error: ${err.cause?.code || err.cause?.message || err.message}`, { kind: 'network' });
    }
  },
};

export async function nodeSha256(bytes) {
  return crypto.createHash('sha256').update(bytes).digest('hex');
}

export async function fileSource(filePath) {
  const st = await fsp.stat(filePath);
  return {
    name: path.basename(filePath),
    size: st.size,
    lastModified: Math.floor(st.mtimeMs),
    type: 'application/octet-stream',
    async read(start, end) {
      const fh = await fsp.open(filePath, 'r');
      try {
        const buf = Buffer.allocUnsafe(end - start);
        let off = 0;
        while (off < buf.length) {
          const { bytesRead } = await fh.read(buf, off, buf.length - off, start + off);
          if (bytesRead === 0) break;
          off += bytesRead;
        }
        return buf.subarray(0, off);
      } finally {
        await fh.close();
      }
    },
  };
}

/** Tiny synchronous JSON key/value store so interrupted uploads survive a restart of the CLI. */
export function jsonFileStorage(file) {
  const load = () => {
    try {
      return JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      return {};
    }
  };
  const save = (data) => {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = `${file}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2));
    fs.renameSync(tmp, file);
  };
  return {
    get: (key) => load()[key] ?? null,
    set: (key, value) => save({ ...load(), [key]: value }),
    remove: (key) => {
      const data = load();
      delete data[key];
      save(data);
    },
  };
}

export function memoryStorage() {
  const map = new Map();
  return {
    get: (k) => (map.has(k) ? structuredClone(map.get(k)) : null),
    set: (k, v) => map.set(k, structuredClone(v)),
    remove: (k) => map.delete(k),
    map,
  };
}
