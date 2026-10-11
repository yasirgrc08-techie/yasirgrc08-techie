#!/usr/bin/env node
import { createApp, randomFaults } from './app.js';

function parseSize(value, fallback) {
  if (!value) return fallback;
  const m = /^(\d+(?:\.\d+)?)\s*([kmgt]?)i?b?$/i.exec(String(value).trim());
  if (!m) throw new Error(`Invalid size: ${value}`);
  const mult = { '': 1, k: 1024, m: 1024 ** 2, g: 1024 ** 3, t: 1024 ** 4 }[m[2].toLowerCase()];
  return Math.floor(Number(m[1]) * mult);
}

const env = process.env;
const faultRate = Number(env.FAULT_RATE || 0);
const log = (msg) => console.log(`${new Date().toISOString()} ${msg}`);

const app = await createApp({
  dataDir: env.DATA_DIR || env.UPLOAD_DIR || './data',
  maxFileSize: parseSize(env.MAX_FILE_SIZE, 6 * 1024 ** 3),
  minChunkSize: parseSize(env.MIN_CHUNK_SIZE, 256 * 1024),
  maxChunkSize: parseSize(env.MAX_CHUNK_SIZE, 64 * 1024 ** 2),
  uploadTtlMs: Number(env.UPLOAD_TTL_HOURS || 24) * 60 * 60 * 1000,
  faults: faultRate > 0 ? randomFaults(faultRate) : null,
  faultDelayMs: Number(env.FAULT_DELAY_MS || 45_000),
  log,
});

const port = await app.listen(Number(env.PORT || 8080), env.HOST || '0.0.0.0');
log(`resumable-upload listening on http://localhost:${port}`);
if (faultRate > 0) log(`fault injection enabled: ~${Math.round(faultRate * 100)}% of upload requests will fail`);

let shuttingDown = false;
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    if (shuttingDown) process.exit(1);
    shuttingDown = true;
    log(`${sig} received, shutting down (in-progress uploads stay resumable)`);
    await app.close();
    process.exit(0);
  });
}
