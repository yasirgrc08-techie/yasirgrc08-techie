#!/usr/bin/env node
// Command-line uploader. Re-running the same command after a crash, Ctrl+C,
// or network loss resumes where it left off.
//
//   node client/upload.js <file> [--server http://localhost:8080] [--chunk-size 8MB] [--concurrency 3]

import os from 'node:os';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { ResumableUpload, MiB } from '../public/uploader-core.js';
import { nodeHttp, nodeSha256, fileSource, jsonFileStorage } from './node-adapters.js';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    server: { type: 'string', default: process.env.UPLOAD_SERVER || 'http://localhost:8080' },
    'chunk-size': { type: 'string', default: '200KB' },
    concurrency: { type: 'string', default: '3' },
    state: { type: 'string', default: path.join(os.homedir(), '.resumable-upload', 'state.json') },
  },
});

if (positionals.length !== 1) {
  console.error('usage: node client/upload.js <file> [--server URL] [--chunk-size 200KB] [--concurrency 3]');
  process.exit(2);
}

const m = /^(\d+)\s*(k|m)?i?b?$/i.exec(values['chunk-size']);
if (!m) {
  console.error(`invalid --chunk-size ${values['chunk-size']}`);
  process.exit(2);
}
const chunkSize = Number(m[1]) * ({ k: 1024, m: MiB }[m[2]?.toLowerCase()] || 1);

const fmt = (n) => (n >= MiB ? `${(n / MiB).toFixed(1)} MiB` : `${(n / 1024).toFixed(0)} KiB`);
let lastLine = 0;
let lastState = null;

const upload = new ResumableUpload({
  source: await fileSource(positionals[0]),
  endpoint: values.server,
  http: nodeHttp,
  sha256: nodeSha256,
  storage: jsonFileStorage(values.state),
  chunkSize,
  concurrency: Number(values.concurrency),
  onUpdate(s) {
    const now = Date.now();
    if (now - lastLine < 250 && s.state === lastState) return;
    lastLine = now;
    lastState = s.state;
    const pct = s.totalBytes ? ((s.uploadedBytes / s.totalBytes) * 100).toFixed(1) : '100.0';
    let extra = '';
    if (s.state === 'retrying') extra = ` - retrying in ${Math.max(0, Math.round((s.retryAt - now) / 1000))}s (${s.lastError})`;
    process.stderr.write(
      `\r\x1b[K${s.state.padEnd(10)} ${pct}%  ${fmt(s.uploadedBytes)} / ${fmt(s.totalBytes)}  chunks ${s.confirmedChunks}/${s.totalChunks}${extra}`,
    );
  },
});

process.on('SIGINT', () => {
  process.stderr.write('\npausing; run the same command again to resume\n');
  upload.pause();
});

try {
  const result = await upload.start();
  process.stderr.write('\n');
  if (!result) process.exit(130);
  console.log(`done: ${values.server.replace(/\/+$/, '')}${result.url}`);
} catch (err) {
  process.stderr.write('\n');
  console.error(`upload failed: ${err.message}`);
  process.exit(1);
}
