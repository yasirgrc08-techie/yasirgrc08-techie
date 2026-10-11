import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ResumableUpload } from '../public/uploader-core.js';
import { nodeHttp, nodeSha256, memoryStorage } from '../client/node-adapters.js';
import { CHUNK, tempDir, startServer, randomBytes, sha256, memorySource } from './helpers.js';

function makeUploader(base, data, opts = {}) {
  return new ResumableUpload({
    source: memorySource(data),
    endpoint: base,
    http: nodeHttp,
    sha256: nodeSha256,
    storage: opts.storage || memoryStorage(),
    chunkSize: CHUNK,
    concurrency: 3,
    baseDelayMs: 5,
    maxDelayMs: 50,
    minRequestTimeoutMs: 1000,
    ...opts,
  });
}

async function download(base, url) {
  return Buffer.from(await (await fetch(base + url)).arrayBuffer());
}

test('uploads a file in chunks end to end', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(CHUNK * 10 + 999);
  const up = makeUploader(base, data);
  const result = await up.start();
  assert.equal(up.state, 'complete');
  assert.equal(sha256(await download(base, result.url)), sha256(data));
});

test('survives dropped connections, lost responses and client timeouts', async (t) => {
  // Deterministic chaos: fail every 3rd request in rotating ways.
  let n = 0;
  const kinds = ['drop-before', 'drop-after', 'delay'];
  const faults = () => (++n % 3 === 0 ? kinds[(n / 3) % 3] : null);
  const { app, base } = await startServer(await tempDir(), { faults, faultDelayMs: 1500 });
  t.after(() => app.close());

  const data = randomBytes(CHUNK * 12 + 5);
  const up = makeUploader(base, data);
  const result = await up.start();
  assert.equal(sha256(await download(base, result.url)), sha256(data));
  assert.ok(up.stats.failures > 0, 'faults were actually hit');
  assert.ok(up.stats.duplicates > 0, 'lost responses caused duplicate chunk sends');
  assert.equal(up.stats.sessionsCreated, 1, 'retried create reused the same session');
});

test('pause then resume (even from a new client instance) only sends missing chunks', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(CHUNK * 20);
  const storage = memoryStorage();

  const first = makeUploader(base, data, {
    storage,
    concurrency: 1,
    onUpdate: (s) => {
      if (s.confirmedChunks === 5) first.pause();
    },
  });
  assert.equal(await first.start(), null);
  assert.equal(first.state, 'paused');
  const confirmedBefore = (await (await fetch(`${base}/api/uploads/${first.uploadId}`)).json()).receivedCount;
  assert.ok(confirmedBefore >= 5 && confirmedBefore < 20);

  // Simulates a page reload / CLI restart: same file, same persisted storage.
  const second = makeUploader(base, data, { storage });
  const result = await second.start();
  assert.equal(second.uploadId, first.uploadId);
  assert.equal(second.stats.requests, 1 + (20 - confirmedBefore) + 1, 'status + missing chunks + complete');
  assert.equal(sha256(await download(base, result.url)), sha256(data));
  assert.equal(storage.map.size, 0, 'local resume state cleared after success');
});

test('resumes after the server restarts mid-upload', async () => {
  const dir = await tempDir();
  let { app, base, port } = await startServer(dir);
  const data = randomBytes(CHUNK * 30);
  let restarted = false;
  let restartDone;

  const up = makeUploader(base, data, {
    concurrency: 2,
    onUpdate: (s) => {
      if (!restarted && s.confirmedChunks >= 10) {
        restarted = true;
        restartDone = (async () => {
          await app.close(); // in-flight requests die with the server
          await new Promise((r) => setTimeout(r, 200));
          ({ app } = await startServer(dir, { port }));
        })();
      }
    },
  });
  try {
    const result = await up.start();
    await restartDone;
    assert.ok(restarted);
    assert.ok(up.stats.failures > 0);
    assert.equal(sha256(await download(base, result.url)), sha256(data));
  } finally {
    await restartDone;
    await app.close();
  }
});

test('starts a fresh session if the server expired the old one', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(CHUNK * 4);
  const storage = memoryStorage();

  const first = makeUploader(base, data, {
    storage,
    concurrency: 1,
    onUpdate: (s) => s.confirmedChunks === 2 && first.pause(),
  });
  await first.start();
  await app.store.sweep(Date.now() + 48 * 3600 * 1000);

  const second = makeUploader(base, data, { storage });
  const result = await second.start();
  assert.notEqual(second.uploadId, first.uploadId);
  assert.equal(sha256(await download(base, result.url)), sha256(data));
});

test('waits while offline and continues when the network returns', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(CHUNK * 6);

  let online = true;
  let goOnline;
  const flaky = {
    calls: 0,
    async request(method, url, opts) {
      if (method === 'PUT' && ++this.calls === 3) {
        online = false;
        setTimeout(() => {
          online = true;
          goOnline?.();
        }, 100);
        throw new (await import('../public/uploader-core.js')).TransferError('offline', { kind: 'network' });
      }
      return nodeHttp.request(method, url, opts);
    },
  };
  const states = new Set();
  const up = makeUploader(base, data, {
    http: flaky,
    isOnline: () => online,
    waitForOnline: () => new Promise((r) => (online ? r() : (goOnline = r))),
    onUpdate: (s) => states.add(s.state),
  });
  const result = await up.start();
  assert.ok(states.has('offline'));
  assert.equal(sha256(await download(base, result.url)), sha256(data));
});

test('a zero-byte file completes', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const up = makeUploader(base, Buffer.alloc(0));
  const result = await up.start();
  assert.equal(result.fileSize, 0);
  assert.equal((await download(base, result.url)).length, 0);
});

test('default chunk size is 4 MB: a 10 MB file is sent as 3 chunks', async (t) => {
  const { app, base } = await startServer(await tempDir());
  t.after(() => app.close());
  const data = randomBytes(10 * 1024 * 1024);
  const sizes = [];
  const http = {
    request(method, url, opts) {
      if (method === 'PUT') sizes.push(opts.body.byteLength);
      return nodeHttp.request(method, url, opts);
    },
  };
  const up = makeUploader(base, data, { http, chunkSize: undefined });
  const result = await up.start();
  assert.equal(up.chunkSize, 4 * 1024 * 1024);
  assert.deepEqual(sizes.sort((a, b) => b - a), [4194304, 4194304, 2097152]);
  assert.equal(sha256(await download(base, result.url)), sha256(data));
});
