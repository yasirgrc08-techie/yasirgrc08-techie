// Resumable, chunked uploader shared by the browser UI and the Node CLI.
//
// Environment-specific pieces are injected:
//   source   { name, size, lastModified, type, read(start, end) -> Promise<Uint8Array> }
//   http     { request(method, url, opts) -> Promise<{ status, body, retryAfter }> }
//            throws TransferError with kind 'network' | 'timeout' | 'aborted'
//   sha256   (Uint8Array) -> Promise<hex string>   (optional)
//   storage  { get(key), set(key, value), remove(key) } (sync; JSON values)
//   isOnline / waitForOnline(signal)                  (optional)

export const KiB = 1024;
export const MiB = 1024 * 1024;

export class TransferError extends Error {
  constructor(message, { kind = 'http', status = 0, body = null, retryAfter = null } = {}) {
    super(message);
    this.name = 'TransferError';
    this.kind = kind;
    this.status = status;
    this.body = body;
    this.retryAfter = retryAfter;
  }
}

class SessionLostError extends Error {}

const RETRYABLE_STATUS = new Set([0, 408, 422, 425, 429, 500, 502, 503, 504, 507]);

export function isRetryable(err) {
  if (!(err instanceof TransferError)) return false;
  if (err.kind === 'network' || err.kind === 'timeout') return true;
  return err.kind === 'http' && RETRYABLE_STATUS.has(err.status);
}

export function fileFingerprint(source) {
  return `${source.name}|${source.size}|${source.lastModified ?? 0}`;
}

export const STORAGE_PREFIX = 'resumable-upload:';

function randomKey() {
  const bytes = new Uint8Array(16);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

export class ResumableUpload {
  constructor(opts) {
    this.source = opts.source;
    this.endpoint = (opts.endpoint || '').replace(/\/+$/, '');
    this.http = opts.http;
    this.sha256 = opts.sha256 || null;
    this.storage = opts.storage;
    this.isOnline = opts.isOnline || (() => true);
    this.waitForOnline = opts.waitForOnline || (async () => {});
    this.onUpdate = opts.onUpdate || (() => {});
    this.log = opts.log || (() => {});

    this.requestedChunkSize = opts.chunkSize || 4 * MiB;
    this.concurrency = Math.max(1, opts.concurrency || 3);
    this.baseDelayMs = opts.baseDelayMs ?? 1000;
    this.maxDelayMs = opts.maxDelayMs ?? 30_000;
    this.maxRetries = opts.maxRetries ?? Infinity;
    // Allow at least ~32 KiB/s per request before declaring a timeout.
    this.minBytesPerSecond = opts.minBytesPerSecond ?? 32 * KiB;
    this.minRequestTimeoutMs = opts.minRequestTimeoutMs ?? 30_000;
    this.stallTimeoutMs = opts.stallTimeoutMs ?? 30_000;

    this.storageKey = STORAGE_PREFIX + fileFingerprint(this.source);
    this.state = 'idle';
    this.uploadId = null;
    this.chunkSize = null;
    this.totalChunks = 0;
    this.received = new Set();
    this.inflightBytes = new Map();
    this.retryAt = null;
    this.lastError = null;
    this.result = null;
    this.stats = { requests: 0, failures: 0, duplicates: 0, sessionsCreated: 0 };
    this._wakers = new Set();
    this._abort = null;
    this._running = null;
  }

  // ---------- public API ----------

  start() {
    if (this._running) return this._running;
    if (this.state === 'complete') return Promise.resolve(this.result);
    this._abort = new AbortController();
    this.lastError = null;
    this._running = this._run().finally(() => {
      this._running = null;
    });
    return this._running;
  }

  /** Stop sending. Everything already acknowledged stays on the server. */
  pause() {
    if (!this._running) return;
    this._setState('paused');
    this._abort.abort();
  }

  /** Abort and delete the upload on the server. */
  async cancel() {
    this.pause();
    await this._running?.catch(() => {});
    const id = this.uploadId || this.storage.get(this.storageKey)?.uploadId;
    this.storage.remove(this.storageKey);
    this._setState('cancelled');
    if (id) {
      try {
        await this.http.request('DELETE', `${this.endpoint}/api/uploads/${id}`, { timeoutMs: 10_000 });
      } catch {
        // Best effort: the server's TTL sweep cleans it up anyway.
      }
    }
  }

  /** Skip any remaining back-off delay (e.g. the browser just came back online). */
  nudge() {
    for (const wake of this._wakers) wake();
  }

  snapshot() {
    let uploadedBytes = 0;
    for (const i of this.received) uploadedBytes += this._chunkLength(i);
    for (const b of this.inflightBytes.values()) uploadedBytes += b;
    return {
      state: this.state,
      fileName: this.source.name,
      totalBytes: this.source.size,
      uploadedBytes: Math.min(uploadedBytes, this.source.size),
      confirmedChunks: this.received.size,
      totalChunks: this.totalChunks,
      uploadId: this.uploadId,
      retryAt: this.retryAt,
      lastError: this.lastError,
      result: this.result,
      stats: { ...this.stats },
    };
  }

  // ---------- main loop ----------

  async _run() {
    const signal = this._abort.signal;
    try {
      let sessionAttempts = 0;
      for (;;) {
        try {
          await this._ensureSession(signal);
          if (this.state === 'complete') return this.result;
          this._setState('uploading');
          await this._uploadMissing(signal);
          this._setState('finalizing');
          const done = await this._complete(signal);
          if (done) {
            this.result = done;
            this.storage.remove(this.storageKey);
            this._setState('complete');
            return done;
          }
          // Server reported missing chunks: loop and send them.
        } catch (err) {
          if (!(err instanceof SessionLostError)) throw err;
          // The server forgot this upload (expired or deleted): start over.
          if (++sessionAttempts > 3) throw new TransferError('Upload session keeps disappearing on the server');
          this.log('upload session lost on the server; starting a new one');
          this.storage.remove(this.storageKey);
          this.uploadId = null;
          this.received.clear();
        }
      }
    } catch (err) {
      if (signal.aborted) {
        if (this.state !== 'cancelled') this._setState('paused');
        return null;
      }
      this.lastError = err.message;
      this._setState('failed');
      throw err;
    } finally {
      this.inflightBytes.clear();
      this.retryAt = null;
    }
  }

  async _ensureSession(signal) {
    this._setState('preparing');
    let saved = this.storage.get(this.storageKey);

    if (saved?.uploadId) {
      // Resume: ask the server which chunks it already has.
      const res = await this._withRetry(signal, () =>
        this._call('GET', `/api/uploads/${saved.uploadId}`, { signal, okStatuses: [200, 404] }),
      );
      if (res.status === 200) return this._applyStatus(res.body);
      this.storage.remove(this.storageKey);
      saved = null;
    }

    // The idempotency key is persisted *before* the request so that if the
    // response is lost, the retry (even after a page reload) gets the same upload.
    const chunkSize = saved?.chunkSize || this.requestedChunkSize;
    const idempotencyKey = saved?.idempotencyKey || randomKey();
    this.storage.set(this.storageKey, {
      idempotencyKey,
      chunkSize,
      fileName: this.source.name,
      fileSize: this.source.size,
      savedAt: Date.now(),
    });
    const res = await this._withRetry(signal, () =>
      this._call('POST', '/api/uploads', {
        signal,
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
        body: JSON.stringify({
          fileName: this.source.name,
          fileSize: this.source.size,
          mimeType: this.source.type || 'application/octet-stream',
          chunkSize,
        }),
        okStatuses: [200, 201],
      }),
    );
    if (res.status === 201) this.stats.sessionsCreated++;
    this.storage.set(this.storageKey, {
      uploadId: res.body.uploadId,
      idempotencyKey,
      chunkSize: res.body.chunkSize,
      fileName: this.source.name,
      fileSize: this.source.size,
      savedAt: Date.now(),
    });
    this._applyStatus(res.body);
  }

  _applyStatus(s) {
    this.uploadId = s.uploadId;
    this.chunkSize = s.chunkSize;
    this.totalChunks = s.totalChunks;
    this.received = new Set(s.receivedChunks);
    if (s.state === 'complete') {
      this.result = { id: s.fileId, url: `/files/${s.fileId}`, fileName: s.fileName, fileSize: s.fileSize };
      this.storage.remove(this.storageKey);
      this._setState('complete');
    } else {
      this._emit();
    }
  }

  async _uploadMissing(signal) {
    const queue = [];
    for (let i = 0; i < this.totalChunks; i++) if (!this.received.has(i)) queue.push(i);
    const worker = async () => {
      while (queue.length && !signal.aborted) {
        const index = queue.shift();
        try {
          await this._withRetry(signal, () => this._sendChunk(index, signal));
        } catch (err) {
          queue.length = 0; // stop the other workers too
          throw err;
        }
      }
    };
    const workers = Array.from({ length: Math.min(this.concurrency, queue.length) }, worker);
    const results = await Promise.allSettled(workers);
    const failed = results.find((r) => r.status === 'rejected');
    if (failed) throw failed.reason;
    if (signal.aborted) throw new TransferError('Aborted', { kind: 'aborted' });
  }

  async _sendChunk(index, signal) {
    if (this.received.has(index)) return;
    const start = index * this.chunkSize;
    const len = this._chunkLength(index);
    const bytes = await this.source.read(start, start + len);
    if (bytes.byteLength !== len) {
      throw new TransferError('The file changed or could not be read; select it again', { kind: 'fatal' });
    }
    const headers = { 'Content-Type': 'application/octet-stream' };
    if (this.sha256) headers['X-Chunk-Sha256'] = await this.sha256(bytes);

    this.inflightBytes.set(index, 0);
    try {
      const res = await this._call('PUT', `/api/uploads/${this.uploadId}/chunks/${index}`, {
        signal,
        headers,
        body: bytes,
        timeoutMs: Math.max(this.minRequestTimeoutMs, (len / this.minBytesPerSecond) * 1000),
        stallTimeoutMs: this.stallTimeoutMs,
        onProgress: (loaded) => {
          this.inflightBytes.set(index, Math.min(loaded, len));
          this._emit();
        },
        okStatuses: [200, 201],
      });
      if (res.status === 200) this.stats.duplicates++;
      this.received.add(index);
    } finally {
      this.inflightBytes.delete(index);
      this._emit();
    }
  }

  async _complete(signal) {
    const res = await this._withRetry(signal, () =>
      this._call('POST', `/api/uploads/${this.uploadId}/complete`, {
        signal,
        // Finalizing is a rename on the server, but be generous anyway.
        timeoutMs: 120_000,
        okStatuses: [200, 409],
      }),
    );
    if (res.status === 200) return res.body;
    // 409: incomplete. Drop whatever the server says it doesn't have.
    const missing = res.body?.missingChunks || [];
    if (!missing.length) throw new TransferError(res.body?.error || 'Upload could not be completed', { status: 409 });
    for (const i of missing) this.received.delete(i);
    this._emit();
    return null;
  }

  // ---------- plumbing ----------

  async _call(method, path, { okStatuses, signal, headers, body, timeoutMs = 30_000, stallTimeoutMs, onProgress }) {
    this.stats.requests++;
    const res = await this.http.request(method, this.endpoint + path, {
      headers,
      body,
      timeoutMs,
      stallTimeoutMs,
      onProgress,
      signal,
    });
    if (okStatuses.includes(res.status)) return res;
    if (res.status === 404 && path.startsWith(`/api/uploads/${this.uploadId}`)) {
      throw new SessionLostError('Upload session not found');
    }
    throw new TransferError(res.body?.error || `HTTP ${res.status}`, {
      status: res.status,
      body: res.body,
      retryAfter: res.retryAfter,
    });
  }

  async _withRetry(signal, fn) {
    for (let attempt = 0; ; attempt++) {
      if (signal.aborted) throw new TransferError('Aborted', { kind: 'aborted' });
      try {
        const result = await fn();
        if (this.state === 'retrying' || this.state === 'offline') {
          this.lastError = null;
          this._setState(this.uploadId ? 'uploading' : 'preparing');
        }
        return result;
      } catch (err) {
        if (signal.aborted) throw new TransferError('Aborted', { kind: 'aborted' });
        if (!isRetryable(err) || attempt >= this.maxRetries) throw err;
        this.stats.failures++;
        this.lastError = err.message;

        if (!this.isOnline()) {
          this._setState('offline');
          await this.waitForOnline(signal);
          attempt = 0;
          continue;
        }

        // Exponential back-off with full jitter, capped; honour Retry-After.
        const cap = Math.min(this.maxDelayMs, this.baseDelayMs * 2 ** Math.min(attempt, 16));
        let delay = Math.round(cap / 2 + (Math.random() * cap) / 2);
        const retryAfter = Number(err.retryAfter);
        if (retryAfter > 0) delay = Math.max(delay, retryAfter * 1000);
        this.retryAt = Date.now() + delay;
        this._setState('retrying');
        this.log(`retry #${attempt + 1} in ${delay}ms: ${err.message}`);
        await this._sleep(delay, signal);
        this.retryAt = null;
      }
    }
  }

  _sleep(ms, signal) {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this._wakers.delete(done);
        signal.removeEventListener('abort', done);
        resolve();
      };
      const timer = setTimeout(done, ms);
      this._wakers.add(done);
      signal.addEventListener('abort', done, { once: true });
    });
  }

  _chunkLength(index) {
    return Math.min(this.chunkSize, this.source.size - index * this.chunkSize);
  }

  _setState(state) {
    if (this.state === state) return;
    if (this.state === 'cancelled' || (this.state === 'complete' && state !== 'complete')) return;
    this.state = state;
    this._emit();
  }

  _emit() {
    try {
      this.onUpdate(this.snapshot());
    } catch {
      // UI errors must never break the upload.
    }
  }
}
