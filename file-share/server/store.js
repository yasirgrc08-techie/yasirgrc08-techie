// Durable upload state.
//
// Layout on disk (everything needed to resume lives here, so a server restart
// loses nothing that was acknowledged to a client):
//
//   <dataDir>/uploads/<uploadId>/meta.json    upload description + state
//   <dataDir>/uploads/<uploadId>/data.bin     preallocated (sparse) target file
//   <dataDir>/uploads/<uploadId>/journal.log  one "<index> <sha256>" line per stored chunk
//   <dataDir>/files/<fileId>/meta.json        finished, shareable file
//   <dataDir>/files/<fileId>/content.bin
//
// A chunk is acknowledged only after its bytes are fsync'ed into data.bin AND
// its journal line is fsync'ed. A crash between the two simply means the chunk
// is not recorded and the client re-sends it.

import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

export const ID_RE = /^[a-f0-9]{32}$/;
const JOURNAL_LINE_RE = /^(\d+) ([a-f0-9]{64})$/;

export class HttpError extends Error {
  constructor(status, message, extra = {}) {
    super(message);
    this.status = status;
    this.extra = extra;
  }
}

export function sha256Hex(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function sanitizeFileName(name) {
  const base = String(name ?? '')
    .replace(/[\\/]/g, '_')
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f\u007f]/g, '')
    .trim();
  if (!base || base === '.' || base === '..') return null;
  return base.slice(0, 255);
}

async function exists(p) {
  try {
    await fsp.access(p);
    return true;
  } catch {
    return false;
  }
}

async function writeJsonAtomic(file, value) {
  const tmp = `${file}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`;
  const fh = await fsp.open(tmp, 'w');
  try {
    await fh.writeFile(JSON.stringify(value, null, 2));
    await fh.sync();
  } finally {
    await fh.close();
  }
  await fsp.rename(tmp, file);
}

async function fsyncFile(file) {
  const fh = await fsp.open(file, 'r+');
  try {
    await fh.sync();
  } finally {
    await fh.close();
  }
}

export class UploadStore {
  constructor({
    dataDir,
    maxFileSize = 5 * 1024 ** 3,
    minChunkSize = 64 * 1024,
    maxChunkSize = 4 * 1024 ** 2,
    uploadTtlMs = 24 * 60 * 60 * 1000,
    log = () => {},
  }) {
    this.dataDir = path.resolve(dataDir);
    this.uploadsDir = path.join(this.dataDir, 'uploads');
    this.filesDir = path.join(this.dataDir, 'files');
    this.maxFileSize = maxFileSize;
    this.minChunkSize = minChunkSize;
    this.maxChunkSize = maxChunkSize;
    this.uploadTtlMs = uploadTtlMs;
    this.log = log;
    this.uploads = new Map(); // uploadId -> record
    this.idempotencyKeys = new Map(); // key -> Promise<uploadId>
    this.files = new Map(); // fileId -> file meta
  }

  async init() {
    await fsp.mkdir(this.uploadsDir, { recursive: true });
    await fsp.mkdir(this.filesDir, { recursive: true });

    for (const id of await fsp.readdir(this.filesDir)) {
      if (!ID_RE.test(id)) continue;
      try {
        const meta = JSON.parse(await fsp.readFile(path.join(this.filesDir, id, 'meta.json'), 'utf8'));
        this.files.set(id, meta);
      } catch {
        // meta.json not written yet: the owning upload is still "completing"
        // and will be finalized below.
      }
    }

    for (const id of await fsp.readdir(this.uploadsDir)) {
      if (!ID_RE.test(id)) continue;
      try {
        await this._loadUpload(id);
      } catch (err) {
        this.log(`skipping unreadable upload ${id}: ${err.message}`);
      }
    }
    this.log(`recovered ${this.uploads.size} upload(s) and ${this.files.size} file(s) from ${this.dataDir}`);
  }

  async _loadUpload(id) {
    const dir = path.join(this.uploadsDir, id);
    let meta;
    try {
      meta = JSON.parse(await fsp.readFile(path.join(dir, 'meta.json'), 'utf8'));
    } catch (err) {
      if (err.code === 'ENOENT') {
        // Crashed half way through create(); the client never got an id.
        await fsp.rm(dir, { recursive: true, force: true });
        return;
      }
      throw err;
    }

    const journalPath = path.join(dir, 'journal.log');
    const received = new Map();
    let journalMtime = 0;
    try {
      let text = await fsp.readFile(journalPath, 'utf8');
      journalMtime = (await fsp.stat(journalPath)).mtimeMs;
      if (text && !text.endsWith('\n')) {
        // Torn final line from a crash: terminate it so later appends start clean.
        await fsp.appendFile(journalPath, '\n');
        text += '\n';
      }
      for (const line of text.split('\n')) {
        const m = JOURNAL_LINE_RE.exec(line);
        if (!m) continue;
        const index = Number(m[1]);
        if (index < meta.totalChunks) received.set(index, m[2]);
      }
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }

    const rec = this._makeRecord(meta, dir, received);
    rec.updatedAt = Math.max(Date.parse(meta.updatedAt) || 0, journalMtime);
    this.uploads.set(id, rec);
    if (meta.idempotencyKey) this.idempotencyKeys.set(meta.idempotencyKey, Promise.resolve(id));

    if (meta.state === 'completing') {
      this.log(`resuming interrupted finalization of ${id}`);
      await this._finishCompletion(rec);
    }
  }

  _makeRecord(meta, dir, received) {
    return {
      meta,
      dir,
      received, // index -> sha256
      updatedAt: Date.now(),
      inflight: new Map(), // index -> Promise (serializes writes of the same chunk)
      journalQueue: Promise.resolve(),
      completing: null,
    };
  }

  _get(id) {
    const rec = ID_RE.test(id) ? this.uploads.get(id) : undefined;
    if (!rec) throw new HttpError(404, 'Upload not found (it may have expired). Start a new upload.');
    return rec;
  }

  chunkLength(meta, index) {
    const start = index * meta.chunkSize;
    return Math.min(meta.chunkSize, meta.fileSize - start);
  }

  async create({ fileName, fileSize, chunkSize, mimeType, idempotencyKey }) {
    const name = sanitizeFileName(fileName);
    if (!name) throw new HttpError(400, 'fileName is required');
    if (!Number.isSafeInteger(fileSize) || fileSize < 0) throw new HttpError(400, 'fileSize must be a non-negative integer');
    if (fileSize > this.maxFileSize) {
      throw new HttpError(413, `File too large: limit is ${this.maxFileSize} bytes`, { maxFileSize: this.maxFileSize });
    }
    if (!Number.isSafeInteger(chunkSize) || chunkSize < this.minChunkSize || chunkSize > this.maxChunkSize) {
      throw new HttpError(400, `chunkSize must be between ${this.minChunkSize} and ${this.maxChunkSize}`);
    }
    if (idempotencyKey != null && (typeof idempotencyKey !== 'string' || idempotencyKey.length > 200)) {
      throw new HttpError(400, 'Invalid idempotency key');
    }

    // Duplicate create (e.g. the response to the first attempt was lost):
    // hand back the upload that already exists.
    if (idempotencyKey && this.idempotencyKeys.has(idempotencyKey)) {
      const existingId = await this.idempotencyKeys.get(idempotencyKey).catch(() => null);
      const rec = existingId && this.uploads.get(existingId);
      if (rec) {
        const m = rec.meta;
        if (m.fileSize !== fileSize || m.chunkSize !== chunkSize || m.fileName !== name) {
          throw new HttpError(422, 'Idempotency key was already used for a different file');
        }
        return { created: false, status: this.status(existingId) };
      }
      this.idempotencyKeys.delete(idempotencyKey);
    }

    const id = crypto.randomBytes(16).toString('hex');
    const creating = this._createOnDisk(id, { name, fileSize, chunkSize, mimeType, idempotencyKey });
    if (idempotencyKey) this.idempotencyKeys.set(idempotencyKey, creating.then(() => id));
    try {
      await creating;
    } catch (err) {
      if (idempotencyKey) this.idempotencyKeys.delete(idempotencyKey);
      throw err;
    }
    return { created: true, status: this.status(id) };
  }

  async _createOnDisk(id, { name, fileSize, chunkSize, mimeType, idempotencyKey }) {
    await this._checkFreeSpace(fileSize);
    const dir = path.join(this.uploadsDir, id);
    await fsp.mkdir(dir, { recursive: true });
    try {
      // Sparse preallocation: chunks are written straight to their final
      // offset, so finishing an upload is a rename, not a 5 GB copy.
      const fh = await fsp.open(path.join(dir, 'data.bin'), 'w');
      try {
        await fh.truncate(fileSize);
      } finally {
        await fh.close();
      }
      await fsp.writeFile(path.join(dir, 'journal.log'), '');
      const now = new Date().toISOString();
      const meta = {
        id,
        fileName: name,
        fileSize,
        chunkSize,
        totalChunks: Math.ceil(fileSize / chunkSize),
        mimeType: typeof mimeType === 'string' && mimeType.length < 200 ? mimeType : 'application/octet-stream',
        idempotencyKey: idempotencyKey || null,
        state: 'uploading',
        createdAt: now,
        updatedAt: now,
        completedAt: null,
      };
      // meta.json is written last: its presence marks the upload as real.
      await writeJsonAtomic(path.join(dir, 'meta.json'), meta);
      this.uploads.set(id, this._makeRecord(meta, dir, new Map()));
    } catch (err) {
      await fsp.rm(dir, { recursive: true, force: true });
      throw err;
    }
  }

  async _checkFreeSpace(fileSize) {
    if (typeof fsp.statfs !== 'function') return;
    try {
      const s = await fsp.statfs(this.dataDir);
      if (s.bavail * s.bsize < fileSize) {
        throw new HttpError(507, 'Not enough free disk space on the server for this file');
      }
    } catch (err) {
      if (err instanceof HttpError) throw err;
    }
  }

  status(id) {
    const rec = this._get(id);
    const m = rec.meta;
    const receivedChunks = [...rec.received.keys()].sort((a, b) => a - b);
    let receivedBytes = 0;
    for (const i of receivedChunks) receivedBytes += this.chunkLength(m, i);
    return {
      uploadId: m.id,
      fileName: m.fileName,
      fileSize: m.fileSize,
      mimeType: m.mimeType,
      chunkSize: m.chunkSize,
      totalChunks: m.totalChunks,
      state: m.state,
      receivedChunks,
      receivedCount: receivedChunks.length,
      receivedBytes,
      fileId: m.state === 'complete' ? m.id : null,
      expiresAt: new Date(rec.updatedAt + this.uploadTtlMs).toISOString(),
    };
  }

  /**
   * Store one chunk. Idempotent: re-sending a chunk that is already stored
   * (same checksum) is a no-op that succeeds.
   */
  async writeChunk(id, index, body, { expectedHash, contentLength }) {
    const rec = this._get(id);
    const m = rec.meta;
    if (!Number.isSafeInteger(index) || index < 0 || index >= m.totalChunks) {
      throw new HttpError(400, `Chunk index must be between 0 and ${m.totalChunks - 1}`);
    }
    if (expectedHash != null && !/^[a-f0-9]{64}$/.test(expectedHash)) {
      throw new HttpError(400, 'X-Chunk-Sha256 must be a lowercase hex SHA-256');
    }
    const expectedLen = this.chunkLength(m, index);
    if (contentLength != null && contentLength !== expectedLen) {
      throw new HttpError(400, `Chunk ${index} must be exactly ${expectedLen} bytes, got ${contentLength}`);
    }

    const existing = rec.received.get(index);
    if (existing && (!expectedHash || expectedHash === existing)) {
      body.resume?.(); // discard the duplicate body
      return { index, duplicate: true, sha256: existing };
    }
    if (existing) throw new HttpError(409, `Chunk ${index} was already stored with different content`);
    if (m.state !== 'uploading') throw new HttpError(409, `Upload is ${m.state}`);

    const buf = await readExactly(body, expectedLen);
    const hash = sha256Hex(buf);
    if (expectedHash && hash !== expectedHash) {
      throw new HttpError(422, `Checksum mismatch for chunk ${index}: data was corrupted in transit`);
    }

    // Serialize concurrent writes of the same chunk (e.g. a retry racing the
    // original request whose response was lost).
    const prev = rec.inflight.get(index);
    const op = (async () => {
      if (prev) await prev.catch(() => {});
      const stored = rec.received.get(index);
      if (stored) {
        if (stored !== hash) throw new HttpError(409, `Chunk ${index} was already stored with different content`);
        return { index, duplicate: true, sha256: stored };
      }
      if (rec.meta.state !== 'uploading') throw new HttpError(409, `Upload is ${rec.meta.state}`);
      await writeAt(path.join(rec.dir, 'data.bin'), buf, index * m.chunkSize);
      await this._appendJournal(rec, `${index} ${hash}\n`);
      rec.received.set(index, hash);
      rec.updatedAt = Date.now();
      return { index, duplicate: false, sha256: hash };
    })();
    rec.inflight.set(index, op);
    try {
      return await op;
    } finally {
      if (rec.inflight.get(index) === op) rec.inflight.delete(index);
    }
  }

  _appendJournal(rec, line) {
    const next = rec.journalQueue.then(async () => {
      const fh = await fsp.open(path.join(rec.dir, 'journal.log'), 'a');
      try {
        await fh.write(line);
        await fh.datasync();
      } finally {
        await fh.close();
      }
    });
    rec.journalQueue = next.catch(() => {});
    return next;
  }

  /** Finalize an upload. Idempotent: completing twice returns the same file. */
  async complete(id) {
    const rec = this._get(id);
    if (rec.meta.state === 'complete') return this._fileInfo(id);
    if (rec.completing) return rec.completing;

    const missing = [];
    for (let i = 0; i < rec.meta.totalChunks; i++) if (!rec.received.has(i)) missing.push(i);
    if (missing.length) {
      throw new HttpError(409, `Upload is incomplete: ${missing.length} chunk(s) missing`, {
        missingCount: missing.length,
        missingChunks: missing.slice(0, 10000),
      });
    }

    rec.completing = (async () => {
      rec.meta.state = 'completing';
      rec.meta.updatedAt = new Date().toISOString();
      await writeJsonAtomic(path.join(rec.dir, 'meta.json'), rec.meta);
      await this._finishCompletion(rec);
      return this._fileInfo(id);
    })();
    try {
      return await rec.completing;
    } finally {
      rec.completing = null;
    }
  }

  // Safe to re-run after a crash at any point.
  async _finishCompletion(rec) {
    const m = rec.meta;
    const fileDir = path.join(this.filesDir, m.id);
    const dataPath = path.join(rec.dir, 'data.bin');
    const contentPath = path.join(fileDir, 'content.bin');
    await fsp.mkdir(fileDir, { recursive: true });
    if (await exists(dataPath)) {
      await fsyncFile(dataPath);
      await fsp.rename(dataPath, contentPath);
    } else if (!(await exists(contentPath))) {
      throw new Error(`upload ${m.id} lost its data file`);
    }
    const now = new Date().toISOString();
    const fileMeta = {
      id: m.id,
      fileName: m.fileName,
      fileSize: m.fileSize,
      mimeType: m.mimeType,
      chunkSize: m.chunkSize,
      totalChunks: m.totalChunks,
      uploadedAt: now,
    };
    await writeJsonAtomic(path.join(fileDir, 'meta.json'), fileMeta);
    this.files.set(m.id, fileMeta);
    m.state = 'complete';
    m.completedAt = now;
    m.updatedAt = now;
    await writeJsonAtomic(path.join(rec.dir, 'meta.json'), m);
    rec.updatedAt = Date.now();
  }

  _fileInfo(id) {
    const f = this.files.get(id);
    return { ...f, url: `/files/${id}` };
  }

  /** Abort an in-progress upload (or forget the record of a finished one). */
  async remove(id) {
    const rec = this._get(id);
    if (rec.completing) throw new HttpError(409, 'Upload is being finalized');
    this.uploads.delete(id);
    if (rec.meta.idempotencyKey) this.idempotencyKeys.delete(rec.meta.idempotencyKey);
    await fsp.rm(rec.dir, { recursive: true, force: true });
  }

  listFiles() {
    return [...this.files.values()]
      .map((f) => ({ ...f, url: `/files/${f.id}` }))
      .sort((a, b) => b.uploadedAt.localeCompare(a.uploadedAt));
  }

  getFile(id) {
    const f = ID_RE.test(id) ? this.files.get(id) : undefined;
    if (!f) throw new HttpError(404, 'File not found');
    return { meta: f, path: path.join(this.filesDir, id, 'content.bin') };
  }

  async removeFile(id) {
    this.getFile(id);
    this.files.delete(id);
    await fsp.rm(path.join(this.filesDir, id), { recursive: true, force: true });
    if (this.uploads.has(id)) await this.remove(id).catch(() => {});
  }

  /** Delete uploads that have been idle for longer than the TTL. */
  async sweep(now = Date.now()) {
    const removed = [];
    for (const [id, rec] of this.uploads) {
      if (rec.inflight.size || rec.completing) continue;
      if (now - rec.updatedAt <= this.uploadTtlMs) continue;
      this.uploads.delete(id);
      if (rec.meta.idempotencyKey) this.idempotencyKeys.delete(rec.meta.idempotencyKey);
      await fsp.rm(rec.dir, { recursive: true, force: true });
      removed.push(id);
    }
    if (removed.length) this.log(`expired ${removed.length} stale upload(s)`);
    return removed;
  }
}

/** Read exactly `len` bytes from a stream; reject on short, long or aborted bodies. */
async function readExactly(stream, len) {
  const buf = Buffer.allocUnsafe(len);
  let offset = 0;
  try {
    for await (const piece of stream) {
      if (offset + piece.length > len) {
        stream.destroy?.();
        throw new HttpError(413, `Chunk body is larger than the expected ${len} bytes`);
      }
      piece.copy(buf, offset);
      offset += piece.length;
    }
  } catch (err) {
    if (err instanceof HttpError) throw err;
    throw new HttpError(400, 'Client disconnected before the chunk was fully received', { aborted: true });
  }
  if (offset !== len) {
    throw new HttpError(400, `Chunk body is ${offset} bytes, expected ${len}`, { aborted: true });
  }
  return buf;
}

async function writeAt(file, buf, position) {
  const fh = await fsp.open(file, 'r+');
  try {
    let written = 0;
    while (written < buf.length) {
      const { bytesWritten } = await fh.write(buf, written, buf.length - written, position + written);
      written += bytesWritten;
    }
    await fh.datasync();
  } finally {
    await fh.close();
  }
}
