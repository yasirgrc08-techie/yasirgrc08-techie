import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { UploadStore, HttpError } from './store.js';

const PUBLIC_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

/**
 * Fault injection used to demonstrate (and test) resilience.
 * Returns one of: null, 'drop-before' (connection dies before the request is
 * handled), 'drop-after' (request is processed but the response is lost),
 * 'delay' (response arrives after the client has likely timed out).
 */
export function randomFaults(rate) {
  return () => {
    const r = Math.random();
    if (r >= rate) return null;
    if (r < rate / 3) return 'drop-before';
    if (r < (2 * rate) / 3) return 'drop-after';
    return 'delay';
  };
}

export async function createApp({
  dataDir,
  maxFileSize,
  minChunkSize,
  maxChunkSize,
  uploadTtlMs,
  sweepIntervalMs = 60_000,
  faults = null, // (route) => null | 'drop-before' | 'drop-after' | 'delay'
  faultDelayMs = 45_000,
  socketIdleTimeoutMs = 60_000,
  log = () => {},
} = {}) {
  const store = new UploadStore({ dataDir, maxFileSize, minChunkSize, maxChunkSize, uploadTtlMs, log });
  await store.init();

  const routes = [
    ['GET', /^\/api\/health$/, health],
    ['GET', /^\/api\/config$/, config],
    ['POST', /^\/api\/uploads$/, createUpload, 'create'],
    ['GET', /^\/api\/uploads\/([^/]+)$/, getUpload],
    ['DELETE', /^\/api\/uploads\/([^/]+)$/, deleteUpload],
    ['PUT', /^\/api\/uploads\/([^/]+)\/chunks\/(\d+)$/, putChunk, 'chunk'],
    ['POST', /^\/api\/uploads\/([^/]+)\/complete$/, completeUpload, 'complete'],
    ['GET', /^\/api\/files$/, listFiles],
    ['DELETE', /^\/api\/files\/([^/]+)$/, deleteFile],
    ['GET', /^\/files\/([^/]+)$/, downloadFile],
    ['HEAD', /^\/files\/([^/]+)$/, downloadFile],
  ];

  async function health() {
    return { status: 200, body: { ok: true, time: new Date().toISOString(), faultInjection: Boolean(faults) } };
  }

  async function config() {
    return {
      status: 200,
      body: {
        maxFileSize: store.maxFileSize,
        minChunkSize: store.minChunkSize,
        maxChunkSize: store.maxChunkSize,
        uploadTtlMs: store.uploadTtlMs,
        faultInjection: Boolean(faults),
      },
    };
  }

  async function createUpload(req) {
    const body = await readJson(req);
    const { created, status } = await store.create({
      fileName: body.fileName,
      fileSize: body.fileSize,
      chunkSize: body.chunkSize,
      mimeType: body.mimeType,
      idempotencyKey: req.headers['idempotency-key'] || body.idempotencyKey || null,
    });
    return { status: created ? 201 : 200, body: status };
  }

  async function getUpload(req, [id]) {
    return { status: 200, body: store.status(id) };
  }

  async function deleteUpload(req, [id]) {
    await store.remove(id);
    return { status: 204 };
  }

  async function putChunk(req, [id, indexStr]) {
    const cl = req.headers['content-length'];
    const result = await store.writeChunk(id, Number(indexStr), req, {
      expectedHash: req.headers['x-chunk-sha256'] || null,
      contentLength: cl === undefined ? null : Number(cl),
    });
    const s = store.status(id);
    return {
      status: result.duplicate ? 200 : 201,
      body: { ...result, receivedCount: s.receivedCount, totalChunks: s.totalChunks },
    };
  }

  async function completeUpload(req, [id]) {
    req.resume();
    return { status: 200, body: await store.complete(id) };
  }

  async function listFiles() {
    return { status: 200, body: { files: store.listFiles() } };
  }

  async function deleteFile(req, [id]) {
    await store.removeFile(id);
    return { status: 204 };
  }

  // Downloads support HTTP Range so they can be resumed too.
  async function downloadFile(req, [id], res) {
    const { meta, path: filePath } = store.getFile(id);
    const size = meta.fileSize;
    const headers = {
      'Content-Type': meta.mimeType || 'application/octet-stream',
      'Content-Disposition': `attachment; filename="${meta.fileName.replace(/[^\x20-\x7e]|"/g, '_')}"; filename*=UTF-8''${encodeURIComponent(meta.fileName)}`,
      'Accept-Ranges': 'bytes',
      ETag: `"${meta.id}"`,
      'Last-Modified': new Date(meta.uploadedAt).toUTCString(),
    };
    let start = 0;
    let end = size - 1;
    let status = 200;
    const range = req.headers.range;
    const ifRange = req.headers['if-range'];
    if (range && size > 0 && (!ifRange || ifRange === headers.ETag)) {
      const m = /^bytes=(\d*)-(\d*)$/.exec(range.trim());
      if (!m || (m[1] === '' && m[2] === '')) {
        res.writeHead(416, { 'Content-Range': `bytes */${size}` });
        return res.end();
      }
      if (m[1] === '') {
        start = Math.max(0, size - Number(m[2]));
      } else {
        start = Number(m[1]);
        end = m[2] === '' ? size - 1 : Math.min(Number(m[2]), size - 1);
      }
      if (start > end || start >= size) {
        res.writeHead(416, { 'Content-Range': `bytes */${size}` });
        return res.end();
      }
      status = 206;
      headers['Content-Range'] = `bytes ${start}-${end}/${size}`;
    }
    headers['Content-Length'] = size === 0 ? 0 : end - start + 1;
    res.writeHead(status, headers);
    if (req.method === 'HEAD' || size === 0) return res.end();
    const stream = fs.createReadStream(filePath, { start, end });
    stream.on('error', () => res.destroy());
    res.on('close', () => stream.destroy());
    stream.pipe(res);
  }

  async function serveStatic(req, res, pathname) {
    const rel = pathname === '/' ? 'index.html' : decodeURIComponent(pathname).replace(/^\/+/, '');
    const file = path.resolve(PUBLIC_DIR, rel);
    if (!file.startsWith(PUBLIC_DIR + path.sep)) return sendJson(res, 404, { error: 'Not found' });
    try {
      const data = await fsp.readFile(file);
      res.writeHead(200, {
        'Content-Type': MIME[path.extname(file)] || 'application/octet-stream',
        'Cache-Control': 'no-cache',
      });
      res.end(req.method === 'HEAD' ? undefined : data);
    } catch {
      sendJson(res, 404, { error: 'Not found' });
    }
  }

  async function handle(req, res) {
    const { pathname } = new URL(req.url, 'http://localhost');
    for (const [method, re, fn, faultRoute] of routes) {
      if (method !== req.method) continue;
      const m = re.exec(pathname);
      if (!m) continue;

      const fault = faultRoute && faults ? faults(faultRoute) : null;
      if (fault === 'drop-before') {
        log(`[fault] dropping connection before handling ${req.method} ${pathname}`);
        return req.socket.destroy();
      }
      let result;
      try {
        result = await fn(req, m.slice(1), res);
      } catch (err) {
        result = errorResult(err, log);
        if (result.body.aborted) {
          // The client went away mid-body; nobody is listening for a reply.
          return res.destroy();
        }
        // Don't leave an unread request body on a keep-alive connection.
        if (!req.complete) res.setHeader('Connection', 'close');
      }
      if (!result) return; // handler streamed its own response
      if (fault === 'drop-after') {
        log(`[fault] processed ${req.method} ${pathname} but dropping the response`);
        return req.socket.destroy();
      }
      if (fault === 'delay') {
        log(`[fault] delaying response to ${req.method} ${pathname} by ${faultDelayMs}ms`);
        await new Promise((r) => setTimeout(r, faultDelayMs));
        if (res.destroyed || req.socket.destroyed) return;
      }
      return sendJson(res, result.status, result.body, result.headers);
    }
    if (req.method === 'GET' || req.method === 'HEAD') return serveStatic(req, res, pathname);
    sendJson(res, 404, { error: 'Not found' });
  }

  const server = http.createServer((req, res) => {
    handle(req, res).catch((err) => {
      log(`unhandled error: ${err.stack || err}`);
      if (!res.headersSent) sendJson(res, 500, { error: 'Internal server error' });
      else res.destroy();
    });
  });
  // Drop connections from clients that silently vanished (e.g. Wi-Fi died
  // mid-chunk) instead of holding them open forever.
  server.timeout = socketIdleTimeoutMs;
  server.headersTimeout = 30_000;
  server.requestTimeout = 15 * 60_000;

  const sweeper = setInterval(() => store.sweep().catch((e) => log(`sweep failed: ${e.message}`)), sweepIntervalMs);
  sweeper.unref();

  return {
    server,
    store,
    listen(port = 0, host) {
      return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, host, () => {
          server.off('error', reject);
          resolve(server.address().port);
        });
      });
    },
    async close() {
      clearInterval(sweeper);
      await new Promise((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections();
      });
    },
  };
}

function errorResult(err, log) {
  if (err instanceof HttpError) {
    return { status: err.status, body: { error: err.message, ...err.extra } };
  }
  log(`error: ${err.stack || err}`);
  return { status: 500, body: { error: 'Internal server error' } };
}

function sendJson(res, status, body, headers = {}) {
  if (res.destroyed) return;
  if (status === 204 || body === undefined) {
    res.writeHead(status, headers);
    return res.end();
  }
  const data = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(data),
    'Cache-Control': 'no-store',
    ...headers,
  });
  res.end(data);
}

async function readJson(req, limit = 64 * 1024) {
  let size = 0;
  const parts = [];
  try {
    for await (const part of req) {
      size += part.length;
      if (size > limit) throw new HttpError(413, 'Request body too large');
      parts.push(part);
    }
  } catch (err) {
    if (err instanceof HttpError) throw err;
    throw new HttpError(400, 'Client disconnected', { aborted: true });
  }
  try {
    return JSON.parse(Buffer.concat(parts).toString('utf8') || '{}');
  } catch {
    throw new HttpError(400, 'Invalid JSON body');
  }
}
