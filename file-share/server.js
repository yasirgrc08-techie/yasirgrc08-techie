// Simple file upload & sharing server. No dependencies.
//   node server.js   ->  http://localhost:8080
import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 8080);
const UPLOAD_DIR = path.resolve(process.env.UPLOAD_DIR || path.join(ROOT, 'uploads'));
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 6 * 1024 ** 3); // 6 GB

await fsp.mkdir(UPLOAD_DIR, { recursive: true });

const ID_RE = /^[a-f0-9]{16}$/;

function sendJson(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

function cleanName(name) {
  return String(name || 'file').replace(/[\\/\u0000-\u001f]/g, '_').slice(0, 255) || 'file';
}

async function listFiles() {
  const files = [];
  for (const id of await fsp.readdir(UPLOAD_DIR)) {
    if (!ID_RE.test(id)) continue;
    try {
      files.push(JSON.parse(await fsp.readFile(path.join(UPLOAD_DIR, id, 'meta.json'), 'utf8')));
    } catch {
      // skip unfinished uploads
    }
  }
  return files.sort((a, b) => b.uploadedAt.localeCompare(a.uploadedAt));
}

// POST /api/upload  (raw file body, file name in X-File-Name header)
async function upload(req, res) {
  const size = Number(req.headers['content-length']);
  if (!size && size !== 0) return sendJson(res, 411, { error: 'Content-Length required' });
  if (size > MAX_FILE_SIZE) return sendJson(res, 413, { error: 'File too large' });

  const id = crypto.randomBytes(8).toString('hex');
  const dir = path.join(UPLOAD_DIR, id);
  await fsp.mkdir(dir);
  try {
    await pipeline(req, fs.createWriteStream(path.join(dir, 'content')));
    const meta = {
      id,
      name: cleanName(decodeURIComponent(req.headers['x-file-name'] || 'file')),
      size,
      type: req.headers['content-type'] || 'application/octet-stream',
      uploadedAt: new Date().toISOString(),
      url: `/files/${id}`,
    };
    await fsp.writeFile(path.join(dir, 'meta.json'), JSON.stringify(meta));
    sendJson(res, 201, meta);
  } catch {
    await fsp.rm(dir, { recursive: true, force: true });
    if (!res.headersSent) sendJson(res, 400, { error: 'Upload failed' });
  }
}

async function download(req, res, id) {
  if (!ID_RE.test(id)) return sendJson(res, 404, { error: 'Not found' });
  try {
    const meta = JSON.parse(await fsp.readFile(path.join(UPLOAD_DIR, id, 'meta.json'), 'utf8'));
    res.writeHead(200, {
      'Content-Type': meta.type,
      'Content-Length': meta.size,
      'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(meta.name)}`,
    });
    fs.createReadStream(path.join(UPLOAD_DIR, id, 'content')).pipe(res);
  } catch {
    sendJson(res, 404, { error: 'Not found' });
  }
}

async function remove(res, id) {
  if (!ID_RE.test(id)) return sendJson(res, 404, { error: 'Not found' });
  await fsp.rm(path.join(UPLOAD_DIR, id), { recursive: true, force: true });
  res.writeHead(204).end();
}

const server = http.createServer(async (req, res) => {
  try {
    const { pathname } = new URL(req.url, 'http://x');
    const fileMatch = /^\/(?:api\/)?files\/([^/]+)$/.exec(pathname);

    if (req.method === 'POST' && pathname === '/api/upload') return await upload(req, res);
    if (req.method === 'GET' && pathname === '/api/files') return sendJson(res, 200, await listFiles());
    if (req.method === 'GET' && fileMatch && pathname.startsWith('/files/')) return await download(req, res, fileMatch[1]);
    if (req.method === 'DELETE' && fileMatch) return await remove(res, fileMatch[1]);
    if (req.method === 'GET' && pathname === '/') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return fs.createReadStream(path.join(ROOT, 'public', 'index.html')).pipe(res);
    }
    sendJson(res, 404, { error: 'Not found' });
  } catch (err) {
    console.error(err);
    if (!res.headersSent) sendJson(res, 500, { error: 'Server error' });
  }
});

server.requestTimeout = 0; // allow long uploads of big files
server.listen(PORT, () => console.log(`File share running at http://localhost:${PORT}`));
