# Resilient Upload

Upload and share large files (5 GB+) over unreliable networks. Files are split into chunks.
Every chunk is checksummed, retried and saved durably. An upload survives Wi-Fi drops, server
crashes, lost responses, timeouts, page reloads and duplicate requests, and continues from the
last chunk the server confirmed instead of starting over.

- **Server:** Node.js (≥ 20) with **no npm dependencies**
- **Clients:** a browser UI (drag and drop, progress, pause/resume, share links) and a CLI.
  Both use the same upload engine (`public/uploader-core.js`).
- **Downloads:** support HTTP `Range`, so downloads can resume too.

## Quick start

```bash
cd resumable-upload
npm start                       # http://localhost:8080
npm test                        # server + client resilience tests

# Demo mode: about 20% of upload requests are randomly dropped, lose their response or stall
npm run start:chaos

# CLI upload. Ctrl+C pauses; running the same command again resumes.
node client/upload.js ./big-file.iso --server http://localhost:8080 --chunk-size 8MB
```

Open the page, drop a file, and try breaking things: switch off Wi-Fi, kill the server and
start it again, or reload the tab. The upload carries on.

## How it works

```
 Browser / CLI                                   Server
 ─────────────                                   ──────
 POST /api/uploads  (Idempotency-Key) ─────────▶  create session: meta.json + sparse data.bin
 GET  /api/uploads/:id ────────────────────────▶  which chunks do you already have?
 PUT  /api/uploads/:id/chunks/:n  ×N  ─────────▶  verify SHA-256 → pwrite at n·chunkSize → fsync
      (X-Chunk-Sha256, 3 in parallel)              → append "n sha256" to journal → fsync → 201
 POST /api/uploads/:id/complete ───────────────▶  all chunks present? → rename into files/ → link
 GET  /files/:id   (Range supported) ──────────▶  download / share
```

The server writes each chunk directly to its final offset in a preallocated sparse file.
Finishing a 5 GB upload is therefore a rename (no copy), and it only needs the disk space of
the file itself.

## Failure scenarios handled

| Scenario | What happens |
|---|---|
| **Internet disconnect** | Failed requests are retried with exponential backoff and jitter (1 s → 30 s cap). If the browser reports it is offline, the upload waits in the *Offline* state and continues as soon as the `online` event fires. Retries never give up on transient errors. |
| **Chunked upload** | Files are cut into 1–64 MB chunks (default 4 MB; pick 1 MB for very bad links). Only the failed chunk is resent, never the whole file. Three chunks upload in parallel. |
| **Server restart / crash** | All state lives on disk (`meta.json`, `data.bin`, `journal.log`). A chunk is acknowledged only after its data **and** journal entry are fsync'ed. On startup the server rebuilds every upload from the journal, repairs a torn last journal line, and finishes any upload that crashed during finalization. The client sees connection errors, retries, then asks `GET /api/uploads/:id` which chunks exist and sends only the missing ones. |
| **Client response timeout** | Each chunk request has a timeout that scales with chunk size (assumes ≥ 32 KB/s, minimum 30 s). The browser also aborts a request that sends no bytes for 30 s (stall detection). A timed-out request is simply retried, and the server treats the retry as a duplicate if the original actually arrived. On the server, idle sockets are dropped after 60 s. |
| **Duplicate requests** | Everything is idempotent. **Create**: an `Idempotency-Key` is saved on the client *before* the request, so a retried create (even after a page reload) returns the same upload. **Chunks**: re-sending a stored chunk returns `200 {duplicate:true}` without rewriting it; concurrent copies of the same chunk are serialized; the same index with *different* bytes is rejected with `409`. **Complete**: calling it twice, or concurrently, returns the same file. |
| **Incomplete upload** | `complete` refuses with `409` and lists `missingChunks`; the client sends those and tries again. If the browser tab closes, upload state stays in `localStorage`: the page shows *Interrupted uploads* and resumes when you select the same file again. Uploads idle for longer than `UPLOAD_TTL_HOURS` (24 h) are deleted by a background sweep; a client returning after that starts a fresh session automatically. |
| **Corrupted data in transit** | Every chunk carries a SHA-256 (`X-Chunk-Sha256`). On a mismatch the server returns `422` and the client re-reads and resends the chunk. Wrong-length bodies are rejected. |
| **Client disconnects mid-chunk** | The partial body is discarded and nothing is recorded, so the chunk is sent again. |
| **Disk full / too large** | Free space is checked on create (`507`). Files over `MAX_FILE_SIZE` get `413`. |
| **File changed on disk** | A short read stops the upload with a clear error instead of uploading mixed content. |

## API

| Method & path | Purpose |
|---|---|
| `POST /api/uploads` | Body `{fileName, fileSize, chunkSize, mimeType}`, header `Idempotency-Key`. `201` new, `200` existing. |
| `GET /api/uploads/:id` | Status: `state`, `receivedChunks`, `totalChunks`, `expiresAt`. `404` once expired. |
| `PUT /api/uploads/:id/chunks/:index` | Raw bytes, optional `X-Chunk-Sha256`. `201` stored, `200` duplicate, `400` bad size/index, `409` conflict, `422` checksum mismatch. |
| `POST /api/uploads/:id/complete` | `200` with file info, or `409` with `missingChunks`. |
| `DELETE /api/uploads/:id` | Cancel an upload. |
| `GET /api/files` · `DELETE /api/files/:id` | List and delete shared files. |
| `GET /files/:id` | Download (supports `Range`, `HEAD`). This is the share link. |

## Configuration (environment variables)

| Variable | Default | |
|---|---|---|
| `PORT` | `8080` | |
| `DATA_DIR` | `./data` | Must be on persistent storage. |
| `MAX_FILE_SIZE` | `6GB` | Accepts `KB`/`MB`/`GB` suffixes. |
| `MIN_CHUNK_SIZE` / `MAX_CHUNK_SIZE` | `256KB` / `64MB` | |
| `UPLOAD_TTL_HOURS` | `24` | Idle uploads are deleted after this. |
| `FAULT_RATE` | `0` | `0.2` makes ~20% of upload requests fail (demo/testing). |
| `FAULT_DELAY_MS` | `45000` | How long a "stalled" response is held. |

## Deploying

GitHub stores the code, and GitHub Actions runs the tests on every push
(`.github/workflows/resumable-upload.yml`). **GitHub Pages cannot run it**, because it only serves
static files and this app needs a server with disk. Any host that runs Docker with a persistent
volume works, for example Render, Railway or Fly.io (add a volume), or a small VPS:

```bash
docker build -t resilient-upload resumable-upload
docker run -d -p 8080:8080 -v upload-data:/data --restart unless-stopped resilient-upload
```

Notes for production:

- Put it behind HTTPS. The browser can only compute chunk checksums (`crypto.subtle`) on HTTPS
  or `localhost`; without it, uploads still work but rely on TCP for integrity.
- Reverse proxies must allow request bodies at least as large as the chunk size, e.g. nginx
  `client_max_body_size 70m;`. Chunking also keeps each request under limits such as
  Cloudflare's 100 MB per request.
- There is no authentication. Anyone who can reach the server can upload, and anyone with a link
  can download. Add auth in front of it (a proxy, or a check in `server/app.js`) before exposing
  it publicly.

## Project layout

```
server/index.js            entry point, env config, graceful shutdown
server/app.js              HTTP routes, Range downloads, static files, fault injection
server/store.js            durable upload state: journal, idempotency, finalization, expiry
public/uploader-core.js    upload engine: chunking, retries, backoff, resume, offline wait
public/app.js              browser UI + XHR transport (progress, timeouts, stall detection)
client/upload.js           CLI uploader (resumable across runs)
test/                      node:test suites for server and client failure scenarios
```
