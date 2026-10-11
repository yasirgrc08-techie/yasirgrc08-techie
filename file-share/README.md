# File Share

A simple app to upload files and share them by link. Node.js only, no dependencies.

## Deploy (get a public link)

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/yasirgrc08-techie/yasirgrc08-techie/tree/claude/new-session-h2jp6b)

1. Click the button and sign in to Render with GitHub.
2. Click **Apply**. Render reads `render.yaml` and builds the app (about 2 minutes).
3. Your link appears at the top of the service page, e.g. `https://file-share-xxxx.onrender.com`.

If the button doesn't pick up the branch, use Render's dashboard instead: **New → Blueprint**, select this repo and the
`claude/new-session-h2jp6b` branch.

The free plan sleeps after 15 minutes without visits (the first visit after that takes ~1 minute) and **deletes
uploaded files whenever it restarts or redeploys**. To keep files, switch to a paid plan, add a Disk mounted at
`/var/data`, and set the environment variable `UPLOAD_DIR=/var/data`.

## Run locally

```bash
cd file-share
npm start          # http://localhost:8080
```

Open the page, drop a file, then use **Copy link** to share it. Anyone who has the link can download the file.

## Settings (environment variables)

| Variable | Default |
|---|---|
| `PORT` | `8080` |
| `UPLOAD_DIR` | `./uploads` |
| `MAX_FILE_SIZE` | `6442450944` (6 GB) |

## API

- `POST /api/upload`: raw file body, with the file name in the `X-File-Name` header
- `GET /api/files`: list files
- `GET /files/:id`: download (this is the share link)
- `DELETE /api/files/:id`: delete a file

Note: there is no login, so anyone who can open the app can upload and delete files.
