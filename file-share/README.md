# File Share

A simple app to upload files and share them by link. Node.js only, no dependencies.

## Run

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
GitHub Pages can't run it because it needs a Node server. Run it on a VPS, Render, Railway or similar.
