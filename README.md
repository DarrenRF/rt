## Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

App runs on `http://localhost:8000` by default.

## Deploying on Render

This repo includes a `render.yaml` blueprint configured for:

- `gunicorn main:app` (binds to Render's `$PORT`)
- SQLite stored on a persistent disk (`DB_PATH=/var/data/db.sqlite3`)
- Uploads stored on the same disk (`UPLOAD_FOLDER=/var/data/uploads`)

On Render:

1. Create a new **Web Service** from this repo (or use the blueprint import).
2. Ensure the service uses the commands from `render.yaml`.
3. After deploy, uploaded images will be served at `/uploads/...` (and legacy `/static/uploads/...` paths are also supported).

