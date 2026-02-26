"""
WSGI entrypoint for platforms (like Render) that default to `gunicorn app:app`.

This file keeps backwards compatibility with deployments that expect an `app`
module while the project’s original entrypoint remains `main.py`.
"""

from backend import create_app

app = create_app()

