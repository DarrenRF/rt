import os
from backend import create_app

app = create_app()

# Runs the app
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    debug = (os.environ.get("FLASK_DEBUG") or "").strip() in {"1", "true", "True"}
    app.run(host="0.0.0.0", port=port, debug=debug)
