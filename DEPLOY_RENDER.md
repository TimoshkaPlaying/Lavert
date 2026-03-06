# Deploy (Render Free)

1. Push this project to GitHub (private or public).
2. In Render: New + -> Blueprint, select repo.
3. Render reads `render.yaml` and creates web service.
4. Open service URL after build.

Notes:
- Uses `gunicorn + eventlet` for Flask-SocketIO/WebSocket support.
- Persistent files (JSON/uploads) are ephemeral on free dyno restarts.
