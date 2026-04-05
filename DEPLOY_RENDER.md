# Deploy (Render Free)

1. Push this project to GitHub (private or public).
2. In Render: New + -> Blueprint, select repo.
3. Render reads `render.yaml` and creates web service.
4. Open service URL after build.

Notes:
- Uses `gunicorn + gthread` (less freezes on weak hosts).
- Persistent files (JSON/uploads) are ephemeral on free dyno restarts.

Recommended anti-lag env profile:
- `STATE_SYNC_TO_OBJECT_STORAGE=1`
- `STATE_SYNC_STRICT=0`
- `STATE_SYNC_BLOCKING=0`
- `STATE_STORAGE_SUBPROCESS=1`
- `SOCKETIO_ASYNC_MODE=threading`
- `SEARCH_CTX_TTL_SEC=8`
- `STATE_CACHE_TTL_SEC=2.0`
