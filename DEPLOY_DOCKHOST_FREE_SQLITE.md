# Dockhost free setup (SQLite + B2)

Goal: no paid PostgreSQL, persistent DB after restarts.

## Environment variables

Set in Dockhost project environment:

- `DATA_DIR=/data`
- `JSON_FILE_FALLBACK=0`
- `STATE_SYNC_TO_OBJECT_STORAGE=1`
- `STATE_OBJECT_PREFIX=state/json_store`
- `B2_ENDPOINT=https://s3.eu-central-003.backblazeb2.com`
- `B2_KEY_ID=...`
- `B2_APPLICATION_KEY=...`
- `B2_BUCKET_NAME=...`
- `B2_PRESIGN_TTL=3600`

## Why this works

- App stores all structured data in SQLite (`json_store` table).
- SQLite file path is `DB_FILE`, defaulting to `DATA_DIR/app_data.sqlite3`.
- With `DATA_DIR=/data` and mounted disk, DB survives container restart.
- State datasets are also mirrored to B2 and auto-restored if local DB is empty.
- Media files are stored in B2 (no local disk growth).

## First migration from legacy JSON (one time)

1. Set `JSON_FILE_FALLBACK=1`
2. Restart app
3. Open app sections once (users/chats/groups/stories/support)
4. Set `JSON_FILE_FALLBACK=0`
5. Restart app

## Safe restart

Use pause/unpause only.
Do not delete project, volume, or B2 bucket.
