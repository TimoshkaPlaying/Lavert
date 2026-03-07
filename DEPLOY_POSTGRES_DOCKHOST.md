# PostgreSQL migration for Lavert (Dockhost)

This project already supports PostgreSQL in `server/server.py` via `DATABASE_URL`.

## 1) Create PostgreSQL in Dockhost

1. Open Dockhost panel.
2. Go to `Сетевые сервисы` (Network services).
3. Create a new PostgreSQL service.
4. Set disk size (recommended: `5-10 GiB`).
5. Save credentials:
   - host
   - port
   - database
   - username
   - password

## 2) Set environment variables for app container

In project `lavert` -> `Окружение` -> `Переменные` add/update:

- `DATABASE_URL=postgresql://USERNAME:PASSWORD@HOST:PORT/DBNAME`
- `JSON_FILE_FALLBACK=0`

Optional (for SQLite fallback path if needed later):
- `DATA_DIR=/data`

## 3) One-time migration strategy

If you need to import old file-based state to SQL:

1. Temporarily set:
   - `JSON_FILE_FALLBACK=1`
2. Restart app.
3. Open main screens once (users/chats/groups/etc), so datasets are read and inserted into SQL.
4. Set back:
   - `JSON_FILE_FALLBACK=0`
5. Restart app again.

## 4) Restart without data wipe

Use soft restart only:
- pause project
- unpause project

Do NOT delete project/service/disks.

## 5) Verify SQL backend

Check logs after start:
- No `psycopg` import/connection errors.
- App endpoints work (login/chats/upload).

If app starts but DB URL is wrong, write operations will fail quickly. Fix `DATABASE_URL` and restart.

