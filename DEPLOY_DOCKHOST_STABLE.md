# Stable Dockhost Run

Use this command as container `cmd` (with entrypoint `/bin/sh -c`) to reduce random startup failures:

```sh
set -e; chmod +x /app/scripts/dockhost_boot.sh || true; /app/scripts/dockhost_boot.sh
```

Recommended:

1. Disable Auto Stop in Dockhost UI.
2. Set container memory to at least `1024 MiB`.
3. Keep `ports: 5000/TCP`.
4. Keep `strategy: Recreate`.

If Docker build is available in Dockhost, use `Dockerfile` from repo and run default `CMD`.

