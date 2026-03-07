import os
import sys

try:
    import psycopg
except Exception as exc:
    print(f"psycopg import failed: {exc}")
    sys.exit(2)


def main() -> int:
    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn:
        print("DATABASE_URL is empty")
        return 1

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
        print(f"PostgreSQL OK: {row}")
        return 0
    except Exception as exc:
        print(f"PostgreSQL FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

