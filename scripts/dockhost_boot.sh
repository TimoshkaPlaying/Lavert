#!/bin/sh
set -eu

# Stable boot for Dockhost:
# - retries source download
# - caches app in /data/app-cache when available
# - caches virtualenv in /data/venv when available

REPO_ZIP_URL="${REPO_ZIP_URL:-https://codeload.github.com/TimoshkaPlaying/Lavert/zip/refs/heads/main}"
APP_DIR="${APP_DIR:-/app}"
DATA_DIR="${DATA_DIR:-/data}"
CACHE_APP_DIR="$DATA_DIR/app-cache"
VENV_DIR="$DATA_DIR/venv"
REQ_FILE_REL="requirements.txt"
REQ_HASH_FILE="$DATA_DIR/.requirements.sha256"

mkdir -p "$APP_DIR"

download_app() {
  tmp="$(mktemp -d)"
  zip_path="$tmp/repo.zip"
  attempt=1
  while [ "$attempt" -le 5 ]; do
    echo "Boot: downloading source (attempt $attempt/5)..."
    if curl -fsSL "$REPO_ZIP_URL" -o "$zip_path"; then
      break
    fi
    if [ "$attempt" -eq 5 ]; then
      echo "Boot: failed to download source after 5 attempts."
      return 1
    fi
    sleep $((attempt * 2))
    attempt=$((attempt + 1))
  done

  rm -rf "$APP_DIR"/*
  unzip -q "$zip_path" -d "$tmp"
  src_dir="$(find "$tmp" -maxdepth 1 -type d -name 'Lavert-*' | head -n 1)"
  if [ -z "${src_dir:-}" ]; then
    echo "Boot: source archive layout is unexpected."
    return 1
  fi
  cp -a "$src_dir"/. "$APP_DIR"/
  rm -rf "$tmp"
}

if [ -d "$DATA_DIR" ]; then
  mkdir -p "$CACHE_APP_DIR"
  if [ ! -f "$CACHE_APP_DIR/server/server.py" ]; then
    download_app
    rm -rf "$CACHE_APP_DIR"/*
    cp -a "$APP_DIR"/. "$CACHE_APP_DIR"/
  else
    rm -rf "$APP_DIR"/*
    cp -a "$CACHE_APP_DIR"/. "$APP_DIR"/
  fi
else
  download_app
fi

if [ ! -f "$APP_DIR/$REQ_FILE_REL" ]; then
  echo "Boot: missing $REQ_FILE_REL in app directory."
  exit 1
fi

REQ_HASH="$(sha256sum "$APP_DIR/$REQ_FILE_REL" | awk '{print $1}')"
OLD_HASH=""
if [ -f "$REQ_HASH_FILE" ]; then
  OLD_HASH="$(cat "$REQ_HASH_FILE" 2>/dev/null || true)"
fi

if [ ! -d "$VENV_DIR" ]; then
  python -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
. "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip wheel setuptools >/dev/null 2>&1 || true

if [ "$REQ_HASH" != "$OLD_HASH" ]; then
  attempt=1
  while [ "$attempt" -le 4 ]; do
    echo "Boot: installing dependencies (attempt $attempt/4)..."
    if pip install --no-cache-dir -r "$APP_DIR/$REQ_FILE_REL"; then
      echo "$REQ_HASH" > "$REQ_HASH_FILE"
      break
    fi
    if [ "$attempt" -eq 4 ]; then
      echo "Boot: dependency install failed after 4 attempts."
      exit 1
    fi
    sleep $((attempt * 3))
    attempt=$((attempt + 1))
  done
fi

cd "$APP_DIR"
exec gunicorn --worker-class eventlet -w 1 -b 0.0.0.0:5000 server.server:app

