from flask import Flask, request, jsonify, render_template, send_from_directory, redirect
from flask_socketio import SocketIO, emit
import json, os, time, uuid, tempfile, subprocess, shutil, logging, sqlite3, threading

try:
    import psycopg
except Exception:
    psycopg = None

try:
    import boto3
except Exception:
    boto3 = None

app = Flask(__name__, template_folder="../templates", static_folder="../static")
socketio = SocketIO(app, cors_allowed_origins="*")
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB

_online_users = {}   # { username: {sid, sid, ...} }
_sid_to_user  = {}   # { sid: username }
_active_calls = {}   # { call_id: {chat_id, participants:set, created_at, screen_owner, allow_draw_all, control_allowed:set} }
_auth_tokens = {}    # { token: {username, issued_at, last_seen} }
_forum_rate = {}     # { username: {"topic":[ts...], "reply":[ts...]} }

USERS_FILE   = "users.json"
MESSAGES_FILE = "messages.json"
GROUPS_FILE  = "groups.json"
AVATARS_FILE = "avatars.json"   # <-- НОВЫЙ файл для аватаров (base64)
PRIVACY_FILE = "privacy.json"
NICKNAMES_FILE = "nicknames.json"  # <-- кастомные имена {"me": {"peer": "custom_name"}}
PINNED_FILE = "pinned_messages.json"
STORIES_FILE = "stories.json"
HELP_FORUM_FILE = "help_forum.json"
HELP_MODERATORS = ["admin", "moderator", "support"]

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
os.makedirs(os.path.join(UPLOAD_FOLDER, 'images'), exist_ok=True)
os.makedirs(os.path.join(UPLOAD_FOLDER, 'files'),  exist_ok=True)
os.makedirs(os.path.join(UPLOAD_FOLDER, 'avatars'), exist_ok=True)

# Cloudflare R2 (S3-compatible) storage config
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL", "").strip()
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "").strip()
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "").strip()
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "").strip().rstrip("/")
R2_PRESIGN_TTL = int(os.getenv("R2_PRESIGN_TTL", "3600"))

# Backblaze B2 S3-compatible storage config (alternative)
B2_ENDPOINT = os.getenv("B2_ENDPOINT", "").strip()
B2_KEY_ID = os.getenv("B2_KEY_ID", "").strip()
B2_APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY", "").strip()
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME", "").strip()
B2_PUBLIC_BASE_URL = os.getenv("B2_PUBLIC_BASE_URL", "").strip().rstrip("/")
B2_PRESIGN_TTL = int(os.getenv("B2_PRESIGN_TTL", "3600"))

STORAGE_ENDPOINT_URL = R2_ENDPOINT_URL or B2_ENDPOINT
STORAGE_ACCESS_KEY_ID = R2_ACCESS_KEY_ID or B2_KEY_ID
STORAGE_SECRET_ACCESS_KEY = R2_SECRET_ACCESS_KEY or B2_APPLICATION_KEY
STORAGE_BUCKET_NAME = R2_BUCKET_NAME or B2_BUCKET_NAME
STORAGE_PUBLIC_BASE_URL = R2_PUBLIC_BASE_URL or B2_PUBLIC_BASE_URL
STORAGE_PRESIGN_TTL = R2_PRESIGN_TTL if R2_ENDPOINT_URL else B2_PRESIGN_TTL

_r2_client = None
if boto3 and STORAGE_ENDPOINT_URL and STORAGE_ACCESS_KEY_ID and STORAGE_SECRET_ACCESS_KEY and STORAGE_BUCKET_NAME:
    try:
        _r2_client = boto3.client(
            "s3",
            endpoint_url=STORAGE_ENDPOINT_URL,
            aws_access_key_id=STORAGE_ACCESS_KEY_ID,
            aws_secret_access_key=STORAGE_SECRET_ACCESS_KEY,
            region_name="auto"
        )
    except Exception:
        _r2_client = None

_asr_backend = None
_asr_model = None
_sr_backend = None

@app.route('/crypto/<path:filename>')
def serve_crypto(filename):
    return send_from_directory('../crypto', filename)

# ─── Утилиты ────────────────────────────────────────────────────
DB_FILE = "app_data.sqlite3"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
_db_use_postgres = bool(DATABASE_URL and DATABASE_URL.startswith(("postgres://", "postgresql://")) and psycopg)
_db_init_done = False
_db_lock = threading.Lock()

def _load_json_file(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {}

def _load_json_from_legacy_sqlite(path):
    if not os.path.exists(DB_FILE):
        return None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=5, check_same_thread=False)
        try:
            row = conn.execute("SELECT payload FROM json_store WHERE path = ?", (path,)).fetchone()
            if not row or not row[0]:
                return None
            return json.loads(row[0])
        finally:
            conn.close()
    except Exception:
        return None

def _db_connect():
    if _db_use_postgres:
        return psycopg.connect(DATABASE_URL)
    conn = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def _ensure_db():
    global _db_init_done
    if _db_init_done:
        return
    with _db_lock:
        if _db_init_done:
            return
        conn = _db_connect()
        try:
            if _db_use_postgres:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS json_store (
                            path TEXT PRIMARY KEY,
                            payload TEXT NOT NULL,
                            updated_at DOUBLE PRECISION NOT NULL
                        )
                        """
                    )
            else:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS json_store (
                        path TEXT PRIMARY KEY,
                        payload TEXT NOT NULL,
                        updated_at REAL NOT NULL
                    )
                    """
                )
                try:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA synchronous=NORMAL")
                except Exception:
                    pass
            conn.commit()
        finally:
            conn.close()
        _db_init_done = True

def load_json(path):
    p = str(path or "").strip()
    if not p:
        return {}
    _ensure_db()
    for attempt in range(8):
        conn = _db_connect()
        try:
            if _db_use_postgres:
                with conn.cursor() as cur:
                    cur.execute("SELECT payload FROM json_store WHERE path = %s", (p,))
                    row = cur.fetchone()
            else:
                row = conn.execute("SELECT payload FROM json_store WHERE path = ?", (p,)).fetchone()
            if row and row[0]:
                try:
                    return json.loads(row[0])
                except Exception:
                    return {}
            if _db_use_postgres:
                # One-time migration path: if postgres is empty, reuse existing sqlite payload.
                sqlite_data = _load_json_from_legacy_sqlite(p)
                if sqlite_data is not None:
                    payload = json.dumps(sqlite_data, ensure_ascii=False)
                    now_ts = time.time()
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO json_store(path, payload, updated_at)
                            VALUES (%s, %s, %s)
                            ON CONFLICT(path) DO UPDATE
                            SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at
                            """,
                            (p, payload, now_ts)
                        )
                    conn.commit()
                    return sqlite_data
            # Lazy migration from legacy JSON files (first read of each dataset).
            data = _load_json_file(p)
            payload = json.dumps(data, ensure_ascii=False)
            now_ts = time.time()
            if _db_use_postgres:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO json_store(path, payload, updated_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT(path) DO UPDATE
                        SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at
                        """,
                        (p, payload, now_ts)
                    )
            else:
                conn.execute(
                    """
                    INSERT INTO json_store(path, payload, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(path) DO UPDATE
                    SET payload = excluded.payload, updated_at = excluded.updated_at
                    """,
                    (p, payload, now_ts)
                )
            conn.commit()
            return data
        except Exception as e:
            msg = str(e).lower()
            is_retryable = "locked" in msg or "could not serialize" in msg or "deadlock" in msg
            if not is_retryable or attempt == 7:
                raise
            time.sleep(0.05 * (attempt + 1))
        finally:
            conn.close()
    return {}

def save_json(path, data):
    p = str(path or "").strip()
    if not p:
        return
    _ensure_db()
    payload = json.dumps(data, ensure_ascii=False)
    for attempt in range(8):
        conn = _db_connect()
        try:
            now_ts = time.time()
            if _db_use_postgres:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO json_store(path, payload, updated_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT(path) DO UPDATE
                        SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at
                        """,
                        (p, payload, now_ts)
                    )
            else:
                conn.execute(
                    """
                    INSERT INTO json_store(path, payload, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(path) DO UPDATE
                    SET payload = excluded.payload, updated_at = excluded.updated_at
                    """,
                    (p, payload, now_ts)
                )
            conn.commit()
            return
        except Exception as e:
            msg = str(e).lower()
            is_retryable = "locked" in msg or "could not serialize" in msg or "deadlock" in msg
            if not is_retryable or attempt == 7:
                raise
            time.sleep(0.05 * (attempt + 1))
        finally:
            conn.close()

def _r2_enabled():
    return _r2_client is not None

def _r2_object_key(folder, filename):
    return f"uploads/{folder}/{filename}"

def _r2_upload_fileobj(fileobj, folder, filename, content_type="application/octet-stream"):
    key = _r2_object_key(folder, filename)
    _r2_client.upload_fileobj(
        fileobj,
        STORAGE_BUCKET_NAME,
        key,
        ExtraArgs={"ContentType": content_type}
    )
    return key

def _r2_signed_or_public_url(key):
    if STORAGE_PUBLIC_BASE_URL:
        return f"{STORAGE_PUBLIC_BASE_URL}/{key}"
    return _r2_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": STORAGE_BUCKET_NAME, "Key": key},
        ExpiresIn=max(60, STORAGE_PRESIGN_TTL)
    )

def emit_to_user(username, event_name, payload, exclude_sid=None):
    """Отправка Socket.IO-события всем сессиям пользователя."""
    uname = str(username or "").strip().lower()
    if not uname:
        return
    for sid in _online_users.get(uname, set()):
        if exclude_sid and sid == exclude_sid:
            continue
        socketio.emit(event_name, payload, to=sid)

def get_or_create_call(call_id, chat_id=""):
    call = _active_calls.get(call_id)
    if not call:
        call = {
            "chat_id": chat_id,
            "participants": set(),
            "all_participants": set(),
            "invited_targets": set(),
            "initiator": "",
            "created_at": time.time(),
            "started_at": None,
            "connected_once": False,
            "screen_owner": None,
            "screen_track_id": "",
            "allow_draw_all": False,
            "control_allowed": set(),
            "mode": "audio",
            "lone_since": None,
            "timeout_token": 0
        }
        _active_calls[call_id] = call
    return call

def _safe_chat_id(sender, target, explicit_chat_id=None):
    if explicit_chat_id and str(explicit_chat_id).startswith("group_"):
        return explicit_chat_id
    if str(target).startswith("group_"):
        return str(target)
    if explicit_chat_id and "_" not in str(explicit_chat_id):
        target = explicit_chat_id
    s = str(sender or "").lower()
    t = str(target or "").lower()
    if not s or not t:
        return ""
    return "_".join(sorted([s, t]))

def _chat_participants(chat_id):
    cid = str(chat_id or "").strip().lower()
    if not cid:
        return []
    if cid.startswith("group_"):
        groups = load_json(GROUPS_FILE) or {}
        g = groups.get(cid, {})
        return [str(m).strip().lower() for m in g.get("members", []) if str(m).strip()]
    if "_" in cid:
        a, b = cid.split("_", 1)
        return [a.lower(), b.lower()]
    return [cid]

def _cleanup_stories(stories):
    now = time.time()
    if not isinstance(stories, list):
        return []
    return [s for s in stories if float(s.get("expires_at", 0) or 0) > now]

def _story_viewers_for_owner(owner, users, privacy):
    o = str(owner or "").strip().lower()
    if not o:
        return set()
    mode = str((privacy or {}).get(o, {}).get("story_visibility", "friends")).strip().lower()
    if mode == "nobody":
        return {o}
    if mode == "all":
        return set([o] + [str(u).strip().lower() for u in (users or {}).keys() if str(u).strip()])
    owner_friends = set([str(f).strip().lower() for f in (users or {}).get(o, {}).get("friends", []) if str(f).strip()])
    if mode == "mutual":
        mutuals = set()
        for u, ud in (users or {}).items():
            ul = str(u).strip().lower()
            if not ul:
                continue
            uf = set([str(f).strip().lower() for f in ud.get("friends", []) if str(f).strip()])
            if ul in owner_friends and o in uf:
                mutuals.add(ul)
        return mutuals | {o}
    viewers = set(owner_friends)
    # Также разрешаем тем, кто добавил владельца (односторонняя дружба с другой стороны)
    for u, ud in (users or {}).items():
        ul = str(u).strip().lower()
        if not ul:
            continue
        uf = set([str(f).strip().lower() for f in ud.get("friends", []) if str(f).strip()])
        if o in uf:
            viewers.add(ul)
    viewers.add(o)
    return viewers

def _can_view_story(owner, viewer, users, privacy):
    return str(viewer or "").strip().lower() in _story_viewers_for_owner(owner, users, privacy)

def _msg_order_value(mid, fallback_ts=0):
    try:
        s = str(mid or "")
        if "_" in s:
            raw = float(s.split("_")[-1])
            return raw if raw > 1e12 else raw * 1000
    except Exception:
        pass
    try:
        raw = float(fallback_ts or 0)
        return raw if raw > 1e12 else raw * 1000
    except Exception:
        return 0

def _default_group_permissions():
    return {
        "can_send_messages": True,
        "can_send_media": True,
        "can_send_stickers": True,
        "can_send_gifs": True,
        "can_send_voice": True,
        "can_send_video_notes": True,
        "can_send_links": True,
        "can_start_calls": True,
        "can_add_members": False,
        "can_remove_members": False,
        "can_pin_messages": False,
        "can_change_info": False,
        "can_manage_permissions": False
    }

def _normalize_group_permissions(group_info):
    g = dict(group_info or {})
    perms = g.get("permissions")
    defaults = _default_group_permissions()
    if not isinstance(perms, dict):
        perms = {"defaults": dict(defaults), "members": {}}
    else:
        pd = perms.get("defaults")
        if not isinstance(pd, dict):
            pd = {}
        merged = dict(defaults)
        merged.update({k: bool(v) for k, v in pd.items() if k in defaults})
        pm = perms.get("members")
        if not isinstance(pm, dict):
            pm = {}
        members = {}
        for uname, up in pm.items():
            uu = str(uname or "").strip().lower()
            if not uu or not isinstance(up, dict):
                continue
            members[uu] = {k: bool(v) for k, v in up.items() if k in defaults}
        perms = {"defaults": merged, "members": members}
    g["permissions"] = perms
    return g

def _group_member_permission(group_info, username, perm_key):
    g = _normalize_group_permissions(group_info)
    uname = str(username or "").strip().lower()
    owner = str(g.get("owner", "")).strip().lower()
    if not uname:
        return False
    members = {str(m).strip().lower() for m in g.get("members", []) if str(m).strip()}
    if uname == owner:
        return True
    if uname not in members:
        return False
    defaults = g.get("permissions", {}).get("defaults", _default_group_permissions())
    members = g.get("permissions", {}).get("members", {})
    if uname in members and perm_key in members[uname]:
        return bool(members[uname][perm_key])
    return bool(defaults.get(perm_key, False))

def _ensure_group_invite_token(group_info):
    if not isinstance(group_info, dict):
        return ""
    token = str(group_info.get("invite_token", "")).strip()
    if token:
        return token
    token = uuid.uuid4().hex
    group_info["invite_token"] = token
    return token

def _grant_join_access_permissions(perms, member_username):
    pm = perms.setdefault("members", {})
    member = str(member_username or "").strip().lower()
    if not member:
        return perms
    pm.setdefault(member, {})
    # Гарантируем базовый доступ после вступления/добавления.
    pm[member].update({
        "can_send_messages": True,
        "can_send_media": True,
        "can_send_links": True,
        "can_send_stickers": True,
        "can_send_gifs": True,
        "can_send_voice": True,
        "can_send_video_notes": True,
        "can_start_calls": True,
        "can_pin_messages": True
    })
    return perms

def _emit_group_key_needed_to_member(member_user, group_id, target_member):
    o = str(member_user or "").strip().lower()
    gid = str(group_id or "").strip()
    m = str(target_member or "").strip().lower()
    if not o or not gid or not m:
        return
    if o == m:
        return
    emit_to_user(o, "group_key_needed", {"group_id": gid, "member": m})

def _notify_online_members_about_missing_group_keys(member_user):
    o = str(member_user or "").strip().lower()
    if not o:
        return
    groups = load_json(GROUPS_FILE) or {}
    for gid, info in groups.items():
        g = _normalize_group_permissions(info)
        members = [str(m).strip().lower() for m in g.get("members", []) if str(m).strip()]
        if o not in members:
            continue
        enc = g.get("encrypted_keys", {}) if isinstance(g.get("encrypted_keys"), dict) else {}
        if not str(enc.get(o, "")).strip():
            continue
        for m in members:
            if m == o:
                continue
            if not str(enc.get(m, "")).strip():
                _emit_group_key_needed_to_member(o, gid, m)

def _issue_auth_token(username):
    uname = str(username or "").strip().lower()
    if not uname:
        return ""
    token = uuid.uuid4().hex
    _auth_tokens[token] = {
        "username": uname,
        "issued_at": time.time(),
        "last_seen": time.time()
    }
    # Ограничиваем размер in-memory хранилища токенов.
    if len(_auth_tokens) > 5000:
        # Оставляем только новые токены.
        items = sorted(_auth_tokens.items(), key=lambda kv: float(kv[1].get("last_seen", 0)), reverse=True)[:2500]
        _auth_tokens.clear()
        for t, meta in items:
            _auth_tokens[t] = meta
    return token

def _auth_user_from_request(payload=None):
    data = payload if isinstance(payload, dict) else {}
    token = (
        str(request.headers.get("X-Auth-Token", "")).strip()
        or str(data.get("auth_token", "")).strip()
        or str(request.args.get("auth_token", "")).strip()
    )
    if not token:
        return ""
    entry = _auth_tokens.get(token)
    if not isinstance(entry, dict):
        return ""
    # TTL токена: 30 дней.
    now = time.time()
    if now - float(entry.get("issued_at", 0) or 0) > 30 * 24 * 3600:
        _auth_tokens.pop(token, None)
        return ""
    entry["last_seen"] = now
    return str(entry.get("username", "")).strip().lower()

def _forum_rate_check(username, action):
    uname = str(username or "").strip().lower()
    if not uname:
        return False
    now = time.time()
    entry = _forum_rate.get(uname)
    if not isinstance(entry, dict):
        entry = {"topic": [], "reply": []}
        _forum_rate[uname] = entry
    topic_hist = [float(t) for t in entry.get("topic", []) if now - float(t) <= 300]
    reply_hist = [float(t) for t in entry.get("reply", []) if now - float(t) <= 120]
    entry["topic"] = topic_hist
    entry["reply"] = reply_hist
    if action == "topic":
        # Макс 3 темы за 5 минут.
        if len(topic_hist) >= 3:
            return False
        topic_hist.append(now)
        entry["topic"] = topic_hist
        return True
    if action == "reply":
        # Макс 12 ответов за 2 минуты.
        if len(reply_hist) >= 12:
            return False
        reply_hist.append(now)
        entry["reply"] = reply_hist
        return True
    return False

def _emit_call_state(call_id):
    call = _active_calls.get(call_id)
    if not call:
        return
    participants = list(call.get("participants", set()))
    payload = {
        "call_id": call_id,
        "chat_id": call.get("chat_id", ""),
        "active": True,
        "participants": participants,
        "mode": call.get("mode", "audio"),
        "screen_owner": call.get("screen_owner"),
        "lone_since": call.get("lone_since")
    }
    recipients = set(call.get("all_participants", set())) | set(call.get("invited_targets", set())) | set(participants)
    for uname in recipients:
        emit_to_user(uname, "call_state", payload)

def _emit_call_state_to_user(call_id, username):
    call = _active_calls.get(call_id)
    if not call or not username:
        return
    participants = list(call.get("participants", set()))
    payload = {
        "call_id": call_id,
        "chat_id": call.get("chat_id", ""),
        "active": True,
        "participants": participants,
        "mode": call.get("mode", "audio"),
        "screen_owner": call.get("screen_owner"),
        "lone_since": call.get("lone_since")
    }
    emit_to_user(username, "call_state", payload)

def _emit_call_state_ended(call):
    payload = {
        "call_id": call.get("id"),
        "chat_id": call.get("chat_id", ""),
        "active": False,
        "participants": [],
        "mode": call.get("mode", "audio"),
        "screen_owner": None,
        "lone_since": None
    }
    recipients = set(call.get("all_participants", set())) | set(call.get("invited_targets", set()))
    for uname in recipients:
        emit_to_user(uname, "call_state", payload)

def _send_system_event_message(chat_id, recipients, text):
    if not chat_id or not recipients or not text:
        return
    msgs = load_json(MESSAGES_FILE) or {}
    packet = {
        "id": f"msg_{int(time.time() * 1000)}",
        "from": "system",
        "to": chat_id,
        "type": "system_event",
        "text": text,
        "time": time.strftime("%H:%M"),
        "edited": False,
        "status": "delivered",
        "timestamp": time.time()
    }
    msgs.setdefault(chat_id, []).append(packet)
    save_json(MESSAGES_FILE, msgs)

    sent = set()
    for uname in recipients:
        for sid in _online_users.get(uname, set()):
            if sid not in sent:
                socketio.emit("new_message", packet, to=sid)
                sent.add(sid)

def _send_call_event_message(chat_id, sender, recipients, text):
    _send_system_event_message(chat_id, recipients, text)

def _fmt_duration(sec):
    total = max(0, int(sec))
    m = total // 60
    s = total % 60
    return f"{m:02d}:{s:02d}"

def _finish_call(call_id, ended_by="", reason="ended"):
    call = _active_calls.get(call_id)
    if not call:
        return
    participants = list(call.get("participants", set()))
    call["id"] = call_id
    # Уведомляем клиентов об окончании звонка
    for peer in set(call.get("all_participants", set())) | set(call.get("invited_targets", set())) | set(participants):
        emit_to_user(peer, "call_ended", {"call_id": call_id, "ended_by": ended_by, "reason": reason})

    chat_id = call.get("chat_id", "")
    sender = ended_by or call.get("initiator") or (participants[0] if participants else "") or "system"

    # События в истории чата
    if chat_id:
        recipients = set(call.get("all_participants", set())) | set(call.get("invited_targets", set()))
        if call.get("connected_once") and call.get("started_at"):
            dur = _fmt_duration(time.time() - float(call.get("started_at")))
            _send_call_event_message(chat_id, sender, recipients, f"📞 Звонок длился {dur}")
        else:
            targets = set(call.get("invited_targets", set())) - {sender}
            if targets:
                _send_call_event_message(chat_id, sender, recipients, "📵 Пропущенный звонок")

    _emit_call_state_ended(call)
    _active_calls.pop(call_id, None)

def _refresh_lone_timer(call_id):
    call = _active_calls.get(call_id)
    if not call:
        return
    participants = call.get("participants", set())

    # Если хотя бы 2 участника — сбрасываем таймер одиночества
    if len(participants) >= 2:
        call["lone_since"] = None
        call["timeout_token"] = int(call.get("timeout_token", 0)) + 1
        if not call.get("connected_once"):
            call["connected_once"] = True
        if not call.get("started_at"):
            call["started_at"] = time.time()
        _emit_call_state(call_id)
        return

    # Если 1 участник — даём 3 минуты ожидания (и до первого ответа, и после выхода)
    if len(participants) == 1:
        call["lone_since"] = time.time()
        token = int(call.get("timeout_token", 0)) + 1
        call["timeout_token"] = token
        _emit_call_state(call_id)

        def _timeout_task(cid, tkn):
            socketio.sleep(180)
            c = _active_calls.get(cid)
            if not c:
                return
            if int(c.get("timeout_token", 0)) != tkn:
                return
            if len(c.get("participants", set())) <= 1:
                _finish_call(cid, ended_by="", reason="lonely_timeout")

        socketio.start_background_task(_timeout_task, call_id, token)
        return

    # 0 участников
    if len(participants) == 0:
        _finish_call(call_id, ended_by="", reason="empty")

def get_avatar(username):
    """Получить аватарку из avatars.json"""
    avatars = load_json(AVATARS_FILE)
    return avatars.get(username, "")

def set_avatar(username, base64_data):
    """Сохранить аватарку в avatars.json"""
    avatars = load_json(AVATARS_FILE)
    avatars[username] = base64_data
    save_json(AVATARS_FILE, avatars)

def _ensure_asr_model():
    global _asr_backend, _asr_model
    if _asr_model is not None:
        return _asr_backend, _asr_model
    model_name = os.environ.get("LEVART_ASR_MODEL", "base")
    try:
        import whisper
        _asr_model = whisper.load_model(model_name)
        _asr_backend = "whisper"
        return _asr_backend, _asr_model
    except Exception:
        pass
    try:
        from faster_whisper import WhisperModel
        _asr_model = WhisperModel(model_name, device="cpu", compute_type="int8")
        _asr_backend = "faster_whisper"
        return _asr_backend, _asr_model
    except Exception:
        pass
    raise RuntimeError("ASR model unavailable: install whisper or faster-whisper")

def _transcribe_with_speech_recognition(path, lang="ru"):
    global _sr_backend
    try:
        import speech_recognition as sr
    except Exception:
        return ""
    wav_path = path
    tmp_wav = None
    if not str(path).lower().endswith(".wav"):
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            return ""
        tmp_wav = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
        tmp_wav.close()
        subprocess.run(
            [ffmpeg, "-y", "-i", path, "-ac", "1", "-ar", "16000", tmp_wav.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        wav_path = tmp_wav.name
    try:
        r = sr.Recognizer()
        with sr.AudioFile(wav_path) as source:
            audio = r.record(source)
        text = r.recognize_google(audio, language="ru-RU" if lang.startswith("ru") else "en-US")
        _sr_backend = "speech_recognition"
        return str(text or "").strip()
    except Exception:
        return ""
    finally:
        if tmp_wav and os.path.exists(tmp_wav.name):
            try:
                os.remove(tmp_wav.name)
            except Exception:
                pass

def _transcribe_local_audio(path, lang="ru"):
    backend, model = _ensure_asr_model()
    attempts = [lang or None, None]
    best_text = ""
    for lng in attempts:
        try:
            if backend == "whisper":
                result = model.transcribe(
                    path,
                    language=lng,
                    task="transcribe",
                    fp16=False,
                    verbose=False,
                    temperature=0.0,
                    best_of=5,
                    beam_size=5
                )
                text = str((result or {}).get("text", "")).strip()
            elif backend == "faster_whisper":
                segments, _info = model.transcribe(
                    path,
                    language=lng,
                    vad_filter=True,
                    beam_size=5,
                    best_of=5,
                    temperature=0.0
                )
                text = " ".join([str(seg.text).strip() for seg in segments if str(seg.text).strip()]).strip()
            else:
                text = ""
            if text and len(text) > len(best_text):
                best_text = text
            if best_text and len(best_text) >= 8:
                break
        except Exception:
            continue
    if best_text:
        return best_text
    return _transcribe_with_speech_recognition(path, lang=lang)

# ─── Авторизация ─────────────────────────────────────────────────
@app.route("/info")
def landing(): return render_template("landing.html")

@app.route("/register", methods=["GET", "POST"])
def register_page():
    if request.method == "POST":
        data = request.json
        users = load_json(USERS_FILE)
        username = data.get("username", "").strip().lower()
        if username in users:
            return jsonify({"error": "Пользователь уже существует"}), 400
        users[username] = {
            "password":   data["password"],
            "public_key": data["public_key"],
            "first_name": data.get("first_name", ""),
            "last_name":  data.get("last_name", ""),
            "bio":        "",
            "birthdate":  "",
            "friends":    []
        }
        save_json(USERS_FILE, users)
        auth_token = _issue_auth_token(username)
        # Аватар при регистрации не нужен — он будет в avatars.json
        return jsonify({"status": "ok", "auth_token": auth_token})
    return render_template("register.html")

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        data = request.json
        users = load_json(USERS_FILE)
        u = data.get("username", "").strip().lower()
        p = data.get("password", "")
        if not p:
            return jsonify({"error": "Пароль не указан"}), 400
        if u not in users or users[u]["password"] != p:
            return jsonify({"error": "Неверный логин или пароль"}), 401
        auth_token = _issue_auth_token(u)
        return jsonify({"status": "ok", "public_key": users[u]["public_key"], "auth_token": auth_token})
    return render_template("login.html")

@app.route("/")
def messenger(): return render_template("messenger.html")

@app.route("/help")
def help_center_page():
    return render_template("help.html")

@app.route("/api/help/forum")
def help_forum_feed():
    me = str(request.args.get("me", "")).strip().lower()
    data = load_json(HELP_FORUM_FILE) or {}
    topics = data.get("topics", []) if isinstance(data, dict) else []
    if not isinstance(topics, list):
        topics = []
    # newest first
    topics = sorted(topics, key=lambda t: float(t.get("created_at", 0) or 0), reverse=True)
    return jsonify({"me": me, "topics": topics[:200]})

@app.route("/api/help/forum_topic", methods=["POST"])
def help_forum_topic_create():
    data = request.json or {}
    author = _auth_user_from_request(data)
    title = str(data.get("title", "")).strip()
    body = str(data.get("body", "")).strip()
    if not author:
        return jsonify({"error": "auth_required"}), 401
    if not title or not body:
        return jsonify({"error": "bad_request"}), 400
    if len(title) > 160 or len(body) > 5000:
        return jsonify({"error": "too_long"}), 400
    if not _forum_rate_check(author, "topic"):
        return jsonify({"error": "rate_limited"}), 429
    forum = load_json(HELP_FORUM_FILE) or {}
    if not isinstance(forum, dict):
        forum = {}
    forum.setdefault("topics", [])
    topic = {
        "id": f"topic_{int(time.time() * 1000)}",
        "author": author,
        "title": title,
        "body": body,
        "created_at": time.time(),
        "updated_at": time.time(),
        "replies": []
    }
    forum["topics"].insert(0, topic)
    if len(forum["topics"]) > 500:
        forum["topics"] = forum["topics"][:500]
    save_json(HELP_FORUM_FILE, forum)
    return jsonify({"status": "ok", "topic": topic})

@app.route("/api/help/forum_reply", methods=["POST"])
def help_forum_reply_create():
    data = request.json or {}
    author = _auth_user_from_request(data)
    topic_id = str(data.get("topic_id", "")).strip()
    body = str(data.get("body", "")).strip()
    if not author:
        return jsonify({"error": "auth_required"}), 401
    if not topic_id or not body:
        return jsonify({"error": "bad_request"}), 400
    if len(body) > 5000:
        return jsonify({"error": "too_long"}), 400
    if not _forum_rate_check(author, "reply"):
        return jsonify({"error": "rate_limited"}), 429
    forum = load_json(HELP_FORUM_FILE) or {}
    topics = forum.get("topics", []) if isinstance(forum, dict) else []
    if not isinstance(topics, list):
        return jsonify({"error": "not_found"}), 404
    for t in topics:
        if str(t.get("id", "")) != topic_id:
            continue
        replies = t.get("replies", [])
        if not isinstance(replies, list):
            replies = []
        reply = {
            "id": f"reply_{int(time.time() * 1000)}",
            "author": author,
            "body": body,
            "created_at": time.time(),
            "is_moderator": author in HELP_MODERATORS
        }
        replies.append(reply)
        t["replies"] = replies[-200:]
        t["updated_at"] = time.time()
        save_json(HELP_FORUM_FILE, forum)
        return jsonify({"status": "ok", "reply": reply})
    return jsonify({"error": "not_found"}), 404

@app.route("/invite/<token>")
def invite_entry(token):
    token_s = str(token or "").strip().lower()
    groups = load_json(GROUPS_FILE) or {}
    for gid, info in groups.items():
        if str(info.get("invite_token", "")).strip().lower() == token_s:
            return redirect(f"/?invite={token_s}")
    return "Invite link is invalid or expired", 404

@app.route("/api/group_join_by_invite", methods=["POST"])
def group_join_by_invite():
    data = request.json or {}
    username = str(data.get("username", "")).strip().lower()
    token_s = str(data.get("token", "")).strip().lower()
    if not username or not token_s:
        return jsonify({"error": "bad_request"}), 400
    users = load_json(USERS_FILE) or {}
    if username not in users:
        return jsonify({"error": "user_not_found"}), 404

    groups = load_json(GROUPS_FILE) or {}
    group_id = ""
    group_info = {}
    for gid, info in groups.items():
        if str(info.get("invite_token", "")).strip().lower() == token_s:
            group_id = gid
            group_info = info
            break

    if not group_id:
        return jsonify({"error": "invalid_invite"}), 404

    members = [str(m).strip().lower() for m in group_info.get("members", []) if str(m).strip()]
    already_member = username in members
    changed = False
    if not already_member:
        members.append(username)
        groups[group_id]["members"] = members
        changed = True

    # Всегда гарантируем права на отправку после входа по инвайту.
    g = _normalize_group_permissions(groups[group_id])
    perms = g.get("permissions", {})
    pm = perms.setdefault("members", {})
    before_perms = dict(pm.get(username, {}))
    _grant_join_access_permissions(perms, username)
    if before_perms != pm.get(username, {}):
        changed = True
    groups[group_id]["permissions"] = perms

    # Если у участника еще нет ключа группы, владелец получает запрос на выдачу.
    has_key_for_user = bool((groups[group_id].get("encrypted_keys") or {}).get(username))
    owner = str(groups[group_id].get("owner", "")).strip().lower()
    need_key_emit = bool(not has_key_for_user)

    if changed:
        save_json(GROUPS_FILE, groups)

    if not already_member:
        msgs = load_json(MESSAGES_FILE) or {}
        sys_packet = {
            "id": f"msg_{int(time.time() * 1000)}",
            "from": "system",
            "to": group_id,
            "type": "call_event",
            "text": f"➕ @{username} вступил(а) в группу по ссылке-приглашению",
            "time": time.strftime("%H:%M"),
            "edited": False,
            "status": "delivered",
            "timestamp": time.time()
        }
        msgs.setdefault(group_id, []).append(sys_packet)
        save_json(MESSAGES_FILE, msgs)

        sent_sids = set()
        for member in members:
            emit_to_user(member, "group_members_updated", {"group_id": group_id, "removed": False})
            for sid in _online_users.get(member, set()):
                if sid in sent_sids:
                    continue
                socketio.emit("new_message", sys_packet, to=sid)
                sent_sids.add(sid)

    if need_key_emit:
        enc = groups[group_id].get("encrypted_keys", {}) if isinstance(groups[group_id].get("encrypted_keys"), dict) else {}
        for grantor in members:
            gu = str(grantor or "").strip().lower()
            if not gu or gu == username:
                continue
            if not str(enc.get(gu, "")).strip():
                continue
            _emit_group_key_needed_to_member(gu, group_id, username)

    return jsonify({
        "status": "ok",
        "group_id": group_id,
        "already_member": already_member
    })

@app.route("/api/group_member_key/set", methods=["POST"])
def set_group_member_key():
    data = request.json or {}
    group_id = str(data.get("group_id", "")).strip()
    actor = _auth_user_from_request(data)
    target = str(data.get("target", "")).strip().lower()
    encrypted_key = data.get("encrypted_key")
    if not actor:
        return jsonify({"error": "auth_required"}), 401
    if not group_id or not target:
        return jsonify({"error": "bad_request"}), 400
    if isinstance(encrypted_key, dict):
        cipher = str(encrypted_key.get("cipher", "")).strip()
        by = str(encrypted_key.get("by", "")).strip().lower()
        if not cipher or not by:
            return jsonify({"error": "bad_request"}), 400
    elif isinstance(encrypted_key, str):
        encrypted_key = encrypted_key.strip()
        if not encrypted_key:
            return jsonify({"error": "bad_request"}), 400
    else:
        return jsonify({"error": "bad_request"}), 400
    groups = load_json(GROUPS_FILE) or {}
    g = groups.get(group_id)
    if not g:
        return jsonify({"error": "not_found"}), 404
    g = _normalize_group_permissions(g)
    owner = str(g.get("owner", "")).strip().lower()
    members = [str(m).strip().lower() for m in g.get("members", []) if str(m).strip()]
    if actor not in members:
        return jsonify({"error": "forbidden"}), 403
    enc = g.get("encrypted_keys", {}) if isinstance(g.get("encrypted_keys"), dict) else {}
    if actor != owner and not str(enc.get(actor, "")).strip():
        return jsonify({"error": "forbidden"}), 403
    if isinstance(encrypted_key, dict):
        by = str(encrypted_key.get("by", "")).strip().lower()
        if by != actor:
            return jsonify({"error": "forbidden"}), 403
    if target not in members:
        return jsonify({"error": "member_not_found"}), 404
    g.setdefault("encrypted_keys", {})
    g["encrypted_keys"][target] = encrypted_key
    groups[group_id] = g
    save_json(GROUPS_FILE, groups)
    emit_to_user(target, "group_key_updated", {"group_id": group_id})
    return jsonify({"status": "ok"})

# ─── Поиск ───────────────────────────────────────────────────────
@app.route("/search")
def search():
    query = request.args.get("q", "").lower().strip()
    me    = request.args.get("me", "").strip().lower()
    if not query:
        return jsonify([])

    users    = load_json(USERS_FILE)
    my_data  = users.get(me, {})
    friends  = [f.lower() for f in my_data.get("friends", [])]
    all_msgs = load_json(MESSAGES_FILE) or {}
    groups   = load_json(GROUPS_FILE) or {}

    # С кем переписывались
    contacted = set()
    for chat_id in all_msgs:
        if not chat_id.startswith("group_") and me in chat_id.lower() and "_" in chat_id:
            parts = chat_id.split("_")
            other = parts[1] if parts[0].lower() == me else parts[0]
            contacted.add(other.lower())

    def user_to_item(u, is_friend, has_chatted):
        ud = users.get(u, {})
        first = ud.get("first_name", "")
        last  = ud.get("last_name", "")
        display = f"{first} {last}".strip() or u
        
        # Учитываем кастомное имя которое me поставил этому пользователю
        nicks = load_json(NICKNAMES_FILE)
        custom_name = nicks.get(me, {}).get(u.lower(), "")
        if custom_name:
            display = custom_name
        
        return {
            "username":     u,
            "first_name":   first,
            "last_name":    last,
            "display_name": display,
            "is_group":     False,
            "is_friend":    is_friend,
            "has_chatted":  has_chatted,
            "avatar":       get_avatar(u)
        }

    results = []

    if query.startswith("@"):
        # Ищем всех по никнейму
        target = query[1:]
        for u in users:
            if u == me: continue
            if target in u.lower():
                results.append(user_to_item(u, u in friends, u in contacted))
    else:
        # Поиск среди знакомых
        nicks = load_json(NICKNAMES_FILE)
        my_nicks = nicks.get(me, {})  # Мои кастомные имена
        seen = set()
        
        for u in users:
            if u == me: continue
            is_friend   = u.lower() in friends
            has_chatted = u.lower() in contacted
            if not (is_friend or has_chatted):
                continue

            ud    = users[u]
            first = ud.get("first_name", "").lower()
            last  = ud.get("last_name", "").lower()
            full  = f"{first} {last}".strip()
            custom = my_nicks.get(u.lower(), "").lower()  # кастомное имя
            uname  = u.lower()
            
            if (query in uname or query in first or query in last 
                    or query in full or (custom and query in custom)):
                if u not in seen:
                    seen.add(u)
                    results.append(user_to_item(u, is_friend, has_chatted))

        # Группы
        for gid, info in groups.items():
            members = [m.lower() for m in info.get("members", [])]
            if me in members and query in info.get("name", "").lower():
                results.append({
                    "username":     gid,
                    "display_name": info["name"],
                    "is_group":     True,
                    "is_friend":    False,
                    "has_chatted":  True,
                    "avatar":       get_avatar(gid) or info.get("avatar", "")
                })

    return jsonify(results)


# ─── Друзья ──────────────────────────────────────────────────────
@app.route("/add_friend", methods=["POST"])
def add_friend():
    data = request.json
    me     = data.get("me", "").lower()
    friend = data.get("friend", "").lower()
    users  = load_json(USERS_FILE)
    if me not in users or friend not in users:
        return jsonify({"success": False, "error": "User not found"}), 404
    if "friends" not in users[me]:
        users[me]["friends"] = []
    if friend not in users[me]["friends"]:
        users[me]["friends"].append(friend)
        save_json(USERS_FILE, users)
    return jsonify({"success": True})

@app.route("/remove_friend", methods=["POST"])
def remove_friend():
    data = request.json
    me     = data.get("me", "").lower()
    friend = data.get("friend", "").lower()
    users  = load_json(USERS_FILE)
    if me in users:
        users[me]["friends"] = [f for f in users[me].get("friends", []) if f != friend]
        save_json(USERS_FILE, users)
    return jsonify({"success": True})

@app.route("/get_friends")
def get_friends():
    user = request.args.get("username", "")
    users = load_json(USERS_FILE)
    return jsonify(users.get(user, {}).get("friends", []))

@app.route("/api/user_pubkey/<username>")
def get_pubkey(username):
    users = load_json(USERS_FILE)
    if username in users:
        return jsonify({"public_key": users[username]["public_key"]})
    return jsonify({"error": "User not found"}), 404


# ─── Профиль ─────────────────────────────────────────────────────
@app.route("/api/user_profile/<target>")
def get_user_profile(target):
    users   = load_json(USERS_FILE)
    privacy = load_json(PRIVACY_FILE)
    requester = request.args.get("me", target)

    if target not in users:
        return jsonify({"error": "Не найден"}), 404

    u  = users[target]
    bv = privacy.get(target, {}).get("birthdate_visibility", "friends")

    # Проверяем видимость даты рождения
    if bv == "all":
        bd_visible = True
    elif bv == "friends":
        req_friends = [f.lower() for f in users.get(requester, {}).get("friends", [])]
        bd_visible = target.lower() in req_friends or requester == target
    else:
        bd_visible = (requester == target)

    return jsonify({
        "username":         target,
        "first_name":       u.get("first_name", ""),
        "last_name":        u.get("last_name", ""),
        "bio":              u.get("bio", ""),
        "birthdate":        u.get("birthdate", "") if bd_visible else "",
        "birthdate_visible": bd_visible,
        "avatar":           get_avatar(target)
    })

@app.route("/api/user_profiles_batch", methods=["POST"])
def get_user_profiles_batch():
    data = request.json or {}
    requester = str(data.get("me", "")).strip().lower()
    users_req = data.get("users", [])
    if not isinstance(users_req, list):
        return jsonify({"error": "bad_request"}), 400
    users_db = load_json(USERS_FILE) or {}
    privacy = load_json(PRIVACY_FILE) or {}
    out = {}
    for raw_u in users_req[:300]:
        target = str(raw_u or "").strip().lower()
        if not target or target not in users_db:
            continue
        u = users_db[target]
        bv = privacy.get(target, {}).get("birthdate_visibility", "friends")
        if bv == "all":
            bd_visible = True
        elif bv == "friends":
            req_friends = [f.lower() for f in users_db.get(requester, {}).get("friends", [])]
            bd_visible = target in req_friends or requester == target
        else:
            bd_visible = requester == target
        out[target] = {
            "username": target,
            "first_name": u.get("first_name", ""),
            "last_name": u.get("last_name", ""),
            "bio": u.get("bio", ""),
            "birthdate": u.get("birthdate", "") if bd_visible else "",
            "birthdate_visible": bd_visible,
            "avatar": get_avatar(target)
        }
    return jsonify({"profiles": out})

@app.route("/api/update_profile", methods=["POST"])
def update_profile():
    data = request.json
    username = data.get("username", "").strip().lower()
    users = load_json(USERS_FILE)
    if username not in users:
        return jsonify({"error": "Пользователь не найден"}), 404

    users[username]["first_name"] = data.get("first_name", "")
    users[username]["last_name"]  = data.get("last_name", "")
    users[username]["bio"]        = data.get("bio", "")
    users[username]["birthdate"]  = data.get("birthdate", "")
    save_json(USERS_FILE, users)

    # Аватар — отдельно
    avatar = data.get("avatar", "")
    if avatar:
        set_avatar(username, avatar)

    return jsonify({"status": "ok"})

@app.route("/api/privacy_settings/<username>")
def get_privacy_settings(username):
    privacy = load_json(PRIVACY_FILE)
    u = str(username or "").strip().lower()
    cfg = privacy.get(u, {})
    return jsonify({
        "birthdate_visibility": cfg.get("birthdate_visibility", "friends"),
        "story_visibility": cfg.get("story_visibility", "friends")
    })

@app.route("/api/update_privacy", methods=["POST"])
def update_privacy():
    data = request.json
    username = str(data.get("username", "")).strip().lower()
    privacy  = load_json(PRIVACY_FILE)
    if username not in privacy:
        privacy[username] = {}
    privacy[username]["birthdate_visibility"] = data.get("birthdate_visibility", "friends")
    privacy[username]["story_visibility"] = data.get("story_visibility", "friends")
    save_json(PRIVACY_FILE, privacy)
    return jsonify({"status": "ok"})

@app.route("/api/change_password", methods=["POST"])
def change_password():
    data = request.json
    username     = data.get("username")
    old_password = data.get("old_password")
    new_password = data.get("new_password")
    users = load_json(USERS_FILE)
    if username not in users:
        return jsonify({"error": "Не найден"}), 404
    if users[username]["password"] != old_password:
        return jsonify({"error": "Неверный пароль"}), 403
    users[username]["password"] = new_password
    save_json(USERS_FILE, users)
    return jsonify({"status": "ok"})

@app.route("/api/delete_account", methods=["POST"])
def delete_account():
    data = request.json
    username = data.get("username")
    password = data.get("password")
    users = load_json(USERS_FILE)
    if username not in users:
        return jsonify({"error": "Не найден"}), 404
    if users[username]["password"] != password:
        return jsonify({"error": "Неверный пароль"}), 403
    del users[username]
    save_json(USERS_FILE, users)
    # Удалить аватар
    avatars = load_json(AVATARS_FILE)
    if username in avatars:
        del avatars[username]
        save_json(AVATARS_FILE, avatars)
    return jsonify({"status": "ok"})


# ─── Кастомные имена (переименование чата) ───────────────────────
@app.route("/api/set_nickname", methods=["POST"])
def set_nickname():
    """Пользователь me ставит custom_name для peer"""
    data = request.json
    me      = data.get("me", "").lower()
    peer    = data.get("peer", "").lower()
    name    = data.get("name", "").strip()
    nicks   = load_json(NICKNAMES_FILE)
    if me not in nicks:
        nicks[me] = {}
    if name:
        nicks[me][peer] = name
    elif peer in nicks[me]:
        del nicks[me][peer]
    save_json(NICKNAMES_FILE, nicks)
    return jsonify({"status": "ok"})

@app.route("/api/get_nickname")
def get_nickname():
    me   = request.args.get("me", "").lower()
    peer = request.args.get("peer", "").lower()
    nicks = load_json(NICKNAMES_FILE)
    return jsonify({"name": nicks.get(me, {}).get(peer, "")})


# ─── Контакты ────────────────────────────────────────────────────
@app.route("/api/my_contacts/<username>")
def get_contacts(username):
    try:
        all_msgs  = load_json(MESSAGES_FILE) or {}
        groups    = load_json(GROUPS_FILE) or {}
        users_db  = load_json(USERS_FILE) or {}
        avatars   = load_json(AVATARS_FILE) or {}
        nicks_db  = load_json(NICKNAMES_FILE) or {}
        me        = str(username).strip().lower()
        my_nicks  = nicks_db.get(me, {})
        contacts_map = {}

        def extract_time(msgs_list):
            if not msgs_list: return 0.0
            last = msgs_list[-1]
            mid = str(last.get('id', ''))
            if "_" in mid:
                try: return float(mid.split("_")[1])
                except: pass
            return float(last.get('timestamp', 0))

        def get_display_name(peer, user_data):
            """Имя с учётом кастомного псевдонима"""
            custom = my_nicks.get(peer.lower(), "")
            if custom:
                return custom
            first = user_data.get("first_name", "")
            last  = user_data.get("last_name", "")
            return f"{first} {last}".strip() or peer

        # Друзья сначала
        for f in users_db.get(username, {}).get("friends", []):
            fl = f.lower()
            ud = users_db.get(fl, {})
            contacts_map[fl] = {
                "username":    f,
                "display_name": get_display_name(f, ud),
                "avatar":      avatars.get(fl, ""),
                "is_group":    False,
                "is_friend":   True,
                "last_time":   0.0,
                "last_message_preview": "Нет сообщений"
            }

        def get_last_preview(msgs_list, is_group=False):
            """Возвращает текст последнего сообщения для превью"""
            if not msgs_list:
                return "Нет сообщений"
            last = msgs_list[-1]
            # Имя отправителя для группы
            sender_prefix = ""
            if is_group:
                sender = last.get("from", "")
                sender_prefix = f"{sender}: " if sender else ""
            # Тип сообщения
            msg_type = last.get("type", "text")
            if msg_type == "image":
                return sender_prefix + "🖼 Фото"
            if msg_type == "file":
                fname = last.get("file_name", "Файл")
                return sender_prefix + f"📄 {fname}"
            # Текстовое — cipher есть, но оригинал не расшифровать на сервере
            # Показываем что-то осмысленное
            if last.get("cipher"):
                return sender_prefix + "🔒 Сообщение"
            text = last.get("text", "")
            if text:
                return sender_prefix + (text[:40] + "…" if len(text) > 40 else text)
            return sender_prefix + "Сообщение"

        # Все чаты
        for chat_id, msgs in all_msgs.items():
            t   = extract_time(msgs)
            cid = chat_id.lower()

            if cid.startswith("group_"):
                g_info  = groups.get(chat_id, {})
                members = [str(m).lower() for m in g_info.get("members", [])]
                if me in members:
                    if chat_id not in contacts_map or t > contacts_map[chat_id]['last_time']:
                        # Аватар группы — из avatars.json под ключом group_id
                        g_avatar = avatars.get(chat_id, "")
                        # Если нет в avatars.json, смотрим в groups.json (legacy)
                        if not g_avatar:
                            g_avatar = g_info.get("avatar", "")
                        contacts_map[chat_id] = {
                            "username":     chat_id,
                            "display_name": g_info.get("name", "Группа"),
                            "avatar":       g_avatar,
                            "is_group":     True,
                            "last_time":    t,
                            "last_message_preview": get_last_preview(msgs, is_group=True),
                            "owner":        g_info.get("owner", "")
                        }
            elif me in cid and "_" in chat_id:
                parts = chat_id.split("_")
                other = parts[1] if parts[0].lower() == me else parts[0]
                ol    = other.lower()
                ud    = users_db.get(ol, {})
                if ol not in contacts_map or t > contacts_map[ol]['last_time']:
                    contacts_map[ol] = {
                        "username":     other,
                        "display_name": get_display_name(other, ud),
                        "avatar":       avatars.get(ol, ""),
                        "is_group":     False,
                        "is_friend":    ol in [f.lower() for f in users_db.get(me, {}).get("friends", [])],
                        "last_time":    t,
                        "last_message_preview": get_last_preview(msgs, is_group=False),
                    }

        # ── Подсчёт непрочитанных для каждого контакта ──────────────
        last_read_data = load_json(LAST_READ_FILE) or {}
        my_last_read   = last_read_data.get(me, {})

        for contact in contacts_map.values():
            peer = contact["username"]
            chat_key = peer if contact.get("is_group") else "_".join(sorted([me, peer.lower()]))
            msgs_list = all_msgs.get(chat_key, [])
            lr_ts = float(my_last_read.get(peer, my_last_read.get(peer.lower(), 0)))

            unread = 0
            for msg in reversed(msgs_list):
                if str(msg.get("from", "")).lower() == me:
                    break
                mid = str(msg.get("id", ""))
                if "_" in mid:
                    try:
                        raw = float(mid.split("_")[-1])
                        # ID = Date.now() на клиенте = миллисекунды, конвертируем в секунды
                        msg_ts = raw / 1000.0 if raw > 1e11 else raw
                    except:
                        msg_ts = 0.0
                else:
                    msg_ts = float(msg.get("timestamp", 0))
                if msg_ts > lr_ts:
                    unread += 1
                else:
                    break
            contact["unread_count"] = unread

        result = sorted(contacts_map.values(), key=lambda x: float(x['last_time']), reverse=True)
        return jsonify(result)

    except Exception as e:
        print(f"Ошибка get_contacts: {e}")
        return jsonify([])


# ─── Сообщения ───────────────────────────────────────────────────
@app.route("/get_history")
def get_history():
    me     = request.args.get("me")
    friend = request.args.get("friend")
    limit  = request.args.get("limit", type=int)
    msgs   = load_json(MESSAGES_FILE)
    if friend and friend.startswith("group_"):
        chat_id = friend
    else:
        chat_id = "_".join(sorted([me, friend]))
    result = msgs.get(chat_id, [])
    if limit and limit > 0:
        result = result[-limit:]
    return jsonify(result)

@socketio.on("connect")
def handle_connect():
    pass

@socketio.on("user_online")
def handle_user_online(data):
    from flask import request as req
    uname = str(data.get("username", "")).strip().lower()
    if not uname:
        return
    sid = req.sid
    _sid_to_user[sid] = uname
    if uname not in _online_users:
        _online_users[uname] = set()
    _online_users[uname].add(sid)
    emit("user_status", {"username": uname, "online": True}, broadcast=True)

    # Отправляем состояние активных звонков, чтобы восстановить после перезагрузки
    for call_id, call in _active_calls.items():
        if uname in call.get("participants", set()) or uname in call.get("invited_targets", set()) or uname in call.get("all_participants", set()):
            _emit_call_state_to_user(call_id, uname)
    # Если пользователь — владелец групп, где у участников отсутствуют ключи,
    # отправляем запросы на выдачу ключа сразу после его входа.
    _notify_online_members_about_missing_group_keys(uname)

@socketio.on("disconnect")
def handle_disconnect():
    from flask import request as req
    sid = req.sid
    uname = _sid_to_user.pop(sid, None)
    if uname:
        sids = _online_users.get(uname, set())
        sids.discard(sid)
        still_online = len(sids) > 0

        # Удаляем пользователя из активных звонков только когда у него не осталось ни одной активной сессии
        if not still_online:
            for call_id, call in list(_active_calls.items()):
                if uname in call.get("participants", set()):
                    call["participants"].discard(uname)
                    call["control_allowed"].discard(uname)
                    if call.get("screen_owner") == uname:
                        call["screen_owner"] = None
                    for peer in call.get("participants", set()):
                        emit_to_user(peer, "call_user_left", {"call_id": call_id, "username": uname})
                    _refresh_lone_timer(call_id)
                    if call_id in _active_calls:
                        _emit_call_state(call_id)

        if not sids:
            _online_users.pop(uname, None)
            emit("user_status", {"username": uname, "online": False, "last_seen": int(time.time())}, broadcast=True)

@app.route("/api/online_status")
def online_status():
    return jsonify(list(_online_users.keys()))

@socketio.on("send_message")
def handle_message(packet):
    from flask import request as req
    packet["id"]     = f"msg_{int(time.time() * 1000)}"
    packet["time"]   = time.strftime("%H:%M")
    packet["edited"] = False
    packet["status"] = "delivered"
    packet["reactions"] = {}
    sender = str(packet.get("from", "")).lower()
    target = packet.get("to", "")
    if not sender or not target:
        return
    msgs   = load_json(MESSAGES_FILE)
    if target.startswith("group_"):
        chat_id = target
        ginfo = _normalize_group_permissions((load_json(GROUPS_FILE) or {}).get(chat_id, {}))
        if not ginfo or sender not in [m.lower() for m in ginfo.get("members", [])]:
            emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "forbidden"})
            return
        msg_type = str(packet.get("type", "text")).strip().lower()
        media_kind = str(packet.get("media_kind", "")).strip().lower()
        if msg_type in ("text", "") and not _group_member_permission(ginfo, sender, "can_send_messages"):
            emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "forbidden"})
            return
        if msg_type == "text" and bool(packet.get("has_link", False)) and not _group_member_permission(ginfo, sender, "can_send_links"):
            emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "forbidden"})
            return
        if msg_type == "sticker" and not _group_member_permission(ginfo, sender, "can_send_stickers"):
            emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "forbidden"})
            return
        if msg_type == "gif" and not _group_member_permission(ginfo, sender, "can_send_gifs"):
            emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "forbidden"})
            return
        if msg_type == "file":
            if not _group_member_permission(ginfo, sender, "can_send_media"):
                emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "forbidden"})
                return
            if media_kind in ("voice", "audio", "voice_note") and not _group_member_permission(ginfo, sender, "can_send_voice"):
                emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "forbidden"})
                return
            if media_kind in ("video_note", "circle") and not _group_member_permission(ginfo, sender, "can_send_video_notes"):
                emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "forbidden"})
                return
        msgs.setdefault(chat_id, []).append(packet)
        save_json(MESSAGES_FILE, msgs)
        # Рассылаем только участникам группы
        members = [m.lower() for m in ginfo.get("members", [])]
        sent_sids = set()
        for member in members:
            for sid in _online_users.get(member, set()):
                if sid not in sent_sids:
                    socketio.emit("new_message", packet, to=sid)
                    sent_sids.add(sid)
    else:
        target_lower = target.lower()
        chat_id = "_".join(sorted([sender, target_lower]))
        msgs.setdefault(chat_id, []).append(packet)
        save_json(MESSAGES_FILE, msgs)
        # Рассылаем ТОЛЬКО отправителю и получателю
        recipients = {sender, target_lower}
        sent_sids = set()
        for uname in recipients:
            for sid in _online_users.get(uname, set()):
                if sid not in sent_sids:
                    socketio.emit("new_message", packet, to=sid)
                    sent_sids.add(sid)
    # Подтверждение отправителю — его pending можно убрать
    emit("message_ack", {"id": packet["id"], "client_id": packet.get("client_id"), "status": "delivered"})

@socketio.on("toggle_reaction")
def handle_toggle_reaction(data):
    msg_id = str((data or {}).get("id", "")).strip()
    chat_id = str((data or {}).get("chat_id", "")).strip()
    actor = str((data or {}).get("username", "")).strip().lower()
    emoji = str((data or {}).get("emoji", "")).strip()
    if not msg_id or not chat_id or not actor or not emoji:
        return

    msgs = load_json(MESSAGES_FILE) or {}
    bucket = msgs.get(chat_id, [])
    if not isinstance(bucket, list):
        # Fallback: key may differ by register.
        for k, v in msgs.items():
            if str(k).strip().lower() == chat_id.lower() and isinstance(v, list):
                chat_id = str(k)
                bucket = v
                break
    if not isinstance(bucket, list):
        return

    target = None
    for m in bucket:
        if str(m.get("id", "")).strip() == msg_id:
            target = m
            break
    if not target:
        # Fuzzy match for legacy ids / with or without "msg_" prefix
        raw_id = str(msg_id).strip()
        alt_ids = {raw_id}
        if raw_id.startswith("msg_"):
            alt_ids.add(raw_id[4:])
        else:
            alt_ids.add(f"msg_{raw_id}")
        for m in bucket:
            mid = str(m.get("id", "")).strip()
            if mid in alt_ids:
                target = m
                break
            if any(mid.endswith(x) or x.endswith(mid) for x in alt_ids if x):
                target = m
                break
    if not target:
        return

    # Robust participant validation:
    # - for groups use actual group members;
    # - for direct chats use real packet fields (from/to), not chat_id split.
    participants = []
    if str(chat_id).lower().startswith("group_"):
        participants = _chat_participants(chat_id)
    else:
        frm = str(target.get("from", "")).strip().lower()
        to = str(target.get("to", "")).strip().lower()
        participants = [u for u in [frm, to] if u and not u.startswith("group_")]
        if not participants:
            pset = set()
            for m in bucket[-60:]:
                mf = str(m.get("from", "")).strip().lower()
                mt = str(m.get("to", "")).strip().lower()
                if mf and not mf.startswith("group_"):
                    pset.add(mf)
                if mt and not mt.startswith("group_"):
                    pset.add(mt)
            participants = list(pset)
    # Keep reactions functional even for legacy messages with broken participant metadata.
    # If we couldn't infer participants reliably, do not hard-block.
    if participants and actor not in participants:
        if str(chat_id).lower().startswith("group_"):
            return

    reactions = target.get("reactions")
    if not isinstance(reactions, dict):
        reactions = {}
        target["reactions"] = reactions

    users = reactions.get(emoji)
    if not isinstance(users, list):
        users = []
    users_norm = [str(u).strip().lower() for u in users if str(u).strip()]

    if actor in users_norm:
        users_norm = [u for u in users_norm if u != actor]
    else:
        users_norm.append(actor)

    if users_norm:
        reactions[emoji] = users_norm
    elif emoji in reactions:
        del reactions[emoji]

    save_json(MESSAGES_FILE, msgs)

    payload = {
        "id": msg_id,
        "chat_id": str(chat_id).lower(),
        "reactions": reactions
    }
    sent_sids = set()
    for uname in participants:
        for sid in _online_users.get(uname, set()):
            if sid in sent_sids:
                continue
            socketio.emit("reaction_updated", payload, to=sid)
            sent_sids.add(sid)

@socketio.on("message_read")
def handle_message_read(data):
    reader = data.get("reader")
    emit("chat_read", {"reader": reader, "by_whom": data.get("peer")}, broadcast=True)

# ─── Звонки (WebRTC signal + call state) ─────────────────────────
@socketio.on("call_invite")
def handle_call_invite(data):
    call_id = str(data.get("call_id", "")).strip()
    chat_id = str(data.get("chat_id", "")).strip()
    sender = str(data.get("from", "")).strip().lower()
    targets = [str(t).strip().lower() for t in (data.get("targets") or []) if str(t).strip()]
    mode = str(data.get("mode", "audio")).strip().lower()
    if not call_id or not sender:
        return

    call = get_or_create_call(call_id, chat_id=chat_id)
    call["chat_id"] = _safe_chat_id(sender, targets[0] if len(targets) == 1 else chat_id, explicit_chat_id=chat_id)
    if call["chat_id"].startswith("group_"):
        groups = load_json(GROUPS_FILE) or {}
        g = _normalize_group_permissions(groups.get(call["chat_id"], {}))
        if not _group_member_permission(g, sender, "can_start_calls"):
            emit_to_user(sender, "call_invited", {"call_id": call_id, "targets": [], "error": "forbidden"})
            return
    call["mode"] = mode
    if not call.get("initiator"):
        call["initiator"] = sender
    call["participants"].add(sender)
    call["all_participants"].add(sender)
    for t in targets:
        call["invited_targets"].add(t)
        call["all_participants"].add(t)
    for t in targets:
        emit_to_user(t, "call_incoming", {
            "call_id": call_id,
            "chat_id": chat_id,
            "from": sender,
            "mode": mode
        })
    emit_to_user(sender, "call_invited", {"call_id": call_id, "targets": targets})
    _refresh_lone_timer(call_id)
    _emit_call_state(call_id)
    # Сообщение в чат: ожидание звонка
    if call.get("chat_id") and targets:
        _send_call_event_message(call.get("chat_id"), sender, set(targets) | {sender}, f"📞 @{sender} ожидает вас в звонке")

@socketio.on("call_join")
def handle_call_join(data):
    call_id = str(data.get("call_id", "")).strip()
    chat_id = str(data.get("chat_id", "")).strip()
    uname = str(data.get("username", "")).strip().lower()
    if not call_id or not uname:
        return

    call = get_or_create_call(call_id, chat_id=chat_id)
    existing = [p for p in call["participants"] if p != uname]
    call["participants"].add(uname)
    call["all_participants"].add(uname)
    if uname in call.get("invited_targets", set()):
        call["invited_targets"].discard(uname)
    # Если пользователь принял на одном устройстве — закрываем входящий экран на всех его устройствах.
    emit_to_user(uname, "call_incoming_cancel", {"call_id": call_id})

    emit_to_user(uname, "call_participants", {
        "call_id": call_id,
        "participants": existing,
        "screen_owner": call.get("screen_owner"),
        "screen_track_id": call.get("screen_track_id", ""),
        "allow_draw_all": bool(call.get("allow_draw_all", False))
    })
    for peer in existing:
        emit_to_user(peer, "call_user_joined", {"call_id": call_id, "username": uname})
    _refresh_lone_timer(call_id)
    _emit_call_state(call_id)

@socketio.on("call_decline")
def handle_call_decline(data):
    call_id = str(data.get("call_id", "")).strip()
    uname = str(data.get("username", "")).strip().lower()
    if not call_id or not uname:
        return
    call = _active_calls.get(call_id)
    if not call:
        return
    if uname in call.get("invited_targets", set()):
        call["invited_targets"].discard(uname)
    # Закрываем входящий экран на всех сессиях этого пользователя.
    emit_to_user(uname, "call_incoming_cancel", {"call_id": call_id})
    _emit_call_state(call_id)

@socketio.on("call_leave")
def handle_call_leave(data):
    call_id = str(data.get("call_id", "")).strip()
    uname = str(data.get("username", "")).strip().lower()
    if not call_id or not uname:
        return
    call = _active_calls.get(call_id)
    if not call:
        return
    call["participants"].discard(uname)
    call["control_allowed"].discard(uname)
    if call.get("screen_owner") == uname:
        call["screen_owner"] = None
    for peer in call.get("participants", set()):
        emit_to_user(peer, "call_user_left", {"call_id": call_id, "username": uname})
    _refresh_lone_timer(call_id)
    if call_id in _active_calls:
        _emit_call_state(call_id)

@socketio.on("call_end")
def handle_call_end(data):
    call_id = str(data.get("call_id", "")).strip()
    ended_by = str(data.get("username", "")).strip().lower()
    call = _active_calls.get(call_id)
    if not call:
        return
    _finish_call(call_id, ended_by=ended_by, reason="ended")

@socketio.on("call_signal")
def handle_call_signal(data):
    target = str(data.get("target", "")).strip().lower()
    if not target:
        return
    emit_to_user(target, "call_signal", data)

@socketio.on("call_chat")
def handle_call_chat(data):
    call_id = str(data.get("call_id", "")).strip()
    sender = str(data.get("from", "")).strip().lower()
    msg = str(data.get("message", "")).strip()
    if not call_id or not sender or not msg:
        return
    call = _active_calls.get(call_id)
    if not call or sender not in call.get("participants", set()):
        return
    payload = {"call_id": call_id, "from": sender, "message": msg, "ts": int(time.time() * 1000)}
    for peer in call.get("participants", set()):
        emit_to_user(peer, "call_chat", payload)

@socketio.on("call_media_state")
def handle_call_media_state(data):
    call_id = str(data.get("call_id", "")).strip()
    sender = str(data.get("from", "")).strip().lower()
    call = _active_calls.get(call_id)
    if not call or sender not in call.get("participants", set()):
        return
    for peer in call.get("participants", set()):
        if peer != sender:
            emit_to_user(peer, "call_media_state", data)

@socketio.on("call_screen_share")
def handle_call_screen_share(data):
    call_id = str(data.get("call_id", "")).strip()
    sender = str(data.get("from", "")).strip().lower()
    sharing = bool(data.get("sharing", False))
    screen_track_id = str(data.get("screen_track_id", "")).strip()
    call = _active_calls.get(call_id)
    if not call or sender not in call.get("participants", set()):
        return
    call["screen_owner"] = sender if sharing else (None if call.get("screen_owner") == sender else call.get("screen_owner"))
    call["screen_track_id"] = screen_track_id if sharing else ""
    payload = {
        "call_id": call_id,
        "from": sender,
        "sharing": sharing,
        "screen_owner": call.get("screen_owner"),
        "screen_track_id": call.get("screen_track_id", "")
    }
    for peer in call.get("participants", set()):
        if peer != sender:
            emit_to_user(peer, "call_screen_share", payload)

@socketio.on("call_annotation_perm")
def handle_call_annotation_perm(data):
    call_id = str(data.get("call_id", "")).strip()
    sender = str(data.get("from", "")).strip().lower()
    allow_all = bool(data.get("allow_all", False))
    call = _active_calls.get(call_id)
    if not call or sender not in call.get("participants", set()):
        return
    is_owner = call.get("screen_owner") == sender
    has_control = sender in call.get("control_allowed", set())
    if not is_owner and not has_control:
        return
    call["allow_draw_all"] = allow_all
    payload = {"call_id": call_id, "from": sender, "allow_all": allow_all}
    for peer in call.get("participants", set()):
        emit_to_user(peer, "call_annotation_perm", payload)

@socketio.on("call_control_request")
def handle_call_control_request(data):
    call_id = str(data.get("call_id", "")).strip()
    sender = str(data.get("from", "")).strip().lower()
    owner = str(data.get("owner", "")).strip().lower()
    call = _active_calls.get(call_id)
    if not call or sender not in call.get("participants", set()):
        return
    if call.get("screen_owner") != owner:
        return
    emit_to_user(owner, "call_control_request", {"call_id": call_id, "from": sender, "owner": owner})

@socketio.on("call_control_response")
def handle_call_control_response(data):
    call_id = str(data.get("call_id", "")).strip()
    owner = str(data.get("owner", "")).strip().lower()
    target = str(data.get("target", "")).strip().lower()
    allow = bool(data.get("allow", False))
    call = _active_calls.get(call_id)
    if not call or owner not in call.get("participants", set()):
        return
    if call.get("screen_owner") != owner:
        return
    if allow:
        call["control_allowed"].add(target)
    else:
        call["control_allowed"].discard(target)
    payload = {"call_id": call_id, "owner": owner, "target": target, "allow": allow}
    emit_to_user(target, "call_control_response", payload)
    emit_to_user(owner, "call_control_response", payload)

@socketio.on("call_annotation")
def handle_call_annotation(data):
    call_id = str(data.get("call_id", "")).strip()
    sender = str(data.get("from", "")).strip().lower()
    call = _active_calls.get(call_id)
    if not call or sender not in call.get("participants", set()):
        return

    owner = call.get("screen_owner")
    can_draw = (sender == owner) or bool(call.get("allow_draw_all", False)) or (sender in call.get("control_allowed", set()))
    if not can_draw:
        return

    payload = {
        "call_id": call_id,
        "from": sender,
        "kind": data.get("kind"),
        "stroke_id": data.get("stroke_id"),
        "points": data.get("points"),
        "color": data.get("color", "#ff4d4f"),
        "size": data.get("size", 2),
        "aspect": data.get("aspect")
    }
    for peer in call.get("participants", set()):
        emit_to_user(peer, "call_annotation", payload)

@socketio.on("edit_message")
def handle_edit(data):
    msg_id   = data.get("id")
    cipher   = data.get("cipher")
    chat_id  = data.get("chat_id")
    msgs     = load_json(MESSAGES_FILE)
    targets  = [chat_id] if chat_id in msgs else list(msgs.keys())
    for cid in targets:
        for m in msgs.get(cid, []):
            if m.get("id") == msg_id:
                m["cipher"] = cipher
                m["edited"] = True
                save_json(MESSAGES_FILE, msgs)
                emit("message_edited", {"id": msg_id, "cipher": cipher}, broadcast=True)
                return

@socketio.on('delete_message')
def handle_delete(data):
    msg_id  = data.get('id')
    chat_id = data.get('chat_id')
    msgs    = load_json(MESSAGES_FILE)
    found   = False
    targets = [chat_id] if chat_id in msgs else list(msgs.keys())
    for cid in targets:
        before = len(msgs.get(cid, []))
        msgs[cid] = [m for m in msgs.get(cid, []) if m.get('id') != msg_id]
        if len(msgs[cid]) < before:
            found = True; break
    if not found:
        for cid in msgs:
            before = len(msgs[cid])
            msgs[cid] = [m for m in msgs[cid] if m.get('id') != msg_id]
            if len(msgs[cid]) < before:
                found = True; break
    if found:
        save_json(MESSAGES_FILE, msgs)
        emit('message_deleted', {'id': msg_id}, broadcast=True, include_self=True)

@app.route("/api/delete_chat", methods=["POST"])
def delete_chat():
    data    = request.json
    me      = data.get("me", "").lower()
    peer    = data.get("peer", "")
    msgs    = load_json(MESSAGES_FILE)
    chat_id = "_".join(sorted([me, peer]))
    if chat_id in msgs:
        del msgs[chat_id]
        save_json(MESSAGES_FILE, msgs)
    return jsonify({"status": "ok"})

@app.route("/api/pin_message", methods=["POST"])
def pin_message():
    data = request.json or {}
    me = str(data.get("me", "")).strip().lower()
    chat_id = str(data.get("chat_id", "")).strip().lower()
    msg_id = str(data.get("msg_id", "")).strip()
    scope = str(data.get("scope", "self")).strip().lower()
    preview = str(data.get("preview", "")).strip()[:300]
    sender = str(data.get("sender", "")).strip().lower()
    time_label = str(data.get("time", "")).strip()
    if not me or not chat_id or not msg_id:
        return jsonify({"error": "bad_request"}), 400
    participants = _chat_participants(chat_id)
    if me not in participants:
        return jsonify({"error": "forbidden"}), 403
    if chat_id.startswith("group_") and scope == "all":
        groups = load_json(GROUPS_FILE) or {}
        g = groups.get(chat_id, {})
        if not _group_member_permission(g, me, "can_pin_messages"):
            return jsonify({"error": "forbidden"}), 403
    pins = load_json(PINNED_FILE) or {}
    pins.setdefault("all", {})
    pins.setdefault("self", {})
    pin = {
        "pin_id": f"pin_{int(time.time() * 1000)}",
        "chat_id": chat_id,
        "msg_id": msg_id,
        "by": me,
        "scope": "all" if scope == "all" else "self",
        "preview": preview,
        "sender": sender,
        "time": time_label,
        "timestamp": time.time()
    }
    if pin["scope"] == "all":
        arr = pins["all"].setdefault(chat_id, [])
    else:
        user_pins = pins["self"].setdefault(me, {})
        arr = user_pins.setdefault(chat_id, [])
    arr = [p for p in arr if p.get("msg_id") != msg_id]
    arr.insert(0, pin)
    if pin["scope"] == "all":
        pins["all"][chat_id] = arr
    else:
        pins["self"][me][chat_id] = arr
    save_json(PINNED_FILE, pins)
    if pin["scope"] == "all":
        for peer in participants:
            emit_to_user(peer, "pin_updated", {"chat_id": chat_id})
        _send_system_event_message(chat_id, participants, "📌 Закреплено сообщение")
    else:
        emit_to_user(me, "pin_updated", {"chat_id": chat_id})
    return jsonify({"status": "ok", "pin": pin})

@app.route("/api/unpin_message", methods=["POST"])
def unpin_message():
    data = request.json or {}
    me = str(data.get("me", "")).strip().lower()
    chat_id = str(data.get("chat_id", "")).strip().lower()
    pin_id = str(data.get("pin_id", "")).strip()
    scope = str(data.get("scope", "self")).strip().lower()
    if not me or not chat_id or not pin_id:
        return jsonify({"error": "bad_request"}), 400
    pins = load_json(PINNED_FILE) or {}
    pins.setdefault("all", {})
    pins.setdefault("self", {})
    participants = _chat_participants(chat_id)
    if me not in participants:
        return jsonify({"error": "forbidden"}), 403
    changed = False
    if scope == "all":
        arr = pins["all"].get(chat_id, [])
        new_arr = [p for p in arr if str(p.get("pin_id")) != pin_id]
        changed = len(new_arr) != len(arr)
        pins["all"][chat_id] = new_arr
        if changed:
            for peer in participants:
                emit_to_user(peer, "pin_updated", {"chat_id": chat_id})
            _send_system_event_message(chat_id, participants, "📌 Закрепленное сообщение откреплено")
    else:
        user_pins = pins["self"].setdefault(me, {})
        arr = user_pins.get(chat_id, [])
        new_arr = [p for p in arr if str(p.get("pin_id")) != pin_id]
        changed = len(new_arr) != len(arr)
        user_pins[chat_id] = new_arr
        if changed:
            emit_to_user(me, "pin_updated", {"chat_id": chat_id})
    if changed:
        save_json(PINNED_FILE, pins)
    return jsonify({"status": "ok"})

@app.route("/api/pinned_messages")
def pinned_messages():
    me = str(request.args.get("me", "")).strip().lower()
    chat_id = str(request.args.get("chat_id", "")).strip().lower()
    if not me:
        return jsonify([])
    pins = load_json(PINNED_FILE) or {}
    pins.setdefault("all", {})
    pins.setdefault("self", {})
    if chat_id:
        all_arr = pins["all"].get(chat_id, [])
        self_arr = pins["self"].get(me, {}).get(chat_id, [])
        merged = sorted(
            all_arr + self_arr,
            key=lambda x: _msg_order_value(x.get("msg_id"), x.get("timestamp", 0)),
            reverse=True
        )
        return jsonify(merged)
    result = []
    for cid, arr in pins["all"].items():
        if me in _chat_participants(cid):
            result.extend(arr)
    for cid, arr in pins["self"].get(me, {}).items():
        result.extend(arr)
    result = sorted(
        result,
        key=lambda x: _msg_order_value(x.get("msg_id"), x.get("timestamp", 0)),
        reverse=True
    )
    return jsonify(result)


# ─── Группы ──────────────────────────────────────────────────────
@app.route("/api/create_group", methods=["POST"])
def create_group():
    data     = request.json
    group_id = f"group_{uuid.uuid4().hex[:10]}"
    groups   = load_json(GROUPS_FILE)
    avatar   = data.get("avatar", "")

    groups[group_id] = _normalize_group_permissions({
        "name":           data["name"],
        "desc":           data.get("desc", ""),
        "members":        data["members"],
        "owner":          data["owner"],
        "encrypted_keys": data["keys"],
        "invite_token":   uuid.uuid4().hex,
        "created_at":     time.time()
    })
    save_json(GROUPS_FILE, groups)

    # Сохраняем аватар группы в avatars.json
    if avatar:
        set_avatar(group_id, avatar)

    msgs = load_json(MESSAGES_FILE)
    owner = str(data.get("owner", "")).strip().lower()
    members = [str(m).strip().lower() for m in (data.get("members") or []) if str(m).strip()]
    if owner and owner not in members:
        members.append(owner)
    sys_packet = {
        "id": f"msg_{int(time.time() * 1000)}",
        "from": "system",
        "to": group_id,
        "type": "system_event",
        "text": "Группа создана",
        "time": time.strftime("%H:%M"),
        "edited": False,
        "status": "delivered",
        "timestamp": time.time()
    }
    msgs[group_id] = [sys_packet]
    save_json(MESSAGES_FILE, msgs)
    sent_sids = set()
    for member in members:
        for sid in _online_users.get(member, set()):
            if sid in sent_sids:
                continue
            socketio.emit("new_message", sys_packet, to=sid)
            sent_sids.add(sid)
    return jsonify({"success": True, "group_id": group_id})

@app.route("/api/group_info/<group_id>")
def get_group_info(group_id):
    groups = load_json(GROUPS_FILE)
    if group_id not in groups:
        return jsonify({"error": "Группа не найдена"}), 404
    info = _normalize_group_permissions(groups[group_id])
    changed = False
    if not str(info.get("invite_token", "")).strip():
        info["invite_token"] = uuid.uuid4().hex
        groups[group_id]["invite_token"] = info["invite_token"]
        changed = True
    if "permissions" not in groups[group_id]:
        groups[group_id] = _normalize_group_permissions(groups[group_id])
        changed = True
    if changed:
        save_json(GROUPS_FILE, groups)
    info["avatar"] = get_avatar(group_id)   # всегда из avatars.json
    info["invite_link"] = request.host_url.rstrip("/") + f"/invite/{info['invite_token']}"
    return jsonify(info)

@app.route("/api/update_group", methods=["POST"])
def update_group():
    data     = request.json
    group_id = data.get("group_id")
    username = data.get("username")
    groups   = load_json(GROUPS_FILE)
    if group_id not in groups:
        return jsonify({"error": "Не найдена"}), 404
    g = _normalize_group_permissions(groups[group_id])
    if str(g.get("owner", "")).lower() != str(username or "").lower() and not _group_member_permission(g, username, "can_change_info"):
        return jsonify({"error": "Нет прав"}), 403
    if "name" in data:
        groups[group_id]["name"] = data["name"]
    if "desc" in data:
        groups[group_id]["desc"] = data["desc"]
    groups[group_id] = _normalize_group_permissions(groups[group_id])
    _ensure_group_invite_token(groups[group_id])
    save_json(GROUPS_FILE, groups)
    if data.get("avatar"):
        set_avatar(group_id, data["avatar"])
    return jsonify({"status": "ok"})

@app.route("/api/group_permissions/<group_id>")
def get_group_permissions(group_id):
    me = str(request.args.get("me", "")).strip().lower()
    groups = load_json(GROUPS_FILE) or {}
    g = groups.get(group_id)
    if not g:
        return jsonify({"error": "not_found"}), 404
    g = _normalize_group_permissions(g)
    members = [str(m).strip().lower() for m in g.get("members", []) if str(m).strip()]
    if me not in members:
        return jsonify({"error": "forbidden"}), 403
    return jsonify({"permissions": g.get("permissions", {}), "owner": g.get("owner", ""), "members": members})

@app.route("/api/group_permissions/update", methods=["POST"])
def update_group_permissions():
    data = request.json or {}
    group_id = str(data.get("group_id", "")).strip()
    actor = _auth_user_from_request(data)
    target = str(data.get("target", "")).strip().lower()
    perms_patch = data.get("permissions", {})
    if not actor:
        return jsonify({"error": "auth_required"}), 401
    if not group_id or not isinstance(perms_patch, dict):
        return jsonify({"error": "bad_request"}), 400
    groups = load_json(GROUPS_FILE) or {}
    g = groups.get(group_id)
    if not g:
        return jsonify({"error": "not_found"}), 404
    g = _normalize_group_permissions(g)
    owner = str(g.get("owner", "")).strip().lower()
    members = [str(m).strip().lower() for m in g.get("members", []) if str(m).strip()]
    if actor != owner and not _group_member_permission(g, actor, "can_manage_permissions"):
        return jsonify({"error": "forbidden"}), 403
    allowed_keys = set(_default_group_permissions().keys())
    cleaned = {k: bool(v) for k, v in perms_patch.items() if k in allowed_keys}
    if target:
        if target not in members:
            return jsonify({"error": "member_not_found"}), 404
        g["permissions"].setdefault("members", {})
        g["permissions"]["members"].setdefault(target, {})
        g["permissions"]["members"][target].update(cleaned)
    else:
        g["permissions"].setdefault("defaults", _default_group_permissions())
        g["permissions"]["defaults"].update(cleaned)
    groups[group_id] = _normalize_group_permissions(g)
    save_json(GROUPS_FILE, groups)
    for peer in members:
        emit_to_user(peer, "group_members_updated", {"group_id": group_id, "removed": False})
    return jsonify({"status": "ok", "permissions": groups[group_id].get("permissions", {})})

@app.route("/api/delete_group", methods=["POST"])
def delete_group():
    data     = request.json
    group_id = data.get("group_id")
    username = data.get("username")
    groups   = load_json(GROUPS_FILE)
    if group_id not in groups:
        return jsonify({"error": "Не найдена"}), 404
    if groups[group_id]["owner"] != username:
        return jsonify({"error": "Нет прав"}), 403
    del groups[group_id]
    save_json(GROUPS_FILE, groups)
    # Удаляем историю
    msgs = load_json(MESSAGES_FILE)
    if group_id in msgs:
        del msgs[group_id]
        save_json(MESSAGES_FILE, msgs)
    # Удаляем аватар
    avatars = load_json(AVATARS_FILE)
    if group_id in avatars:
        del avatars[group_id]
        save_json(AVATARS_FILE, avatars)
    return jsonify({"status": "ok"})

@app.route("/api/leave_group", methods=["POST"])
def leave_group():
    data     = request.json
    group_id = data.get("group_id")
    username = data.get("username")
    groups   = load_json(GROUPS_FILE)
    if group_id not in groups:
        return jsonify({"error": "Не найдена"}), 404
    groups[group_id]["members"] = [m for m in groups[group_id]["members"] if m != username]
    save_json(GROUPS_FILE, groups)
    return jsonify({"status": "ok"})

@app.route("/api/group_add_member", methods=["POST"])
def group_add_member():
    data = request.json or {}
    group_id = str(data.get("group_id", "")).strip()
    actor = str(data.get("username", "")).strip().lower()
    member = str(data.get("member", "")).strip().lower()
    if not group_id or not actor or not member:
        return jsonify({"error": "bad_request"}), 400

    groups = load_json(GROUPS_FILE) or {}
    users = load_json(USERS_FILE) or {}
    g = groups.get(group_id)
    if not g:
        return jsonify({"error": "not_found"}), 404
    if str(g.get("owner", "")).lower() != actor and not _group_member_permission(g, actor, "can_add_members"):
        return jsonify({"error": "forbidden"}), 403
    if member not in users:
        return jsonify({"error": "user_not_found"}), 404

    members = [str(m).lower() for m in g.get("members", [])]
    if member in members:
        return jsonify({"status": "ok", "already_member": True})

    members.append(member)
    groups[group_id]["members"] = members
    g = _normalize_group_permissions(groups[group_id])
    perms = g.get("permissions", {})
    _grant_join_access_permissions(perms, member)
    groups[group_id]["permissions"] = perms
    save_json(GROUPS_FILE, groups)
    msgs = load_json(MESSAGES_FILE) or {}
    packet = {
        "id": f"msg_{int(time.time() * 1000)}",
        "from": "system",
        "to": group_id,
        "type": "call_event",
        "text": f"➕ @{member} добавлен(а) в группу",
        "time": time.strftime("%H:%M"),
        "edited": False,
        "status": "delivered",
        "timestamp": time.time()
    }
    msgs.setdefault(group_id, []).append(packet)
    save_json(MESSAGES_FILE, msgs)

    for peer in members:
        emit_to_user(peer, "group_members_updated", {"group_id": group_id, "removed": False})
        emit_to_user(peer, "new_message", packet)
    enc = groups[group_id].get("encrypted_keys", {}) if isinstance(groups[group_id].get("encrypted_keys"), dict) else {}
    for grantor in members:
        gu = str(grantor or "").strip().lower()
        if not gu or gu == member:
            continue
        if not str(enc.get(gu, "")).strip():
            continue
        _emit_group_key_needed_to_member(gu, group_id, member)
    return jsonify({"status": "ok", "already_member": False})

@app.route("/api/group_remove_member", methods=["POST"])
def group_remove_member():
    data = request.json or {}
    group_id = str(data.get("group_id", "")).strip()
    actor = str(data.get("username", "")).strip().lower()
    member = str(data.get("member", "")).strip().lower()
    if not group_id or not actor or not member:
        return jsonify({"error": "bad_request"}), 400

    groups = load_json(GROUPS_FILE) or {}
    g = groups.get(group_id)
    if not g:
        return jsonify({"error": "not_found"}), 404
    if str(g.get("owner", "")).lower() != actor and not _group_member_permission(g, actor, "can_remove_members"):
        return jsonify({"error": "forbidden"}), 403
    if member == actor:
        return jsonify({"error": "cant_remove_owner"}), 400

    members = [str(m).lower() for m in g.get("members", [])]
    if member not in members:
        return jsonify({"status": "ok", "already_removed": True})

    members = [m for m in members if m != member]
    groups[group_id]["members"] = members
    if isinstance(groups[group_id].get("encrypted_keys"), dict):
        groups[group_id]["encrypted_keys"].pop(member, None)
    if isinstance(groups[group_id].get("permissions"), dict):
        pm = groups[group_id]["permissions"].get("members")
        if isinstance(pm, dict):
            pm.pop(member, None)
    save_json(GROUPS_FILE, groups)
    msgs = load_json(MESSAGES_FILE) or {}
    packet = {
        "id": f"msg_{int(time.time() * 1000)}",
        "from": "system",
        "to": group_id,
        "type": "call_event",
        "text": f"➖ @{member} удален(а) из группы",
        "time": time.strftime("%H:%M"),
        "edited": False,
        "status": "delivered",
        "timestamp": time.time()
    }
    msgs.setdefault(group_id, []).append(packet)
    save_json(MESSAGES_FILE, msgs)

    for peer in members:
        emit_to_user(peer, "group_members_updated", {"group_id": group_id, "removed": False})
        emit_to_user(peer, "new_message", packet)
    emit_to_user(member, "group_members_updated", {"group_id": group_id, "removed": True})
    return jsonify({"status": "ok", "already_removed": False})

@app.route("/api/my_groups/<username>")
def get_my_groups(username):
    groups = load_json(GROUPS_FILE)
    return jsonify([{"id": gid, "name": info["name"]} for gid, info in groups.items() if username in info["members"]])

@app.route("/api/search")
def search_users():
    """Поиск пользователей для создания группы"""
    query    = request.args.get("query", "").lower().strip()
    me_param = request.args.get("me", "").lower().strip()
    users    = load_json(USERS_FILE)
    nicks    = load_json(NICKNAMES_FILE)
    my_nicks = nicks.get(me_param, {})
    results  = []
    
    for u in users:
        if u == me_param: continue
        ud     = users[u]
        first  = ud.get("first_name", "").lower()
        last   = ud.get("last_name", "").lower()
        full   = f"{first} {last}".strip()
        custom = my_nicks.get(u.lower(), "").lower()
        uname  = u.lower()
        
        if not query or query in uname or query in first or query in last \
                or query in full or (custom and query in custom):
            display = f"{ud.get('first_name','')} {ud.get('last_name','')}".strip() or u
            if my_nicks.get(u.lower()): display = my_nicks[u.lower()]
            results.append({
                "username": u,
                "display_name": display,
                "avatar": get_avatar(u)
            })
    return jsonify(results[:30])


# ─── Файлы ───────────────────────────────────────────────────────
@app.route("/upload", methods=["POST"])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file"}), 400
    file      = request.files['file']
    file_type = request.form.get('type', 'files')
    if file_type not in ['images', 'files']:
        file_type = 'files'
    file.seek(0, os.SEEK_END)
    size = file.tell(); file.seek(0)
    if size > 2 * 1024 * 1024 * 1024:
        return jsonify({"error": "Файл слишком большой"}), 400
    ext      = os.path.splitext(file.filename or "")[1]
    filename = f"{uuid.uuid4().hex}{ext}"
    if _r2_enabled():
        file.seek(0)
        _r2_upload_fileobj(file.stream, file_type, filename, content_type=(file.content_type or "application/octet-stream"))
    else:
        file.save(os.path.join(UPLOAD_FOLDER, file_type, filename))
    return jsonify({"status": "ok", "url": f"/{file_type}/{filename}", "size": size})

@app.route("/api/transcribe_audio", methods=["POST"])
def api_transcribe_audio():
    if "audio" not in request.files:
        return jsonify({"error": "No audio"}), 400
    audio = request.files["audio"]
    lang = str(request.form.get("lang", "ru")).strip().lower() or "ru"
    ext = os.path.splitext(audio.filename or "")[1] or ".webm"
    temp_path = None
    try:
        audio.seek(0, os.SEEK_END)
        size = audio.tell()
        audio.seek(0)
        # Ограничение для быстрой транскрибации (ориентировочно до 2 минут webm-opus)
        if size > 8 * 1024 * 1024:
            return jsonify({"error": "voice_too_long"}), 413
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            temp_path = tmp.name
            audio.save(temp_path)
        transcript = _transcribe_local_audio(temp_path, lang=lang)
        return jsonify({"status": "ok", "transcript": transcript})
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    except Exception:
        return jsonify({"error": "transcribe_failed"}), 500
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

@app.route('/images/<path:filename>')
def serve_images(filename):
    if _r2_enabled():
        return redirect(_r2_signed_or_public_url(_r2_object_key("images", filename)))
    return send_from_directory(os.path.join(UPLOAD_FOLDER, 'images'), filename)

@app.route('/files/<path:filename>')
def serve_files(filename):
    if _r2_enabled():
        return redirect(_r2_signed_or_public_url(_r2_object_key("files", filename)))
    return send_from_directory(os.path.join(UPLOAD_FOLDER, 'files'), filename)

@app.route('/uploads/<folder>/<filename>')
def uploaded_file(folder, filename):
    folder = str(folder or "").strip().lower()
    if folder not in ("images", "files", "avatars"):
        return jsonify({"error": "bad_folder"}), 400
    if _r2_enabled():
        return redirect(_r2_signed_or_public_url(_r2_object_key(folder, filename)))
    return send_from_directory(os.path.join(UPLOAD_FOLDER, folder), filename)


# ─── Бэкап ───────────────────────────────────────────────────────
@app.route("/backup_key", methods=["POST"])
def backup_key():
    data      = request.json
    username  = data.get("username", "").strip().lower()
    password  = data.get("password")
    initiator = data.get("initiator")
    users = load_json(USERS_FILE)
    if users.get(username, {}).get("password") != password:
        return jsonify({"status": "error"}), 403
    users[username]["enc_priv_key"] = data.get("enc_priv_key")
    save_json(USERS_FILE, users)
    socketio.emit('backup_status_changed', {'username': username, 'has_backup': True, 'initiator': initiator})
    return jsonify({"status": "ok"})

@app.route("/disable_backup", methods=["POST"])
def disable_backup():
    data      = request.json
    username  = data.get("username")
    password  = data.get("password")
    initiator = data.get("initiator")
    users = load_json(USERS_FILE)
    if users.get(username, {}).get("password") != password:
        return jsonify({"status": "error"}), 403
    users[username].pop("enc_priv_key", None)
    save_json(USERS_FILE, users)
    socketio.emit('force_logout_others', {'username': username, 'initiator': initiator})
    return jsonify({"status": "ok"})

@app.route("/check_backup/<username>")
def check_backup(username):
    users = load_json(USERS_FILE)
    if username not in users:
        return jsonify({"error": "Не найден"}), 404
    return jsonify({"has_backup": "enc_priv_key" in users[username], "key": users[username].get("enc_priv_key")})

@app.route("/check_password", methods=["POST"])
def check_password():
    data = request.json
    users = load_json(USERS_FILE)
    u = data.get("username")
    p = data.get("password")
    if u in users and users[u]["password"] == p:
        return jsonify({"status": "ok"})
    return jsonify({"status": "error"}), 401

@app.route("/api/upload_avatar", methods=["POST"])
def upload_avatar():
    if 'avatar' not in request.files:
        return jsonify({"error": "No file"}), 400
    import base64
    file     = request.files['avatar']
    username = request.form.get("username")
    if file and username:
        data = file.read()
        b64  = "data:" + (file.content_type or "image/jpeg") + ";base64," + base64.b64encode(data).decode()
        set_avatar(username, b64)
        return jsonify({"status": "ok"})
    return jsonify({"error": "Fail"}), 400


# ─── Тех. поддержка ──────────────────────────────────────────────
SUPPORT_FILE   = "support_tickets.json"
SUPPORT_ADMINS = ["admin@levart.app", "support@levart.app"]  # список почт администраторов

@app.route("/api/support_ticket", methods=["POST"])
def support_ticket():
    data     = request.json
    username_req = data.get("username", "anonymous")
    subject  = data.get("subject", "Без темы")
    message  = data.get("message", "").strip()
    
    if not message:
        return jsonify({"error": "Сообщение не может быть пустым"}), 400
    
    ticket = {
        "id":        f"ticket_{int(time.time() * 1000)}",
        "username":  username_req,
        "subject":   subject,
        "message":   message,
        "status":    "open",
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    tickets = load_json(SUPPORT_FILE)
    if not isinstance(tickets, list):
        tickets = []
    tickets.append(ticket)
    save_json(SUPPORT_FILE, tickets)
    
    # Отправка на почту (раскомментировать и настроить SMTP)
    # try:
    #     import smtplib
    #     from email.mime.text import MIMEText
    #     from email.mime.multipart import MIMEMultipart
    #     smtp_host = "smtp.mail.ru"  # Настроить под свой почтовый сервис
    #     smtp_port = 465
    #     smtp_user = "bot@levart.app"
    #     smtp_pass = "your_password"
    #     for admin_email in SUPPORT_ADMINS:
    #         msg = MIMEMultipart()
    #         msg['From']    = smtp_user
    #         msg['To']      = admin_email
    #         msg['Subject'] = f"[Levart Support] {subject} — @{username_req}"
    #         body = f"От: @{username_req}\n\nТема: {subject}\n\nСообщение:\n{message}\n\nID тикета: {ticket['id']}"
    #         msg.attach(MIMEText(body, 'plain', 'utf-8'))
    #         with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
    #             server.login(smtp_user, smtp_pass)
    #             server.send_message(msg)
    # except Exception as e:
    #     print(f"Ошибка отправки письма: {e}")
    
    print(f"[SUPPORT] Новый тикет от @{username_req}: {subject}")
    return jsonify({"status": "ok", "ticket_id": ticket["id"]})

@app.route("/api/support_tickets")
def get_support_tickets():
    """Только для администраторов"""
    tickets = load_json(SUPPORT_FILE)
    return jsonify(tickets if isinstance(tickets, list) else [])

# ─── Last read timestamps ─────────────────────────────────────────
LAST_READ_FILE = "last_read.json"

@app.route("/api/mark_read", methods=["POST"])
def mark_read():
    data    = request.json
    me      = data.get("me", "").lower()
    peer    = data.get("peer", "")
    last_read = load_json(LAST_READ_FILE) or {}
    if me not in last_read:
        last_read[me] = {}
    last_read[me][peer] = time.time()
    save_json(LAST_READ_FILE, last_read)
    socketio.emit("chat_read", {"reader": me, "by_whom": peer}, broadcast=True)
    return jsonify({"ok": True})

@app.route("/api/last_read")
def get_last_read():
    me   = request.args.get("me", "").lower()
    peer = request.args.get("peer", "")
    data = load_json(LAST_READ_FILE) or {}
    ts   = data.get(me, {}).get(peer, 0)
    return jsonify({"last_read": ts})


# ─── Истории ──────────────────────────────────────────────────────
@app.route("/api/story_create", methods=["POST"])
def story_create():
    data = request.json or {}
    owner = str(data.get("username", "")).strip().lower()
    meta = data.get("meta") or {}
    if not owner or not isinstance(meta, dict):
        return jsonify({"error": "bad_request"}), 400
    stories = _cleanup_stories(load_json(STORIES_FILE) or [])
    story = {
        "id": f"story_{int(time.time() * 1000)}",
        "owner": owner,
        "meta": {
            "type": str(meta.get("type", "image")),
            "url": str(meta.get("url", "")),
            "file_key": str(meta.get("file_key", "")),
            "name": str(meta.get("name", "")),
            "mime": str(meta.get("mime", "")),
            "caption": str(meta.get("caption", ""))[:220]
        },
        "viewers": [],
        "created_at": time.time(),
        "expires_at": time.time() + 24 * 60 * 60
    }
    stories.append(story)
    save_json(STORIES_FILE, stories)
    emit_to_user(owner, "stories_updated", {"owner": owner})
    users = load_json(USERS_FILE) or {}
    privacy = load_json(PRIVACY_FILE) or {}
    for viewer in _story_viewers_for_owner(owner, users, privacy):
        if viewer == owner:
            continue
        emit_to_user(viewer, "stories_updated", {"owner": owner})
    return jsonify({"status": "ok", "story": story})

@app.route("/api/stories_feed/<username>")
def stories_feed(username):
    me = str(username or "").strip().lower()
    users = load_json(USERS_FILE) or {}
    privacy = load_json(PRIVACY_FILE) or {}
    stories = _cleanup_stories(load_json(STORIES_FILE) or [])
    save_json(STORIES_FILE, stories)
    result = [s for s in stories if _can_view_story(str(s.get("owner", "")).lower(), me, users, privacy)]
    result.sort(key=lambda x: float(x.get("created_at", 0)), reverse=True)
    return jsonify(result)

@app.route("/api/story_view", methods=["POST"])
def story_view():
    data = request.json or {}
    me = str(data.get("username", "")).strip().lower()
    story_id = str(data.get("story_id", "")).strip()
    if not me or not story_id:
        return jsonify({"error": "bad_request"}), 400
    stories = _cleanup_stories(load_json(STORIES_FILE) or [])
    users = load_json(USERS_FILE) or {}
    privacy = load_json(PRIVACY_FILE) or {}
    changed_owner = ""
    for s in stories:
        if str(s.get("id")) != story_id:
            continue
        owner = str(s.get("owner", "")).lower()
        if not _can_view_story(owner, me, users, privacy):
            return jsonify({"error": "forbidden"}), 403
        viewers = [str(v).lower() for v in s.get("viewers", [])]
        if me not in viewers:
            viewers.append(me)
            s["viewers"] = viewers
            changed_owner = owner
        break
    save_json(STORIES_FILE, stories)
    if changed_owner:
        emit_to_user(changed_owner, "stories_updated", {"owner": changed_owner})
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    import eventlet
    from datetime import datetime, timedelta
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").disabled = True
    try:
        from OpenSSL import crypto
        pkey = crypto.PKey(); pkey.generate_key(crypto.TYPE_RSA, 2048)
        cert = crypto.X509()
        cert.get_subject().CN = "localhost"; cert.set_serial_number(1000)
        now = datetime.utcnow(); expire = now + timedelta(days=365)
        cert.set_notBefore(now.strftime("%Y%m%d%H%M%SZ").encode())
        cert.set_notAfter(expire.strftime("%Y%m%d%H%M%SZ").encode())
        cert.set_issuer(cert.get_subject()); cert.set_pubkey(pkey); cert.sign(pkey, 'sha256')
        import tempfile
        cf = tempfile.NamedTemporaryFile(delete=False); kf = tempfile.NamedTemporaryFile(delete=False)
        cf.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert)); kf.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))
        cf.close(); kf.close()
        print("\n[!] HTTPS запущен на https://localhost:5000\n")
        socketio.run(app, host="0.0.0.0", port=5000, certfile=cf.name, keyfile=kf.name, debug=False)
    except Exception as e:
        print(f"SSL ошибка ({e}), запуск HTTP...")
        socketio.run(app, host="0.0.0.0", port=5000, debug=False)
