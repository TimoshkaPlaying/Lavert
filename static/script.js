import * as cryptoMod from "/static/crypto.js";

/**
 * ==========================================
 * 1. КОНФИГУРАЦИЯ (APP_CONFIG)
 * ==========================================
 */
const APP_CONFIG = {
    UI_TEXT: {
        AUTH_REQUIRED: "Нужна авторизация. Перенаправление...",
        ALREADY_LOGGED: "Вы уже вошли. Перенаправление...",
        FILL_FIELDS: "Заполните все поля",
        LOGIN_SUCCESS: "Вход выполнен успешно!",
        REG_SUCCESS: "Регистрация завершена!",
        LOGOUT_CONFIRM: "Вы точно хотите выйти из аккаунта?",
        
        CRYPTO_ERR_KEYS: "Ошибка при создании ключей",
        CRYPTO_ERR_DECRYPT: "Не удалось вскрыть контейнер ключей. Неверный пароль?",
        CRYPTO_ERR_NO_LOCAL: "Ключи не найдены в этом браузере или облаке",
        CRYPTO_ERR_SEC: "Ошибка безопасности чата",
        CHAT_KEY_ERR: "Не удалось создать ключ для ",
        
        BACKUP_INFO: "Ваш ключ будет зашифрован паролем и сохранен в облаке для входа с других устройств.",
        BACKUP_SUCCESS: "Облачный бэкап успешно создан!",
        BACKUP_FAIL: "Ошибка синхронизации: ",
        
        NEW_MSG: "Новое сообщение от ",
        CHAT_REQ: "Запрос переписки от ",
        DECRYPT_ERR: "[!] Ошибка: Невозможно расшифровать (ключи не совпадают)"
    },
    SETTINGS: {
        DB_NAME: "LevartVault",
        DB_STORE: "user_keys",
        REDIRECTION_DELAY: 800,
        TOAST_DURATION: 5000
    },
    ROUTES: {
        MAIN: "/",
        INFO: "/info",
        LOGIN: "/login",
        REGISTER: "/register"
    }
};

const { UI_TEXT, SETTINGS, ROUTES } = APP_CONFIG;

const ACTIVE_KEYS_KEY = (u) => `active_keys_${String(u || "").toLowerCase()}`;
const PASSWORD_CACHE_KEY = (u) => `user_password_${String(u || "").toLowerCase()}`;
const MEDIA_CACHE_NAME = (u) => `levart_media_cache_${String(u || '').toLowerCase()}`;
const AUTH_TOKEN_KEY = (u) => `auth_token_${String(u || "").toLowerCase()}`;
const OFFLINE_HISTORY_KEY = (u) => `levart_offline_history_${String(u || "").toLowerCase()}`;
const OFFLINE_META_KEY = (u) => `levart_offline_meta_${String(u || "").toLowerCase()}`;
const OFFLINE_STORE_NAME = "offline_chat_meta";
const API_OFFLINE_CACHE_KEY = (u) => `levart_api_cache_${String(u || "anon").toLowerCase()}`;
const CONNECTION_BANNER_ID = "connectionStatusBanner";
const API_CACHE_MAX_ENTRIES = 180;

function getAuthToken(user = "") {
    const u = String(user || localStorage.getItem("username") || "").toLowerCase();
    if (!u) return "";
    return String(localStorage.getItem(AUTH_TOKEN_KEY(u)) || "");
}

function setAuthToken(user, token) {
    const u = String(user || "").toLowerCase();
    const t = String(token || "").trim();
    if (!u || !t) return;
    localStorage.setItem(AUTH_TOKEN_KEY(u), t);
}

function clearAuthToken(user = "") {
    const u = String(user || localStorage.getItem("username") || "").toLowerCase();
    if (!u) return;
    localStorage.removeItem(AUTH_TOKEN_KEY(u));
}

function authJsonHeaders() {
    const headers = { "Content-Type": "application/json" };
    const token = getAuthToken();
    if (token) headers["X-Auth-Token"] = token;
    return headers;
}

function logSilent(scope, err) {
    try {
        console.warn(`[Levart] ${scope}`, err || "");
    } catch {}
}

async function clearSessionMediaCache(user = "") {
    try {
        if (!('caches' in window)) return;
        const u = String(user || localStorage.getItem("username") || "").toLowerCase();
        if (u) await caches.delete(MEDIA_CACHE_NAME(u));
    } catch (e) {
        logSilent("clearSessionMediaCache", e);
    }
}

function normalizeChatId(me, peer) {
    const a = String(me || "").toLowerCase();
    const b = String(peer || "").toLowerCase();
    if (!a || !b) return "";
    if (b.startsWith("group_")) return b;
    return [a, b].sort().join("_");
}

function getStoredActiveKeys(user) {
    if (!user) return null;
    return localStorage.getItem(ACTIVE_KEYS_KEY(user)) || sessionStorage.getItem(ACTIVE_KEYS_KEY(user));
}

function persistAuthSession(user, keyObj, plainPassword = "") {
    if (!user || !keyObj) return;
    localStorage.setItem("username", user);
    localStorage.setItem(ACTIVE_KEYS_KEY(user), keyObj);
    // Миграция со старой схемы: больше не полагаемся на sessionStorage.
    sessionStorage.removeItem(ACTIVE_KEYS_KEY(user));
    if (plainPassword) {
        localStorage.setItem(PASSWORD_CACHE_KEY(user), plainPassword);
    }
    sessionStorage.removeItem("userPassword");
}

function clearAuthSession(user) {
    const u = String(user || localStorage.getItem("username") || "").toLowerCase();
    if (!u) return;
    clearSessionMediaCache(u).catch?.(() => {});
    clearAuthToken(u);
    localStorage.removeItem("username");
    localStorage.removeItem(ACTIVE_KEYS_KEY(u));
    localStorage.removeItem(PASSWORD_CACHE_KEY(u));
    localStorage.removeItem(OFFLINE_HISTORY_KEY(u));
    localStorage.removeItem(OFFLINE_META_KEY(u));
    sessionStorage.removeItem(ACTIVE_KEYS_KEY(u));
    sessionStorage.removeItem("userPassword");
    clearOfflineSessionCache(u).catch?.(() => {});
}

async function ensureAuthTokenForCurrentUser() {
    const u = String(localStorage.getItem("username") || "").toLowerCase();
    if (!u) return "";
    const existing = getAuthToken(u);
    if (existing) return existing;
    const rawPass = localStorage.getItem(PASSWORD_CACHE_KEY(u)) || "";
    if (!rawPass) return "";
    try {
        const hashedPassword = await cryptoMod.hashPassword(rawPass);
        const res = await fetch("/login", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username: u, password: hashedPassword })
        });
        const data = await res.json();
        if (res.ok && data?.auth_token) {
            setAuthToken(u, data.auth_token);
            return data.auth_token;
        }
    } catch (e) {
        logSilent("ensureAuthTokenForCurrentUser", e);
    }
    return "";
}

/**
 * ==========================================
 * 2. ИНИЦИАЛИЗАЦИЯ И ХРАНИЛИЩЕ (IndexedDB)
 * ==========================================
 */
function openDB() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(SETTINGS.DB_NAME, 2);
        request.onupgradeneeded = () => {
            if (!request.result.objectStoreNames.contains(SETTINGS.DB_STORE)) {
                request.result.createObjectStore(SETTINGS.DB_STORE);
            }
            if (!request.result.objectStoreNames.contains(OFFLINE_STORE_NAME)) {
                request.result.createObjectStore(OFFLINE_STORE_NAME);
            }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
}

async function saveOfflineChatPayload(chatId, payload) {
    if (!chatId || !payload) return;
    try {
        const db = await openDB();
        const tx = db.transaction(OFFLINE_STORE_NAME, "readwrite");
        tx.objectStore(OFFLINE_STORE_NAME).put(payload, chatId);
        await new Promise((resolve) => (tx.oncomplete = resolve));
    } catch (e) {
        logSilent("saveOfflineChatPayload", e);
    }
}

async function getOfflineChatPayload(chatId) {
    if (!chatId) return null;
    try {
        const db = await openDB();
        const tx = db.transaction(OFFLINE_STORE_NAME, "readonly");
        const req = tx.objectStore(OFFLINE_STORE_NAME).get(chatId);
        return await new Promise((resolve) => (req.onsuccess = () => resolve(req.result || null)));
    } catch (e) {
        logSilent("getOfflineChatPayload", e);
        return null;
    }
}

async function clearOfflineSessionCache(user = "") {
    const me = String(user || localStorage.getItem("username") || "").toLowerCase();
    if (!me) return;
    try {
        const db = await openDB();
        const tx = db.transaction(OFFLINE_STORE_NAME, "readwrite");
        const store = tx.objectStore(OFFLINE_STORE_NAME);
        const allKeysReq = store.getAllKeys();
        const allKeys = await new Promise((resolve) => (allKeysReq.onsuccess = () => resolve(allKeysReq.result || [])));
        for (const key of allKeys) {
            const k = String(key || "").toLowerCase();
            if (k.includes(me)) store.delete(key);
        }
        await new Promise((resolve) => (tx.oncomplete = resolve));
    } catch (e) {
        logSilent("clearOfflineSessionCache", e);
    }
}

async function fetchHistoryWithOffline(peerId) {
    const me = String(username || localStorage.getItem("username") || "").toLowerCase();
    const peer = String(peerId || "").toLowerCase();
    const chatId = normalizeChatId(me, peer);
    const fallback = async () => {
        const cached = await getOfflineChatPayload(chatId);
        const history = Array.isArray(cached?.history) ? cached.history : [];
        return { history, fromCache: true };
    };

    if (!me || !peer || !chatId) return fallback();

    try {
        const res = await fetch(`/get_history?me=${encodeURIComponent(me)}&friend=${encodeURIComponent(peer)}`);
        if (!res.ok) return fallback();
        const history = await res.json();
        if (!Array.isArray(history)) return fallback();
        await saveOfflineChatPayload(chatId, {
            chat_id: chatId,
            me,
            peer,
            history,
            updated_at: Date.now()
        });
        return { history, fromCache: false };
    } catch {
        return fallback();
    }
}

async function preloadOfflineHistoryForContacts() {
    const me = String(username || localStorage.getItem("username") || "").toLowerCase();
    if (!me) return;
    const contacts = Array.isArray(window._allContacts) ? window._allContacts : [];
    if (!contacts.length) return;

    const peers = contacts
        .map((c) => String(c?.username || "").toLowerCase())
        .filter(Boolean)
        .slice(0, 120);

    const workers = [];
    const concurrency = 3;
    for (let i = 0; i < concurrency; i++) {
        workers.push((async () => {
            while (peers.length) {
                const peer = peers.shift();
                if (!peer) break;
                await fetchHistoryWithOffline(peer);
                await new Promise((r) => setTimeout(r, 35));
            }
        })());
    }
    await Promise.allSettled(workers);
}

async function saveKeyToDB(userId, encryptedData) {
    const db = await openDB();
    const tx = db.transaction(SETTINGS.DB_STORE, "readwrite");
    tx.objectStore(SETTINGS.DB_STORE).put(encryptedData, userId);
    return new Promise((resolve) => (tx.oncomplete = resolve));
}

async function getKeyFromDB(userId) {
    const db = await openDB();
    const tx = db.transaction(SETTINGS.DB_STORE, "readonly");
    const req = tx.objectStore(SETTINGS.DB_STORE).get(userId);
    return new Promise((resolve) => (req.onsuccess = () => resolve(req.result)));
}

/**
 * ==========================================
 * 3. ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ И СОСТОЯНИЕ
 * ==========================================
 */
const socket = typeof io !== 'undefined' ? io() : null;
const username = localStorage.getItem("username");
const sessionId = Math.random().toString(36).substring(7);
let currentChat = null;
const _onlineUsers = new Set();
let currentAES = null;
let decryptedMyKeys = null; 
let replyId = null;
let editMsgId = null;
let isGroupChat = false;
let currentGroupData = null;
let myFriendsList = [];
const sessionAESKeys = {};
const _groupPermCache = {};
let _offlinePreloadInFlight = false;
let _offlinePreloadDoneForUser = "";

function scheduleOfflinePreload() {
    const me = String(username || localStorage.getItem("username") || "").toLowerCase();
    if (!me) return;
    if (_offlinePreloadInFlight) return;
    if (_offlinePreloadDoneForUser === me) return;
    _offlinePreloadInFlight = true;
    setTimeout(async () => {
        try {
            await preloadOfflineHistoryForContacts();
            _offlinePreloadDoneForUser = me;
        } catch (e) {
            logSilent("scheduleOfflinePreload", e);
        } finally {
            _offlinePreloadInFlight = false;
        }
    }, 250);
}

const _nativeFetch = window.fetch.bind(window);
let _connectionMonitorStarted = false;
let _connectionPingTimer = null;
const _connectionState = {
    internet: navigator.onLine,
    server: true
};

function getCurrentUserForCache() {
    return String(localStorage.getItem("username") || "anon").toLowerCase();
}

function getApiOfflineCacheMap() {
    try {
        const raw = localStorage.getItem(API_OFFLINE_CACHE_KEY(getCurrentUserForCache()));
        const parsed = raw ? JSON.parse(raw) : {};
        return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
        return {};
    }
}

function setApiOfflineCacheMap(map) {
    try {
        localStorage.setItem(API_OFFLINE_CACHE_KEY(getCurrentUserForCache()), JSON.stringify(map || {}));
    } catch {}
}

function normalizeApiCacheKey(urlObj) {
    return `${String(urlObj.pathname || "")}${String(urlObj.search || "")}`;
}

function isOfflineCacheablePath(pathname) {
    const p = String(pathname || "");
    return (
        p.startsWith("/api/my_contacts/") ||
        p.startsWith("/api/stories_feed/") ||
        p.startsWith("/api/user_profile/") ||
        p.startsWith("/api/group_info/") ||
        p.startsWith("/api/pinned_messages") ||
        p.startsWith("/api/last_read") ||
        p.startsWith("/api/online_status") ||
        p.startsWith("/get_friends") ||
        p.startsWith("/get_history")
    );
}

async function saveApiResponseToOfflineCache(urlObj, response) {
    try {
        const contentType = String(response.headers.get("content-type") || "").toLowerCase();
        if (!contentType.includes("application/json") && !contentType.startsWith("text/")) return;
        const body = await response.text();
        if (!body || body.length > 1_500_000) return;
        const cacheMap = getApiOfflineCacheMap();
        const key = normalizeApiCacheKey(urlObj);
        cacheMap[key] = {
            status: Number(response.status || 200),
            statusText: String(response.statusText || "OK"),
            headers: Array.from(response.headers.entries()),
            body,
            ts: Date.now()
        };
        const entries = Object.entries(cacheMap);
        if (entries.length > API_CACHE_MAX_ENTRIES) {
            entries
                .sort((a, b) => Number(a[1]?.ts || 0) - Number(b[1]?.ts || 0))
                .slice(0, entries.length - API_CACHE_MAX_ENTRIES)
                .forEach(([k]) => delete cacheMap[k]);
        }
        setApiOfflineCacheMap(cacheMap);
    } catch {}
}

function loadApiResponseFromOfflineCache(urlObj) {
    try {
        const cacheMap = getApiOfflineCacheMap();
        const key = normalizeApiCacheKey(urlObj);
        const item = cacheMap[key];
        if (!item || typeof item.body !== "string") return null;
        return new Response(item.body, {
            status: Number(item.status || 200),
            statusText: String(item.statusText || "OK"),
            headers: new Headers(Array.isArray(item.headers) ? item.headers : [])
        });
    } catch {
        return null;
    }
}

function ensureConnectionBannerElement() {
    let el = document.getElementById(CONNECTION_BANNER_ID);
    if (el) return el;
    el = document.createElement("div");
    el.id = CONNECTION_BANNER_ID;
    el.className = "connection-status-banner";
    el.setAttribute("role", "status");
    el.setAttribute("aria-live", "polite");
    document.body.appendChild(el);
    return el;
}

function renderConnectionBanner() {
    const el = ensureConnectionBannerElement();
    if (!_connectionState.internet) {
        el.textContent = "Нет подключения к интернету";
        el.classList.add("visible");
        return;
    }
    if (!_connectionState.server) {
        el.textContent = "Нет подключения к серверу";
        el.classList.add("visible");
        return;
    }
    el.classList.remove("visible");
}

function setConnectionState(nextState = {}) {
    if (typeof nextState.internet === "boolean") _connectionState.internet = nextState.internet;
    if (typeof nextState.server === "boolean") _connectionState.server = nextState.server;
    renderConnectionBanner();
}

async function checkServerConnection() {
    if (!navigator.onLine) {
        setConnectionState({ internet: false, server: false });
        return false;
    }
    try {
        const ctrl = new AbortController();
        const tm = setTimeout(() => ctrl.abort(), 5000);
        const res = await _nativeFetch(`/api/online_status?t=${Date.now()}`, { cache: "no-store", signal: ctrl.signal });
        clearTimeout(tm);
        const ok = !!res && res.ok;
        setConnectionState({ internet: true, server: ok });
        return ok;
    } catch {
        setConnectionState({ internet: true, server: false });
        return false;
    }
}

function installOfflineAwareFetch() {
    if (window.__levartOfflineFetchInstalled) return;
    window.__levartOfflineFetchInstalled = true;
    window.fetch = async (input, init = {}) => {
        const req = new Request(input, init);
        const method = String((init.method || req.method || "GET")).toUpperCase();
        let urlObj;
        try {
            urlObj = new URL(req.url, window.location.origin);
        } catch {
            return _nativeFetch(input, init);
        }
        const sameOrigin = urlObj.origin === window.location.origin;
        const shouldHandle = sameOrigin && method === "GET";
        const cacheable = shouldHandle && isOfflineCacheablePath(urlObj.pathname);

        if (!shouldHandle) {
            return _nativeFetch(input, init);
        }

        try {
            const ctrl = new AbortController();
            const timeoutMs = cacheable ? 7000 : 12000;
            const tm = setTimeout(() => ctrl.abort(), timeoutMs);
            const response = await _nativeFetch(input, { ...init, signal: ctrl.signal });
            clearTimeout(tm);
            if (navigator.onLine) setConnectionState({ internet: true, server: true });
            if (cacheable && response?.ok) {
                await saveApiResponseToOfflineCache(urlObj, response.clone());
            }
            if (cacheable && response && !response.ok) {
                const cachedResponse = loadApiResponseFromOfflineCache(urlObj);
                if (cachedResponse) return cachedResponse;
            }
            return response;
        } catch (err) {
            if (navigator.onLine) setConnectionState({ internet: true, server: false });
            if (cacheable) {
                const cachedResponse = loadApiResponseFromOfflineCache(urlObj);
                if (cachedResponse) return cachedResponse;
            }
            throw err;
        }
    };
}

function registerLavertServiceWorker() {
    if (!("serviceWorker" in navigator)) return;
    window.addEventListener("load", () => {
        navigator.serviceWorker.register("/static/sw.js").catch(() => {});
    }, { once: true });
}

function initConnectionMonitor() {
    if (_connectionMonitorStarted) return;
    _connectionMonitorStarted = true;
    installOfflineAwareFetch();
    registerLavertServiceWorker();
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", () => {
            ensureConnectionBannerElement();
            renderConnectionBanner();
        }, { once: true });
    } else {
        ensureConnectionBannerElement();
        renderConnectionBanner();
    }
    window.addEventListener("online", () => {
        setConnectionState({ internet: true });
        checkServerConnection();
    });
    window.addEventListener("offline", () => {
        setConnectionState({ internet: false, server: false });
    });
    checkServerConnection();
    _connectionPingTimer = setInterval(checkServerConnection, 15000);
}

initConnectionMonitor();


// ─── КЭШИРОВАННЫЕ ПРЕВЬЮ СООБЩЕНИЙ ───────────────────────────────
const MSG_PREVIEW_KEY = () => {
    const u = username || localStorage.getItem('username') || 'anon';
    return `levart_previews_${u}`;
};

function loadMsgPreviews() {
    try { return JSON.parse(localStorage.getItem(MSG_PREVIEW_KEY()) || '{}'); }
    catch { return {}; }
}

async function initAllPreviews() {
    if (!username) return;
    const previews = loadMsgPreviews();
    const contacts = window._allContacts;
    if (!contacts || contacts.length === 0) return;

    for (const c of contacts) {
        const chatId = c.username.startsWith('group_')
            ? c.username
            : [username, c.username].sort().join('_');
        
        // Пропускаем если превью уже есть
        if (previews[chatId]) continue;

        try {
            // Получаем только последнее сообщение
            const fetched = await fetchHistoryWithOffline(c.username);
            const history = Array.isArray(fetched?.history) ? fetched.history : [];
            if (!history || history.length === 0) continue;

            const packet = history[history.length - 1];
            
            // Получаем ключ
            let aesKey = sessionAESKeys[c.username];
            if (!aesKey) {
                try {
                    aesKey = await getAESKeyForPeer(c.username);
                    sessionAESKeys[c.username] = aesKey;
                } catch { continue; }
            }

            const decrypted = await cryptoMod.decrypt(aesKey, packet.cipher);
            updateMsgPreview(packet.from, decrypted, chatId);

        } catch(e) {
            // Тихо пропускаем ошибки
        }
    }

    // Перерисовываем список с обновлёнными превью
    const folder = getAllFolders().find(f => f.id === window._activeFolder) || BUILT_IN_FOLDERS[0];
    renderContactsList(filterContactsByFolder(window._allContacts, folder));
}

function saveMsgPreview(chatId, preview) {
    const data = loadMsgPreviews();
    data[chatId] = preview;
    localStorage.setItem(MSG_PREVIEW_KEY(), JSON.stringify(data));
}

function getChatIdForPeer(peer) {
    return peer.startsWith('group_') ? peer : [username, peer].sort().join('_');
}

function _effectiveGroupPerm(perms, owner, member, key) {
    const me = String(member || '').toLowerCase();
    if (!me) return false;
    if (me === String(owner || '').toLowerCase()) return true;
    const defaults = perms?.defaults || {};
    const memberPatch = perms?.members?.[me] || {};
    if (Object.prototype.hasOwnProperty.call(memberPatch, key)) return !!memberPatch[key];
    return !!defaults[key];
}

async function fetchGroupPermissionsForChat(groupId) {
    const gid = String(groupId || '').trim();
    if (!gid || !gid.startsWith('group_')) return null;
    try {
        const res = await fetch(`/api/group_permissions/${encodeURIComponent(gid)}?me=${encodeURIComponent(username)}`);
        const data = await res.json();
        if (!res.ok) return null;
        _groupPermCache[gid] = { owner: data.owner || '', permissions: data.permissions || {} };
        return _groupPermCache[gid];
    } catch {
        return null;
    }
}

async function canSendByGroupPermission(chatId, permKey) {
    const cid = String(chatId || '');
    if (!cid.startsWith('group_')) return true;
    let g = _groupPermCache[cid];
    if (!g) g = await fetchGroupPermissionsForChat(cid);
    if (!g) return true;
    return _effectiveGroupPerm(g.permissions || {}, g.owner || '', username, permKey);
}

let _chatPins = [];
const _chatPinCursor = {};
const _chatPinHidden = {};
let _chatPinsScrollBound = false;
const _chatPinNavLockUntil = {};

function getPinOrderValue(pin) {
    try {
        const mid = String(pin?.msg_id || '');
        if (mid.includes('_')) {
            const raw = Number(mid.split('_').pop());
            if (Number.isFinite(raw) && raw > 0) return raw > 1e12 ? raw : raw * 1000;
        }
    } catch {}
    const ts = Number(pin?.timestamp || 0);
    if (Number.isFinite(ts) && ts > 0) return ts > 1e12 ? ts : ts * 1000;
    return 0;
}

function getMessagesOrderMap() {
    const map = new Map();
    document.querySelectorAll('#messages .msg[data-id]').forEach((el, i) => {
        const id = String(el.getAttribute('data-id') || '');
        if (id) map.set(id, i);
    });
    return map;
}

function getPinsOrderedByDomPosition() {
    const orderMap = getMessagesOrderMap();
    return (_chatPins || [])
        .map((p) => ({ ...p, __ord: orderMap.get(String(p?.msg_id || '')) }))
        .filter((p) => Number.isFinite(p.__ord))
        .sort((a, b) => b.__ord - a.__ord); // newest -> oldest by actual DOM order
}

function decoratePinnedMessages() {
    const host = document.getElementById('messages');
    if (!host) return;
    host.querySelectorAll('.msg[data-id]').forEach((msg) => {
        const mid = String(msg.getAttribute('data-id') || '');
        const pinned = isMessagePinned(mid);
        msg.classList.toggle('msg-is-pinned', pinned);
        let mark = msg.querySelector('.msg-pin-mark');
        if (pinned) {
            if (!mark) {
                mark = document.createElement('span');
                mark.className = 'msg-pin-mark';
                mark.textContent = '📌';
                msg.appendChild(mark);
            }
        } else if (mark) {
            mark.remove();
        }
    });
}

function getTopVisibleMessageOrder() {
    const host = document.getElementById('messages');
    if (!host) return -1;
    const nodes = [...host.querySelectorAll('.msg[data-id]')];
    if (!nodes.length) return -1;
    const top = host.getBoundingClientRect().top + 6;
    for (let i = 0; i < nodes.length; i++) {
        const r = nodes[i].getBoundingClientRect();
        if (r.bottom >= top) return i;
    }
    return nodes.length - 1;
}

function findNearestPinnedAboveIndex(list) {
    if (!list.length) return 0;
    const topIdx = getTopVisibleMessageOrder();
    if (topIdx < 0) return 0;

    let bestIdx = -1;
    let bestOrder = -1;
    list.forEach((p, i) => {
        const ord = Number(p?.__ord);
        if (!Number.isFinite(ord)) return;
        if (ord < topIdx && ord > bestOrder) {
            bestOrder = ord;
            bestIdx = i;
        }
    });
    if (bestIdx >= 0) return bestIdx;

    let fallbackIdx = 0;
    let fallbackOrder = Number.POSITIVE_INFINITY;
    list.forEach((p, i) => {
        const ord = Number(p?.__ord);
        if (!Number.isFinite(ord)) return;
        if (ord >= topIdx && ord < fallbackOrder) {
            fallbackOrder = ord;
            fallbackIdx = i;
        }
    });
    return fallbackIdx;
}

function getPinnedEntriesForMessage(msgId) {
    return (_chatPins || []).filter((p) => String(p.msg_id || '') === String(msgId || ''));
}

function isMessagePinned(msgId) {
    return getPinnedEntriesForMessage(msgId).length > 0;
}

async function loadChatPins(peer) {
    const target = String(peer || window.currentChat || '').trim();
    if (!target) return;
    const chatId = getChatIdForPeer(target);
    try {
        const res = await fetch(`/api/pinned_messages?me=${encodeURIComponent(username)}&chat_id=${encodeURIComponent(chatId)}`);
        const data = await res.json();
        _chatPins = (Array.isArray(data) ? data : [])
            .sort((a, b) => getPinOrderValue(b) - getPinOrderValue(a)); // newest -> oldest
        _chatPinCursor[chatId] = 0;
        _chatPinNavLockUntil[chatId] = 0;
        if (_chatPins.length) _chatPinHidden[chatId] = false;
        decoratePinnedMessages();
        renderChatPins();
    } catch {
        _chatPins = [];
        decoratePinnedMessages();
        renderChatPins();
    }
}

function renderChatPins() {
    const bar = document.getElementById('chatPinnedBar');
    if (!bar) return;
    const list = getPinsOrderedByDomPosition();
    if (!list.length || !window.currentChat) {
        bar.style.display = 'none';
        bar.innerHTML = '';
        return;
    }
    const chatId = getChatIdForPeer(window.currentChat);
    if (_chatPinHidden[chatId]) {
        bar.style.display = 'none';
        bar.innerHTML = '';
        return;
    }
    const locked = Date.now() < Number(_chatPinNavLockUntil[chatId] || 0);
    const nearestIdx = findNearestPinnedAboveIndex(list);
    const idx = Math.max(0, Math.min(
        locked ? Number(_chatPinCursor[chatId] || 0) : (Number.isFinite(nearestIdx) ? nearestIdx : Number(_chatPinCursor[chatId] || 0)),
        list.length - 1
    ));
    _chatPinCursor[chatId] = idx;
    const p = list[idx];
    const text = (getCompactPinPreviewText(p) || 'Сообщение').replace(/</g, '&lt;');
    bar.style.display = 'flex';
    bar.innerHTML = `
        <div class="chat-pinned-main" id="chatPinPrimary">
            <div class="chat-pinned-icon">📌</div>
            <div class="chat-pinned-texts">
                <div class="chat-pinned-title">${idx + 1}/${list.length}</div>
                <div class="chat-pinned-snippet">${text}</div>
            </div>
        </div>
        <div class="chat-pinned-controls">
            <button class="chat-pin-btn" id="chatPinUnpin">⛔</button>
            <button class="chat-pin-btn" id="chatPinNext">↑</button>
            <button class="chat-pin-btn" id="chatPinHide">✕</button>
        </div>
    `;
    bar.querySelector('#chatPinPrimary')?.addEventListener('click', () => {
        scrollToMessage(p.msg_id);
        _chatPinCursor[chatId] = Math.min(list.length - 1, idx + 1);
        _chatPinNavLockUntil[chatId] = Date.now() + 1400;
        setTimeout(() => renderChatPins(), 700);
    });
    bar.querySelector('#chatPinNext')?.addEventListener('click', (e) => {
        e.stopPropagation();
        scrollToMessage(p.msg_id);
        _chatPinCursor[chatId] = Math.min(list.length - 1, idx + 1);
        _chatPinNavLockUntil[chatId] = Date.now() + 1400;
        setTimeout(() => renderChatPins(), 700);
    });
    bar.querySelector('#chatPinUnpin')?.addEventListener('click', async (e) => {
        e.stopPropagation();
        if (!p?.pin_id) return;
        const ok = await confirmModal('Открепить сообщение', 'Вы точно хотите открепить это сообщение?');
        if (!ok) return;
        try {
            const res = await fetch('/api/unpin_message', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ me: username, chat_id: chatId, pin_id: p.pin_id, scope: p.scope || 'self' })
            });
            if (!res.ok) throw new Error('unpin_failed');
            await loadChatPins(window.currentChat);
            showToast('Закреп откреплен');
        } catch {
            showToast('Не удалось открепить', 'error');
        }
    });
    bar.querySelector('#chatPinHide')?.addEventListener('click', (e) => {
        e.stopPropagation();
        _chatPinHidden[chatId] = true;
        renderChatPins();
    });
}

function getCompactPinPreviewText(pin) {
    const raw = String(pin?.preview || pin?.text || '').trim();
    const low = raw.toLowerCase();
    if (!raw) return 'Сообщение';
    if (low.includes('голос') || low.includes('voice')) return 'Файл';
    if (low.includes('файл') || low.includes('file')) return 'Файл';
    if (low.includes('видео') || low.includes('video') || low.includes('круж')) return 'Медиа';
    if (low.includes('изображ') || low.includes('фото') || low.includes('image') || low.includes('gif')) return 'Медиа';
    return raw;
}

function bindChatPinsScrollTracking() {
    if (_chatPinsScrollBound) return;
    const host = document.getElementById('messages');
    if (!host) return;
    _chatPinsScrollBound = true;
    let t = null;
    host.addEventListener('scroll', () => {
        if (!window.currentChat || !_chatPins?.length) return;
        const chatId = getChatIdForPeer(window.currentChat);
        if (Date.now() < Number(_chatPinNavLockUntil[chatId] || 0)) return;
        if (t) clearTimeout(t);
        t = setTimeout(() => renderChatPins(), 60);
    }, { passive: true });
}

function closePinScopeChooser() {
    document.querySelectorAll('.msg-pin-scope-chooser').forEach(el => el.remove());
}

window.openPinScopeChooser = (msgId, btn) => {
    openPinScopeModal(msgId, btn);
};

window.openPinScopeModal = (msgId, btn) => {
    const modal = document.getElementById("customModal");
    const titleEl = document.getElementById("modalTitle");
    const textEl = document.getElementById("modalText");
    const input = document.getElementById("modalInput");
    const confirmBtn = document.getElementById("modalConfirm");
    const cancelBtn = document.getElementById("modalCancel");
    if (!modal || !titleEl || !textEl || !input || !confirmBtn || !cancelBtn) return;

    const old = {
        title: titleEl.textContent,
        text: textEl.textContent,
        confirm: confirmBtn.textContent,
        cancel: cancelBtn.textContent,
        inputDisplay: input.style.display,
        cancelDisplay: cancelBtn.style.display
    };

    const cleanup = () => {
        const curConfirm = document.getElementById("modalConfirm");
        const curCancel = document.getElementById("modalCancel");
        titleEl.textContent = old.title || "Подтверждение";
        textEl.textContent = old.text || "";
        if (curConfirm) curConfirm.textContent = old.confirm || "Подтвердить";
        if (curCancel) curCancel.textContent = old.cancel || "Отмена";
        input.style.display = old.inputDisplay || "none";
        if (curCancel) curCancel.style.display = old.cancelDisplay || "inline-block";
        modal.onclick = null;
    };

    btn?.closest('.msg-dropdown')?.classList.remove('open');

    titleEl.textContent = "Закрепить сообщение";
    textEl.textContent = "У кого закрепить это сообщение?";
    input.style.display = "none";
    cancelBtn.style.display = "inline-block";
    confirmBtn.textContent = "Для всех";
    cancelBtn.textContent = "У меня";
    modal.style.display = "flex";

    confirmBtn.replaceWith(confirmBtn.cloneNode(true));
    cancelBtn.replaceWith(cancelBtn.cloneNode(true));
    const cf = document.getElementById("modalConfirm");
    const cc = document.getElementById("modalCancel");
    cf.textContent = "Для всех";
    cc.textContent = "У меня";

    cf.addEventListener('click', () => {
        modal.style.display = "none";
        cleanup();
        pinMessageFromMenu(msgId, 'all', btn);
    });
    cc.addEventListener('click', () => {
        modal.style.display = "none";
        cleanup();
        pinMessageFromMenu(msgId, 'self', btn);
    });
    modal.onclick = (e) => {
        if (e.target === modal) {
            modal.style.display = "none";
            cleanup();
        }
    };
};

window.openUnpinScopeChooser = (msgId, btn) => {
    closePinScopeChooser();
    const parent = btn.closest('.msg-dropdown') || btn.parentElement;
    if (!parent) return;
    const pins = getPinnedEntriesForMessage(msgId);
    if (!pins.length) {
        showToast('Сообщение не закреплено');
        return;
    }
    const chooser = document.createElement('div');
    chooser.className = 'msg-pin-scope-chooser';
    chooser.innerHTML = pins.map((p) => {
        const label = p.scope === 'all' ? '📣 Открепить у всех' : '🔒 Открепить у меня';
        return `<button data-pin-id="${p.pin_id}" data-scope="${p.scope || 'self'}">${label}</button>`;
    }).join('');
    chooser.querySelectorAll('button').forEach((b) => {
        b.addEventListener('click', () => unpinMessageFromMenu(msgId, b.dataset.pinId || '', b.dataset.scope || 'self', btn));
    });
    parent.appendChild(chooser);
};

window.unpinMessageFromMenu = async (msgId, pinId, scope, btn) => {
    const chatId = getChatIdForPeer(window.currentChat);
    if (!chatId || !pinId) return;
    const ok = await confirmModal('Открепить сообщение', 'Вы точно хотите открепить это сообщение?');
    if (!ok) return;
    try {
        const res = await fetch('/api/unpin_message', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ me: username, chat_id: chatId, pin_id: pinId, scope })
        });
        if (!res.ok) throw new Error('unpin_failed');
        closePinScopeChooser();
        btn?.closest('.msg-dropdown')?.classList.remove('open');
        await loadChatPins(window.currentChat);
        showToast('Закреп откреплен');
    } catch {
        showToast('Не удалось открепить', 'error');
    }
};

window.copyMessageFromMenu = async (msgId, btn) => {
    const msg = document.querySelector(`.msg[data-id="${msgId}"]`);
    if (!msg) return;
    const text = msg.querySelector('.msg-text')?.innerText?.trim() || msg.querySelector('.file-name-sub')?.innerText?.trim() || '';
    if (!text) {
        showToast('Нечего копировать', 'error');
        return;
    }
    try {
        if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(text);
        else {
            const ta = document.createElement('textarea');
            ta.value = text;
            document.body.appendChild(ta);
            ta.select();
            document.execCommand('copy');
            ta.remove();
        }
        btn?.closest('.msg-dropdown')?.classList.remove('open');
        showToast('Скопировано');
    } catch {
        showToast('Не удалось скопировать', 'error');
    }
};

window.openForwardMessageModal = (msgId, btn) => {
    btn?.closest('.msg-dropdown')?.classList.remove('open');
    const msgEl = document.querySelector(`.msg[data-id="${msgId}"]`);
    if (!msgEl) {
        showToast('Сообщение не найдено', 'error');
        return;
    }

    const contacts = (window._allContacts || []).filter((c) => !!c?.username);
    if (!contacts.length) {
        showToast('Нет чатов для пересылки', 'error');
        return;
    }

    document.getElementById('forwardOverlay')?.remove();
    const overlay = document.createElement('div');
    overlay.id = 'forwardOverlay';
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
        <div class="modal-content card" style="max-width:420px;padding:18px 16px;">
            <h3 style="margin-bottom:10px;">Переслать сообщение</h3>
            <input id="forwardSearchInput" type="text" placeholder="Поиск чата..." style="margin-bottom:10px;">
            <div id="forwardChatList" style="max-height:48vh;overflow:auto;display:flex;flex-direction:column;gap:7px;"></div>
            <div class="modal-buttons" style="margin-top:12px;">
                <button id="forwardCancelBtn" class="secondary">Отмена</button>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);

    const listEl = overlay.querySelector('#forwardChatList');
    const searchEl = overlay.querySelector('#forwardSearchInput');

    const render = (query = '') => {
        const q = String(query || '').trim().toLowerCase();
        const filtered = contacts.filter((c) => {
            const n = String(c.display_name || '').toLowerCase();
            const u = String(c.username || '').toLowerCase();
            return !q || n.includes(q) || u.includes(q);
        });
        if (!filtered.length) {
            listEl.innerHTML = '<div class="empty-state-note">Ничего не найдено</div>';
            return;
        }
        listEl.innerHTML = '';
        filtered.forEach((c) => {
            const row = document.createElement('button');
            row.type = 'button';
            row.className = 'forward-chat-row';
            const title = (c.display_name || c.username || '').replace(/</g, '&lt;');
            const sub = String(c.username || '').replace(/</g, '&lt;');
            row.innerHTML = `<span style="font-weight:700;">${title}</span><span style="opacity:.72;font-size:12px;">@${sub}</span>`;
            row.onclick = async () => {
                await forwardMessageToChat(msgId, c.username);
                overlay.remove();
            };
            listEl.appendChild(row);
        });
    };

    render('');
    searchEl?.addEventListener('input', (e) => render(e.target?.value || ''));
    overlay.querySelector('#forwardCancelBtn')?.addEventListener('click', () => overlay.remove());
    overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
};

async function forwardMessageToChat(msgId, targetChat) {
    const msgEl = document.querySelector(`.msg[data-id="${msgId}"]`);
    if (!msgEl) return;
    const payload = msgEl._rawContent;
    const originalSender = String(msgEl._rawSender || '').trim().toLowerCase() || username;
    if (payload === undefined || payload === null) {
        showToast('Невозможно переслать это сообщение', 'error');
        return;
    }

    const peer = String(targetChat || '').trim();
    if (!peer) return;
    try {
        const aes = sessionAESKeys[peer] || await getAESKeyForPeer(peer);
        if (!sessionAESKeys[peer]) sessionAESKeys[peer] = aes;

        let rawText = '';
        let msgType = 'text';
        let mediaKind = '';

        if (typeof payload === 'object' && payload.url && payload.file_key) {
            msgType = 'file';
            mediaKind = String(payload.type || '').toLowerCase();
            rawText = '__FILE__' + JSON.stringify(payload);
        } else {
            rawText = String(payload || '');
            if (rawText.trim().startsWith('{')) {
                try {
                    const d = JSON.parse(rawText.trim());
                    if (d.__STICKER__) msgType = 'sticker';
                    else if (d.__GIF__) msgType = 'gif';
                } catch {}
            }
        }

        if (peer.startsWith('group_')) {
            if (msgType === 'text' && !await canSendByGroupPermission(peer, 'can_send_messages')) {
                showToast('Нет прав писать в этой группе', 'error');
                return;
            }
            if (msgType === 'text' && /(https?:\/\/|www\.)\S+/i.test(rawText) && !await canSendByGroupPermission(peer, 'can_send_links')) {
                showToast('Нет прав отправлять ссылки', 'error');
                return;
            }
            if (msgType === 'sticker' && !await canSendByGroupPermission(peer, 'can_send_stickers')) {
                showToast('Нет прав отправлять стикеры', 'error');
                return;
            }
            if (msgType === 'gif' && !await canSendByGroupPermission(peer, 'can_send_gifs')) {
                showToast('Нет прав отправлять GIF', 'error');
                return;
            }
            if (msgType === 'file' && !await canSendByGroupPermission(peer, 'can_send_media')) {
                showToast('Нет прав отправлять медиа', 'error');
                return;
            }
            if (msgType === 'file' && (mediaKind === 'voice' || mediaKind === 'audio') && !await canSendByGroupPermission(peer, 'can_send_voice')) {
                showToast('Нет прав отправлять голосовые', 'error');
                return;
            }
            if (msgType === 'file' && mediaKind === 'video_note' && !await canSendByGroupPermission(peer, 'can_send_video_notes')) {
                showToast('Нет прав отправлять кружки', 'error');
                return;
            }
        }

        const cipher = await cryptoMod.encrypt(aes, rawText);
        socket.emit('send_message', {
            from: username,
            to: peer,
            type: msgType,
            media_kind: mediaKind || undefined,
            cipher,
            forwarded_from: originalSender
        });
        setTimeout(() => {
            syncMyContacts().catch(() => {});
            if (String(window.currentChat || '') === peer) loadHistory(peer).catch(() => {});
        }, 120);
        showToast('Сообщение переслано');
    } catch {
        showToast('Не удалось переслать сообщение', 'error');
    }
}

window.pinMessageFromMenu = async (msgId, scope, btn) => {
    const msg = document.querySelector(`.msg[data-id="${msgId}"]`);
    if (!msg || !window.currentChat) return;
    const text = msg.querySelector('.msg-text')?.textContent?.trim() || msg.querySelector('.file-name')?.textContent?.trim() || 'Сообщение';
    const sender = msg.classList.contains('msg-me') ? username : (msg.querySelector('.msg-info')?.textContent || '');
    const time = msg.querySelector('.msg-time')?.textContent || '';
    const chatId = getChatIdForPeer(window.currentChat);
    try {
        const res = await fetch('/api/pin_message', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ me: username, chat_id: chatId, msg_id: msgId, scope, preview: text.slice(0, 220), sender, time })
        });
        if (!res.ok) throw new Error('pin_failed');
        closePinScopeChooser();
        btn?.closest('.msg-dropdown')?.classList.remove('open');
        await loadChatPins(window.currentChat);
        showToast('Сообщение закреплено');
    } catch {
        showToast('Не удалось закрепить', 'error');
    }
};

// Обновляем превью при отображении сообщения — вызывать после addMessageToScreen
function updateMsgPreview(from, text, chatId) {
    if (!chatId) return;
    let preview = '';
    const isMe = from === username;
    const isSystem = String(from || '').toLowerCase() === 'system';
    const prefix = isSystem ? '' : (isMe ? 'Вы: ' : (window.currentChat?.startsWith('group_') ? `${from}: ` : ''));
    const toPreviewPlain = (raw) => {
        let s = String(raw || '');
        if (!s) return '';
        s = s
            .replace(/```[\s\S]*?```/g, ' [код] ')
            .replace(/`([^`\n]+?)`/g, '$1')
            .replace(/\*\*([\s\S]*?)\*\*/g, '$1')
            .replace(/__([\s\S]*?)__/g, '$1')
            .replace(/\*([\s\S]*?)\*/g, '$1')
            .replace(/~~([\s\S]*?)~~/g, '$1')
            .replace(/\|\|([\s\S]*?)\|\|/g, '$1')
            .replace(/^\s{0,3}#{1,6}\s+/gm, '')
            .replace(/^\s*[-*]\s+/gm, '')
            .replace(/^\s*\d+\.\s+/gm, '')
            .replace(/^\s*&gt;\s?/gm, '')
            .replace(/[\r\n]+/g, ' ')
            .replace(/\s{2,}/g, ' ')
            .trim();
        return s;
    };

    if (typeof text === 'object' && text !== null) {
        // Файл или изображение
        if (text.type === 'image' || (text.mime && text.mime.startsWith('image/'))) preview = prefix + '🖼 Фото';
        else if (text.type === 'video' || (text.mime && text.mime.startsWith('video/'))) preview = prefix + '🎬 Видео';
        else if (text.type === 'video_note') preview = prefix + '⭕ Кружок';
        else if (text.type === 'voice') preview = prefix + '🎤 Голосовое';
        else preview = prefix + `📄 ${text.name || 'Файл'}`;
    } else if (typeof text === 'string') {
        if (text.startsWith('data:image') || text.includes('__IMG__')) preview = prefix + '🖼 Фото';
        else if (text.startsWith('data:video')) preview = prefix + '🎬 Видео';
        else if (text.startsWith('⚠️')) preview = prefix + '⚠️ Ошибка';
        else if (text.trim().startsWith('{')) {
            // JSON — стикер или GIF
            try {
                const d = JSON.parse(text.trim());
                if (d.__STICKER__) preview = prefix + '🎭 Стикер';
                else if (d.__GIF__) preview = prefix + '🎬 GIF';
                else {
                    const clean = toPreviewPlain(text);
                    preview = prefix + (clean.length > 50 ? clean.slice(0, 48) + '…' : clean);
                }
            } catch {
                const clean = toPreviewPlain(text);
                preview = prefix + (clean.length > 50 ? clean.slice(0, 48) + '…' : clean);
            }
        } else {
            const cleanBase = isSystem ? stripSystemPrefix(text) : text;
            const clean = toPreviewPlain(cleanBase);
            preview = prefix + (clean.length > 50 ? clean.slice(0, 48) + '…' : clean);
        }
    }

    saveMsgPreview(chatId, preview);
}

/**
 * ==========================================
 * 4. AUTH GUARD
 * ==========================================
 */
(function authGuard() {
    const user = localStorage.getItem("username");
    const localKeys = user ? localStorage.getItem(ACTIVE_KEYS_KEY(user)) : null;
    const sessionKeys = user ? sessionStorage.getItem(ACTIVE_KEYS_KEY(user)) : null;
    const hasKeys = localKeys || sessionKeys;
    const path = window.location.pathname;

    // Миграция: если ключи были только в sessionStorage, переносим в localStorage.
    if (user && sessionKeys && !localKeys) {
        persistAuthSession(user, sessionKeys, sessionStorage.getItem("userPassword") || "");
    }

    if (user && hasKeys) {
        if (path === ROUTES.LOGIN || path === ROUTES.REGISTER || path === ROUTES.INFO) {
            window.location.replace(ROUTES.MAIN);
        }
    } else {
        if (path === ROUTES.MAIN) {
            const inviteToken = new URLSearchParams(window.location.search).get('invite');
            if (inviteToken) {
                localStorage.setItem('pending_invite_token', inviteToken);
            }
            localStorage.removeItem("username");
            if (user) {
                localStorage.removeItem(ACTIVE_KEYS_KEY(user));
                localStorage.removeItem(PASSWORD_CACHE_KEY(user));
            }
            window.location.replace(ROUTES.INFO);
        }
    }
})();

/**
 * ==========================================
 * 5. СЛОЙ БЕЗОПАСНОСТИ (КРИПТОГРАФИЯ)
 * ==========================================
 */
async function getMyPersistentKeys() {
    if (decryptedMyKeys) return decryptedMyKeys;
    const sessionData = getStoredActiveKeys(username);
    if (sessionData) {
        try {
            const data = JSON.parse(sessionData);
            const privBuf = new Uint8Array(atob(data.priv).split("").map(c => c.charCodeAt(0)));
            const privateKey = await crypto.subtle.importKey(
                "pkcs8", privBuf, { name: "ECDH", namedCurve: "P-256" }, true, ["deriveKey"]
            );
            const publicKey = await cryptoMod.importPublicKey(data.pub);
            decryptedMyKeys = { publicKey, privateKey };
            return decryptedMyKeys;
        } catch (e) { console.error(UI_TEXT.CRYPTO_ERR_KEYS, e); }
    }
    return null;
}

async function getAESKeyForPeer(targetId) {
    const myKeys = await getMyPersistentKeys();
    if (!myKeys) throw new Error("Нет локальных ключей");

    // Если это ГРУППА
    // Внутри getAESKeyForPeer для групп
    if (targetId.startsWith("group_")) {
        const resp = await fetch(`/api/group_info/${targetId}`);
        const groupInfo = await resp.json();
        const encMap = (groupInfo && typeof groupInfo.encrypted_keys === 'object') ? groupInfo.encrypted_keys : {};
        const myEntry = encMap[username] || encMap[String(username || '').toLowerCase()] || encMap[String(username || '').toUpperCase()] || (() => {
            const wanted = String(username || '').toLowerCase();
            const hitKey = Object.keys(encMap).find((k) => String(k || '').toLowerCase() === wanted);
            return hitKey ? encMap[hitKey] : null;
        })();
        if (!myEntry) {
            throw new Error("У вас нет доступа к ключу этой группы");
        }

        const myKeys = await getMyPersistentKeys(); // Здесь получаем объект CryptoKey
        const encryptedData = myEntry;
        let byUser = String(groupInfo.owner || '').toLowerCase();
        let cipher = encryptedData;
        if (encryptedData && typeof encryptedData === 'object') {
            byUser = String(encryptedData.by || byUser).toLowerCase();
            cipher = String(encryptedData.cipher || '');
        }
        if (!byUser || !cipher) throw new Error("Ключ группы поврежден");
        const byPubResp = await fetch(`/api/user_pubkey/${encodeURIComponent(byUser)}`);
        const byData = await byPubResp.json();
        const byPubKey = await cryptoMod.importPublicKey(byData.public_key);

        let rawKeyB64 = '';
        try {
            rawKeyB64 = await cryptoMod.decryptGroupKey(myKeys.privateKey, byPubKey, cipher);
            return await cryptoMod.importAESKey(rawKeyB64);
        } catch {
            // Legacy fallback: in old builds group key could be stored as raw AES key string.
            return await cryptoMod.importAESKey(String(cipher || ''));
        }
    }

    // Если это ОБЫЧНЫЙ пользователь
    const resp = await fetch(`/api/user_pubkey/${targetId}`);
    const data = await resp.json();
    if (!data.public_key) throw new Error("У пользователя нет ключа");

    const peerPubKey = await cryptoMod.importPublicKey(data.public_key);
    return await cryptoMod.deriveAES(myKeys.privateKey, peerPubKey);
}

async function getRawGroupKeyForMember(groupId) {
    if (!groupId || !String(groupId).startsWith('group_')) return null;
    const giResp = await fetch(`/api/group_info/${encodeURIComponent(groupId)}`);
    const gi = await giResp.json();
    const owner = String(gi?.owner || '').trim().toLowerCase();
    if (!owner) return null;
    const myEnc = gi?.encrypted_keys?.[username];
    if (!myEnc) return null;
    const myKeys = await getMyPersistentKeys();
    if (!myKeys?.privateKey) return null;
    let byUser = owner;
    let cipher = myEnc;
    if (myEnc && typeof myEnc === 'object') {
        byUser = String(myEnc.by || owner).toLowerCase();
        cipher = String(myEnc.cipher || '');
    }
    if (!byUser || !cipher) return null;
    const byPubResp = await fetch(`/api/user_pubkey/${encodeURIComponent(byUser)}`);
    const byData = await byPubResp.json();
    if (!byData?.public_key) return null;
    const byPubKey = await cryptoMod.importPublicKey(byData.public_key);
    return await cryptoMod.decryptGroupKey(myKeys.privateKey, byPubKey, cipher);
}

async function tryGrantGroupKeyToMember(groupId, targetUser) {
    const target = String(targetUser || '').trim().toLowerCase();
    if (!groupId || !target || target === String(username || '').toLowerCase()) return false;
    try {
        const rawGroupKeyB64 = await getRawGroupKeyForMember(groupId);
        if (!rawGroupKeyB64) return false;
        const myKeys = await getMyPersistentKeys();
        if (!myKeys?.privateKey) return false;
        const pubResp = await fetch(`/api/user_pubkey/${encodeURIComponent(target)}`);
        const pubData = await pubResp.json();
        if (!pubData?.public_key) return false;
        const encryptedForTarget = await cryptoMod.encryptGroupKey(pubData.public_key, rawGroupKeyB64, myKeys.privateKey);
        const res = await fetch('/api/group_member_key/set', {
            method: 'POST',
            headers: authJsonHeaders(),
            body: JSON.stringify({
                group_id: groupId,
                username: String(username || '').toLowerCase(),
                target,
                encrypted_key: {
                    by: String(username || '').toLowerCase(),
                    cipher: encryptedForTarget,
                    v: 2
                }
            })
        });
        return !!res.ok;
    } catch {
        return false;
    }
}

async function sha256(text) {
    const buffer = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
    return Array.from(new Uint8Array(buffer)).map(b => b.toString(16).padStart(2, "0")).join("");
}

async function passwordToAESKey(password) {
    const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(password));
    return await crypto.subtle.importKey("raw", hash, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

/**
 * ==========================================
 * 6. ИНТЕРФЕЙС, ТОСТЫ
 * ==========================================
 */
let _ringToastEl = null;
let _appLoaderHideTimer = null;

function setAppLoaderText(text) {
    try {
        const el = document.getElementById('appLoaderText');
        if (el && text) el.textContent = String(text);
    } catch {}
}

function showAppLoader(text = "") {
    try {
        if (text) setAppLoaderText(text);
        const loader = document.getElementById('appLoader');
        if (!loader) return;
        loader.classList.remove('hidden');
    } catch {}
}

function hideAppLoader(delayMs = 120) {
    try {
        if (_appLoaderHideTimer) clearTimeout(_appLoaderHideTimer);
        _appLoaderHideTimer = setTimeout(() => {
            const loader = document.getElementById('appLoader');
            if (!loader) return;
            loader.classList.add('hidden');
        }, Math.max(0, Number(delayMs) || 0));
    } catch {}
}

// Safety net: never keep loader forever even if an init branch crashes.
setTimeout(() => {
    try { hideAppLoader(0); } catch {}
}, 8500);

function getToastIcon(type) {
    if (type === 'error') return '⚠';
    if (type === 'call') return '📞';
    return '✓';
}

function showToast(message, type = "success", opts = {}) {
    const container = document.getElementById("toast-container");
    if (!container) return;
    const toast = document.createElement("div");
    const normalized = (type === "error" || type === "info" || type === 'call') ? type : "success";
    toast.className = `toast ${normalized}`;
    if (opts.shake) toast.classList.add('shake');
    const closeBtn = '<button class="toast-close" aria-label="Закрыть">✕</button>';
    toast.innerHTML = `
      <div class="toast-icon">${getToastIcon(normalized)}</div>
      <div class="toast-text">${String(message || '')}</div>
      ${closeBtn}
    `;
    toast.querySelector('.toast-close')?.addEventListener('click', () => toast.remove());
    container.appendChild(toast);
    if (opts.persistent) return toast;
    setTimeout(() => {
        toast.style.opacity = "0";
        toast.style.transform = "translateY(-10px)";
        setTimeout(() => toast.remove(), 500);
    }, SETTINGS.TOAST_DURATION);
    return toast;
}
window.showToast = showToast;

window.showCallToast = function(message, mode = 'outgoing') {
    if (_ringToastEl) _ringToastEl.remove();
    _ringToastEl = showToast(message, 'call', { persistent: true, shake: true });
    if (_ringToastEl && mode === 'incoming') {
        _ringToastEl.classList.add('incoming');
    }
};

window.hideCallToast = function() {
    if (_ringToastEl) {
        _ringToastEl.remove();
        _ringToastEl = null;
    }
};

let _desktopNotifyPermAsked = false;
window.pushDesktopNotification = function(title, body = "", options = {}) {
    try {
        const appInBackground = !!(document.hidden || !document.hasFocus());
        if (!appInBackground) return;

        const notifTitle = String(title || 'Levart');
        const notifBody = String(body || '');
        const notifTag = String(options.tag || `levart_${Date.now()}`);

        // Native bridge: Windows (pywebview)
        try {
            if (window.pywebview?.api?.notify) {
                window.pywebview.api.notify(notifTitle, notifBody, notifTag);
            }
        } catch {}

        // Native bridge: Android WebView app
        try {
            if (window.LevartAndroid?.notify) {
                window.LevartAndroid.notify(notifTitle, notifBody, notifTag);
            }
        } catch {}

        // Native bridge: Electron desktop
        try {
            if (window.LevartDesktop?.notify) {
                window.LevartDesktop.notify(notifTitle, notifBody, notifTag);
            }
        } catch {}

        // Browser fallback notification
        if (typeof Notification !== 'undefined' && Notification.permission === 'granted') {
            const n = new Notification(notifTitle, {
                body: notifBody,
                tag: notifTag,
                icon: '/img/lavert_logo.png',
                badge: '/img/lavert_logo.png',
                silent: !!options.silent,
                renotify: true
            });
            n.onclick = () => {
                window.focus();
                n.close();
            };
        }
    } catch {}
};

function ensureDesktopNotificationsPermission() {
    try {
        if (_desktopNotifyPermAsked) return;
        _desktopNotifyPermAsked = true;
        if (typeof Notification === 'undefined') return;
        if (Notification.permission === 'default') {
            Notification.requestPermission().catch(() => {});
        }
    } catch {}
}

/**
 * ==========================================
 * 7. АВТОРИЗАЦИЯ (LOGIN / REGISTER)
 * ==========================================
 */
window.register = async () => {
    if (window.__levartAuthBusy) return;
    window.__levartAuthBusy = true;
    const firstNameEl = document.getElementById("firstName");
    const lastNameEl = document.getElementById("lastName");
    const usernameEl = document.getElementById("regUser");
    const passwordEl = document.getElementById("regPass");
    
    if (!firstNameEl || !lastNameEl || !usernameEl || !passwordEl) return;
    
    const firstName = firstNameEl.value.trim();
    const lastName = lastNameEl.value.trim();
    const u = usernameEl.value.trim().toLowerCase();
    const p = passwordEl.value.trim();
    
    if (!firstName || !lastName || !u || !p) {
        showToast(UI_TEXT.FILL_FIELDS, "error");
        return;
    }
    showAppLoader("Создаем аккаунт...");
    
    try {
        const keyPair = await cryptoMod.generateECDHKeyPair();
        const pubKeyB64 = await cryptoMod.exportECDHPublicKey(keyPair.publicKey);
        const privKeyJWK = await cryptoMod.exportECDHPrivateKey(keyPair.privateKey);
        const hashedPassword = await cryptoMod.hashPassword(p);
        
        // Шифруем приватный ключ паролем для локального хранения
        const encryptedPrivateKey = await cryptoMod.encryptWithPassword(p, JSON.stringify(privKeyJWK));
        
        await saveKeyToDB(u, encryptedPrivateKey);
        
        // Шифруем имя и фамилию паролем перед отправкой на сервер
        // const encryptedFirstName = await cryptoMod.encryptWithPassword(p, firstName);
        // const encryptedLastName = await cryptoMod.encryptWithPassword(p, lastName);

        const encryptedFirstName = firstName;
        const encryptedLastName = lastName;
        
        const res = await fetch("/register", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                username: u,
                password: hashedPassword,
                public_key: pubKeyB64,
                first_name: encryptedFirstName,
                last_name: encryptedLastName
            })
        });
        
        const result = await res.json().catch(() => ({}));
        
        if (res.ok) {
            // Экспортируем приватный ключ для постоянной сессии браузера
            const privRaw = await crypto.subtle.exportKey("pkcs8", keyPair.privateKey);
            const privBytes = new Uint8Array(privRaw);
            let privStr = "";
            for (let i = 0; i < privBytes.length; i++) privStr += String.fromCharCode(privBytes[i]);
            const privBase64 = btoa(privStr);
            
            const keyObj = JSON.stringify({ pub: pubKeyB64, priv: privBase64 });
            persistAuthSession(u, keyObj, p);
            if (result.auth_token) setAuthToken(u, result.auth_token);
            
            showToast(UI_TEXT.REG_SUCCESS);
            setTimeout(() => window.location.replace(ROUTES.MAIN), SETTINGS.REDIRECTION_DELAY);
        } else {
            showToast(result?.error || "Ошибка регистрации", "error");
            hideAppLoader(80);
        }
    } catch (err) {
        console.error("Registration error:", err);
        showToast("Ошибка регистрации или сохранения ключа", "error");
        hideAppLoader(80);
    } finally {
        window.__levartAuthBusy = false;
    }
};

window.login = async () => {
    if (window.__levartAuthBusy) return;
    window.__levartAuthBusy = true;
    const usernameEl = document.getElementById("loginUser");
    const passwordEl = document.getElementById("loginPass");
    
    if (!usernameEl || !passwordEl) return;
    
    const u = usernameEl.value.trim().toLowerCase();
    const p = passwordEl.value.trim();
    
    if (!u || !p) {
        showToast(UI_TEXT.FILL_FIELDS, "error");
        return;
    }
    showAppLoader("Выполняем вход...");
    
    try {
        const hashedPassword = await cryptoMod.hashPassword(p);
        
        const res = await fetch("/login", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username: u, password: hashedPassword })
        });
        
        const result = await res.json().catch(() => ({}));
        
        if (res.ok) {
            let encryptedPrivKey = await getKeyFromDB(u);
            
            if (!encryptedPrivKey) {
                const backupRes = await fetch(`/check_backup/${u}`);
                const backupData = await backupRes.json();
                
                if (backupData.has_backup && backupData.key) {
                    encryptedPrivKey = backupData.key;
                    await saveKeyToDB(u, encryptedPrivKey);
                } else {
                    showToast(UI_TEXT.CRYPTO_ERR_NO_LOCAL, "error");
                    return;
                }
            }
            
            try {
                const decryptedJWK = await cryptoMod.decryptWithPassword(p, encryptedPrivKey);
                const privKeyObj = JSON.parse(decryptedJWK);
                
                // Импортируем приватный ключ
                const privateKey = await window.crypto.subtle.importKey(
                    "jwk", 
                    privKeyObj, 
                    { name: "ECDH", namedCurve: "P-256" }, 
                    true, 
                    ["deriveKey"]
                );
                
                // Экспортируем приватный ключ в формат для локальной сессии
                const privRaw = await crypto.subtle.exportKey("pkcs8", privateKey);
                const privBytes = new Uint8Array(privRaw);
                let privStr = "";
                for (let i = 0; i < privBytes.length; i++) privStr += String.fromCharCode(privBytes[i]);
                const privBase64 = btoa(privStr);
                
                // Публичный ключ берем с сервера
                const pub = result.public_key;
                
                const keyObj = JSON.stringify({ pub, priv: privBase64 });
                persistAuthSession(u, keyObj, p);
                if (result.auth_token) setAuthToken(u, result.auth_token);
                showToast(UI_TEXT.LOGIN_SUCCESS);
                setTimeout(() => window.location.replace(ROUTES.MAIN), SETTINGS.REDIRECTION_DELAY);
            } catch (cryptoErr) {
                console.error("Key decrypt error:", cryptoErr);
                showToast(UI_TEXT.CRYPTO_ERR_DECRYPT, "error");
                hideAppLoader(80);
            }
        } else {
            showToast(result.error, "error");
            hideAppLoader(80);
        }
    } catch (err) {
        console.error("Login error:", err);
        showToast("Ошибка входа", "error");
        hideAppLoader(80);
    } finally {
        window.__levartAuthBusy = false;
    }
};

/**
 * ==========================================
 * 8. КОНТАКТЫ И ПОИСК (ВОССТАНОВЛЕНО)
 * ==========================================
 */
async function loadMyFriends() {
    const res = await fetch(`/get_friends?username=${username}`);
    myFriendsList = await res.json(); // сохраняем ники друзей: ["user1", "user2"]
    
    const container = document.getElementById("contacts");
    if (container) container.innerHTML = "";
    
    myFriendsList.forEach(friendName => {
        renderUserInList({ username: friendName, is_friend: true });
    });
}

// window.syncMyContacts = async () => {
//     let curUser = window.username || localStorage.getItem("chat_username");
//     if (!curUser || curUser === "undefined") {
//         const meEl = document.getElementById("me");
//         curUser = meEl?.innerText?.trim();
//     }
//     if (!curUser || curUser === "undefined") return;

//     const list = document.getElementById("contacts");
//     if (!list) return;

//     try {
//         const response = await fetch(`/api/my_contacts/${curUser}?t=${Date.now()}`);
//         const contacts = await response.json();
//         list.innerHTML = "";

//         if (!contacts || contacts.length === 0) {
//             list.innerHTML = "<div style='text-align:center;opacity:0.5;padding:20px;'>Чатов нет</div>";
//             return;
//         }

//         contacts.forEach(c => {
//             const div = document.createElement("div");
//             div.className = "contact-item";
//             div.setAttribute("data-peer", c.username);

//             // Аватар
//             const avatarDiv = document.createElement("div");
//             avatarDiv.className = "contact-avatar";
//             let displayName = c.display_name || c.username;
//             if (c.first_name || c.last_name) {
//                 displayName = `${c.first_name || ''} ${c.last_name || ''}`.trim();
//             }

//             if (c.avatar) {
//                 const img = document.createElement("img");
//                 img.src = c.avatar;
//                 avatarDiv.appendChild(img);
//             } else {
//                 avatarDiv.textContent = (c.is_group ? "👥" : displayName.charAt(0).toUpperCase());
//             }

//             // Информация
//             const infoDiv = document.createElement("div");
//             infoDiv.className = "contact-info";

//             const nameEl = document.createElement("div");
//             nameEl.className = "contact-name";
//             nameEl.textContent = displayName;
//             if (c.is_group) nameEl.innerHTML += ' <span style="font-size:11px;color:var(--accent);">группа</span>';

//             const lastMsgEl = document.createElement("div");
//             lastMsgEl.className = "contact-last-msg";
//             lastMsgEl.textContent = c.last_message_preview || "Нет сообщений";

//             infoDiv.appendChild(nameEl);
//             infoDiv.appendChild(lastMsgEl);

//             // Мета (время + непрочитанные)
//             const metaDiv = document.createElement("div");
//             metaDiv.className = "contact-meta";

//             if (c.last_time > 0) {
//                 const timeEl = document.createElement("div");
//                 timeEl.className = "contact-time";
//                 const d = new Date(c.last_time < 1e12 ? c.last_time * 1000 : c.last_time);
//                 const now = new Date();
//                 if (d.toDateString() === now.toDateString()) {
//                     timeEl.textContent = d.toLocaleTimeString('ru', { hour: '2-digit', minute: '2-digit' });
//                 } else {
//                     timeEl.textContent = d.toLocaleDateString('ru', { day: 'numeric', month: 'short' });
//                 }
//                 metaDiv.appendChild(timeEl);
//             }

//             if (c.unread_count > 0) {
//                 const badge = document.createElement("div");
//                 badge.className = "unread-badge";
//                 badge.textContent = c.unread_count > 99 ? "99+" : c.unread_count;
//                 metaDiv.appendChild(badge);
//             }

//             div.appendChild(avatarDiv);
//             div.appendChild(infoDiv);
//             div.appendChild(metaDiv);
//             // Кнопка троеточия
//             const dotsBtn = document.createElement("button");
//             dotsBtn.className = "contact-dots-btn";
//             dotsBtn.textContent = "⋮";
//             dotsBtn.title = "Настройки";
//             dotsBtn.onclick = (e) => {
//                 e.stopPropagation();
//                 showContactDropdown(e, c, div);
//             };
//             div.appendChild(dotsBtn);
//             div.onclick = (e) => {
//                 if (!e.target.classList.contains('contact-dots-btn')) {
//                     window.openChat(c.username);
//                     // Убираем активный класс у всех
//                     document.querySelectorAll('.contact-item').forEach(i => i.classList.remove('active'));
//                     div.classList.add('active');
//                 }
//             };
//             list.appendChild(div);
//         });
//     } catch (e) {
//         console.error("Ошибка syncMyContacts:", e);
//     }
// };



function renderUserInList(userData, isSearch = false) {
    const listId = isSearch ? "searchResults" : "contacts";
    const list = document.getElementById(listId);
    if (!list) return;

    const userId = typeof userData === 'object' ? userData.username : userData;
    const isGroup = userData.is_group || false;
    const displayName = userData.display_name || userId;
    
    // ПРОВЕРКА: Если ник есть в нашем списке друзей ИЛИ флаг пришел явно
    const isFriend = myFriendsList.includes(userId) || userData.is_friend === true;

    const div = document.createElement("div");
    div.className = "user-item";
    
    let prefixHtml = "";
    if (!isGroup) {
        const color = isFriend ? "#10b981" : "#f59e0b"; // Зеленый для друга, желтый для анонима
        const label = isFriend ? "[друг]" : "[аноним]";
        prefixHtml = `<span style="color: ${color}; font-weight: bold; margin-right: 5px; font-size: 0.8em;">${label}</span>`;
    }

    div.innerHTML = `
        <div class="user-item-inner" style="display: flex; align-items: center; width: 100%; gap: 10px;">
            <div class="user-avatar">${isGroup ? '👥' : '👤'}</div>
            <div class="user-info" style="flex: 1; overflow: hidden;">
                <div class="user-name" style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">
                    ${prefixHtml}${displayName}
                </div>
                <div style="font-size: 10px; opacity: 0.5;">${isGroup ? 'Групповой чат' : 'Личный чат'}</div>
            </div>
            <button class="chat-options-btn">⋮</button>
        </div>
    `;

    div.onclick = () => window.openChat(userId);
    list.appendChild(div);
}

window.addFriend = async (friendName) => {
    try {
        const res = await fetch("/add_friend", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ me: username, friend: friendName })
        });
        
        const data = await res.json();
        
        if (data.success) {
            showToast("Контакт добавлен!");
            // Ждем обновления
            await loadMyFriends(); 
            // Если есть функция глобальной синхронизации:
            if (typeof syncMyContacts === 'function') await syncMyContacts();
            
            document.getElementById("search-results").innerHTML = "";
            document.getElementById("searchUser").value = "";
        } else {
            showToast("Ошибка: " + data.error, "error");
        }
    } catch (e) {
        showToast("Ошибка сети", "error");
    }
};

let _searchRequestSeq = 0;
let _searchAbortCtrl = null;
let _searchRetryTimer = null;

window.search = async () => {
    const input = document.getElementById("searchUser");
    const results = document.getElementById("search-results");
    if (!input || !results) return;
    const query = input.value.trim();
    const isPhone = window.matchMedia && window.matchMedia('(max-width: 900px)').matches;

    const applyMobileSearchOverlay = () => {
        if (!isPhone || !results || !input) return;
        const r = input.getBoundingClientRect();
        const left = Math.max(6, Math.round(r.left));
        const right = Math.max(6, Math.round(window.innerWidth - r.right));
        const top = Math.round(r.bottom + 6);
        const bottomPad = 86;
        results.style.position = 'fixed';
        results.style.left = `${left}px`;
        results.style.right = `${right}px`;
        results.style.top = `${top}px`;
        results.style.bottom = `${bottomPad}px`;
        results.style.overflowY = 'auto';
        results.style.overflowX = 'hidden';
        results.style.zIndex = '12150';
    };
    const clearMobileSearchOverlay = () => {
        if (!results) return;
        ['position', 'left', 'right', 'top', 'bottom', 'overflowY', 'overflowX', 'zIndex'].forEach((k) => {
            results.style[k] = '';
        });
    };

    if (!query) {
        if (_searchAbortCtrl) {
            try { _searchAbortCtrl.abort(); } catch {}
            _searchAbortCtrl = null;
        }
        if (_searchRetryTimer) {
            clearTimeout(_searchRetryTimer);
            _searchRetryTimer = null;
        }
        results.innerHTML = "";
        clearMobileSearchOverlay();
        clearSearchMode();
        return;
    }
    activateSearchMode();
    if (isPhone) applyMobileSearchOverlay();

    const reqSeq = ++_searchRequestSeq;
    if (_searchAbortCtrl) {
        try { _searchAbortCtrl.abort(); } catch {}
    }
    _searchAbortCtrl = new AbortController();
    const loadingNodeId = 'search-loading-hint';
    let loadingNode = document.getElementById(loadingNodeId);
    if (!loadingNode) {
        loadingNode = document.createElement('div');
        loadingNode.id = loadingNodeId;
        loadingNode.style.padding = '10px 14px';
        loadingNode.style.color = 'var(--text-dim)';
        loadingNode.style.fontSize = '13px';
        loadingNode.textContent = 'Поиск...';
    }
    if (!results.children.length) {
        results.appendChild(loadingNode);
    }

    try {
        const tm = setTimeout(() => {
            try { _searchAbortCtrl?.abort(); } catch {}
        }, 25000);
        const res = await fetch(`/search?q=${encodeURIComponent(query)}&me=${username}`, { signal: _searchAbortCtrl.signal });
        clearTimeout(tm);
        if (reqSeq !== _searchRequestSeq) return;
        const found = await res.json();
        results.innerHTML = "";

        if (found.length === 0) {
            results.innerHTML = '<div style="padding: 20px; text-align: center; color: var(--text-dim);">Ничего не найдено</div>';
            return;
        }

        // Разделяем на знакомых и остальных
        const friends = found.filter(r => r.is_friend || r.has_chatted).slice(0, 10);
        const others = found.filter(r => !r.is_friend && !r.has_chatted).slice(0, 10);

        // Показываем знакомых
        if (friends.length > 0) {
            const friendsHeader = document.createElement('div');
            friendsHeader.className = 'search-category-header';
            friendsHeader.textContent = 'Ваши знакомые';
            results.appendChild(friendsHeader);

            friends.forEach(item => renderSearchResult(item, results));
        }

        // Показываем остальных
        if (others.length > 0) {
            const othersHeader = document.createElement('div');
            othersHeader.className = 'search-category-header';
            othersHeader.textContent = 'Остальные';
            results.appendChild(othersHeader);

            others.forEach(item => renderSearchResult(item, results));
        }
        if (_searchRetryTimer) {
            clearTimeout(_searchRetryTimer);
            _searchRetryTimer = null;
        }
    } catch (e) {
        if (reqSeq !== _searchRequestSeq) return;
        console.error("Ошибка поиска:", e);
        const errId = 'search-error-hint';
        const oldErr = document.getElementById(errId);
        if (oldErr) oldErr.remove();
        const err = document.createElement('div');
        err.id = errId;
        err.style.padding = '10px 14px';
        err.style.color = 'var(--danger, #ff6b6b)';
        err.style.fontSize = '12px';
        err.textContent = 'Слабое соединение. Продолжаем загрузку...';
        results.appendChild(err);
        if (_searchRetryTimer) clearTimeout(_searchRetryTimer);
        _searchRetryTimer = setTimeout(() => {
            if (String(input.value || '').trim() === query) {
                window.search();
            }
        }, 1200);
    }
};

// Новая функция рендера результата поиска
function renderSearchResult(item, container) {
    const name = item.username || item;
    if (name === username) return;
    
    const div = document.createElement("div");
    div.className = "search-result-item";
    
    const isGroup = !!item.is_group;
    
    if (isGroup) {
        // Группа
        const displayName = item.display_name || name;
        div.innerHTML = `
            <div class="search-avatar">👥</div>
            <div class="search-info">
                <div class="search-name">${displayName}</div>
                <div class="search-username">Группа</div>
            </div>
        `;
        div.onclick = () => {
            window.openChat(name);
            document.getElementById("searchUser").value = "";
            container.innerHTML = "";
            clearSearchMode();
        };
    } else {
        // Пользователь
        const displayName = getPreferredDisplayName(item, name);
        
        const isFriend = item.is_friend || false;
        // Аватарка: реальное фото или первая буква
        const avatarContent = item.avatar
            ? `<img src="${item.avatar}" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">`
            : `<span>${displayName.charAt(0).toUpperCase()}</span>`;
        
        div.innerHTML = `
            <div class="search-avatar" style="overflow:hidden;">${avatarContent}</div>
            <div class="search-info">
                <div class="search-name">${displayName}</div>
                <div class="search-username">@${name}</div>
            </div>
            ${isFriend ? '<div class="friend-badge">✓ Друг</div>' : ''}
        `;
        
        // Кнопка добавления если не друг
        if (!isFriend) {
            const addBtn = document.createElement("button");
            addBtn.textContent = "+";
            addBtn.className = "add-friend-btn-small";
            addBtn.onclick = async (e) => {
                e.stopPropagation();
                await addFriend(name);
                // Обновляем список
                window.search();
            };
            div.appendChild(addBtn);
        }
        
        div.onclick = () => {
            // if (!isFriend) return; // Нельзя писать не другу
            window.openChat(name);
            document.getElementById("searchUser").value = "";
            container.innerHTML = "";
            clearSearchMode();
        };
    }
    
    container.appendChild(div);
}

/**
 * ==========================================
 * 9. ЛОГИКА ЧАТА (E2EE + HISTORY)
 * ==========================================
 */
window.openChat = async (targetId) => {
    closeEmojiPanel();
    const chatHeader = document.getElementById("chatHeader");
    const messagesContainer = document.getElementById("messages");
    const inputArea = document.getElementById("input-area");
    const searchResults = document.getElementById('search-results');
    if (searchResults) {
        searchResults.innerHTML = '';
        ['position', 'left', 'right', 'top', 'bottom', 'overflowY', 'overflowX', 'zIndex'].forEach((k) => {
            searchResults.style[k] = '';
        });
    }
    clearSearchMode();
    toggleStoriesFocusMode(false);

    try {
        window.currentChat = targetId;
        if (inputArea) inputArea.style.display = "flex";

        // Мгновенный фоллбек-хедер: не блокируем открытие чата сетевыми запросами.
        if (chatHeader) {
            const avatarEl = document.getElementById('chatHeaderAvatar');
            const headerName = document.getElementById('chatHeaderName');
            const headerSub = document.getElementById('chatHeaderSub');
            const headerHint = document.getElementById('chatHeaderHint');
            if (targetId.startsWith("group_")) {
                if (avatarEl) {
                    avatarEl.style.backgroundImage = '';
                    avatarEl.textContent = "👥";
                }
                if (headerName) headerName.textContent = "Группа";
                if (headerSub) headerSub.textContent = targetId;
                if (headerHint) headerHint.textContent = "нажмите для информации →";
                chatHeader.onclick = () => openGroupPanel(targetId);
            } else {
                if (avatarEl) {
                    avatarEl.style.backgroundImage = '';
                    avatarEl.textContent = String(targetId || '?').charAt(0).toUpperCase();
                }
                if (headerName) headerName.textContent = targetId;
                if (headerSub) headerSub.textContent = `@${targetId}`;
                if (headerHint) headerHint.textContent = "нажмите для профиля →";
                chatHeader.onclick = () => openUserInfo(targetId);
            }
        }

        // Убираем divider предыдущего чата
        document.getElementById('unreadDivider')?.remove();

        // Убираем бейдж у открываемого чата (не предыдущего!)
        document.querySelector(`.contact-item[data-peer="${targetId}"] .unread-badge`)?.remove();
        
        if (messagesContainer) messagesContainer.innerHTML = "";
        // Подгружаем ключ и историю в фоне, чтобы UI не зависал.
        (async () => {
            try {
                if (!sessionAESKeys[targetId]) {
                    const aesKey = await getAESKeyForPeer(targetId);
                    sessionAESKeys[targetId] = aesKey;
                }
                currentAES = sessionAESKeys[targetId];
            } catch (e) {
                currentAES = null;
                console.warn("AES init failed:", e);
            }
            if (typeof loadHistory === "function") {
                try { await loadHistory(targetId); } catch {}
            }
        })();

        // Обновление точного хедера тоже в фоне.
        (async () => {
            if (!chatHeader) return;
            try {
                if (targetId.startsWith("group_")) {
                    const gResp = await fetch(`/api/group_info/${targetId}`);
                    const gData = await gResp.json();
                    const gAvatar = gData.avatar || "";
                    const gName = gData.name || "Группа";
                    const avatarEl = document.getElementById('chatHeaderAvatar');
                    if (avatarEl) {
                        if (gAvatar) {
                            avatarEl.style.backgroundImage = `url('${gAvatar}')`;
                            avatarEl.style.backgroundSize = 'cover';
                            avatarEl.textContent = "";
                        } else {
                            avatarEl.style.backgroundImage = '';
                            avatarEl.textContent = "👥";
                        }
                    }
                    const nameEl = document.getElementById('chatHeaderName');
                    const subEl = document.getElementById('chatHeaderSub');
                    if (nameEl) nameEl.textContent = gName;
                    if (subEl) subEl.textContent = `${gData.members?.length || 0} участников`;
                } else {
                    const pRes = await fetch(`/api/user_profile/${targetId}?me=${username}`);
                    const pData = await pRes.json();
                    const fullName = `${pData.first_name || ''} ${pData.last_name || ''}`.trim() || targetId;
                    const avatarEl = document.getElementById('chatHeaderAvatar');
                    if (avatarEl) {
                        if (pData.avatar) {
                            avatarEl.style.backgroundImage = `url('${pData.avatar}')`;
                            avatarEl.style.backgroundSize = 'cover';
                            avatarEl.textContent = "";
                        } else {
                            avatarEl.style.backgroundImage = '';
                            avatarEl.textContent = fullName.charAt(0).toUpperCase();
                        }
                    }
                    const nameEl = document.getElementById('chatHeaderName');
                    const subEl = document.getElementById('chatHeaderSub');
                    if (nameEl) nameEl.textContent = fullName;
                    if (subEl) subEl.textContent = `@${targetId}`;
                }
            } catch {}
        })();
        loadChatPins(targetId).catch(() => {});
        if (String(targetId).startsWith('group_')) {
            if (inputArea) inputArea.style.display = "flex";
            (async () => {
                try {
                    const canSend = await canSendByGroupPermission(targetId, 'can_send_messages');
                    if (inputArea) inputArea.style.display = canSend ? "flex" : "none";
                } catch {}
            })();
        } else {
            if (inputArea) inputArea.style.display = "flex";
        }

        // setTimeout(() => {
        //     scrollChatToBottom();
        // }, 200);
    } catch (err) {
        console.error("Ошибка открытия чата:", err);
        showToast("Ошибка: " + err.message, "error");
    }
    
};

window.onChatHeaderClick = () => {
    const chatHeader = document.getElementById('chatHeader');
    if (chatHeader.onclick) chatHeader.onclick();
};

// ─── Мобильная навигация ───────────────────────────────────────
const MOBILE_BREAKPOINT = 900;
function isMobile() { return window.innerWidth <= MOBILE_BREAKPOINT; }
let _baseViewportHeight = Math.max(window.innerHeight || 0, document.documentElement?.clientHeight || 0);

const TELEGRAM_MIC_ICON = `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 15a3 3 0 0 0 3-3V6a3 3 0 1 0-6 0v6a3 3 0 0 0 3 3zm5-3a1 1 0 0 1 2 0 7 7 0 0 1-6 6.92V22h3a1 1 0 1 1 0 2H8a1 1 0 1 1 0-2h3v-3.08A7 7 0 0 1 5 12a1 1 0 1 1 2 0 5 5 0 1 0 10 0z"/></svg>`;
const TELEGRAM_VIDEO_NOTE_ICON = `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3a9 9 0 1 1 0 18a9 9 0 0 1 0-18zm0 3.2a5.8 5.8 0 1 0 0 11.6a5.8 5.8 0 0 0 0-11.6z"/></svg>`;

function updateComposerButtons() {
    const input = document.getElementById('messageInput');
    const sendBtn = document.querySelector('.input-box .send-btn');
    const recordBtn = document.getElementById('voiceRecordBtn');
    const swapBtn = document.getElementById('videoRecordBtn');
    if (!input || !sendBtn || !recordBtn || !swapBtn) return;

    const hasText = !!String(input.value || '').trim();
    if (!isMobile()) {
        sendBtn.style.display = 'flex';
        recordBtn.style.display = 'inline-flex';
        swapBtn.style.display = 'inline-flex';
        recordBtn.innerHTML = TELEGRAM_MIC_ICON;
        swapBtn.innerHTML = TELEGRAM_VIDEO_NOTE_ICON;
        recordBtn.onclick = () => recordVoiceMessage();
        swapBtn.onclick = () => recordVideoNote();
        swapBtn.title = 'Записать кружок';
        return;
    }

    if (hasText) {
        sendBtn.style.display = 'flex';
        recordBtn.style.display = 'none';
        swapBtn.style.display = 'none';
        return;
    }

    sendBtn.style.display = 'none';
    recordBtn.style.display = 'inline-flex';
    swapBtn.style.display = 'inline-flex';
    recordBtn.innerHTML = TELEGRAM_MIC_ICON;
    recordBtn.title = 'Записать голосовое';
    recordBtn.onclick = () => recordVoiceMessage();
    swapBtn.innerHTML = TELEGRAM_VIDEO_NOTE_ICON;
    swapBtn.title = 'Записать кружок';
    swapBtn.onclick = () => recordVideoNote();
}

function initComposerButtons() {
    const input = document.getElementById('messageInput');
    if (!input) return;
    const autoResizeComposer = () => {
        input.style.height = 'auto';
        const h = Math.min(132, Math.max(36, input.scrollHeight));
        input.style.height = `${h}px`;
    };
    input.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') return;
        if (isMobile()) return; // на телефоне только кнопкой отправки
        if (e.shiftKey) {
            autoResizeComposer();
            return; // новая строка
        }
        e.preventDefault();
        sendMessage();
    });
    input.addEventListener('input', autoResizeComposer, { passive: true });
    ['input', 'change', 'focus', 'blur'].forEach((ev) => input.addEventListener(ev, updateComposerButtons, { passive: true }));
    autoResizeComposer();
    updateComposerButtons();
}

function updateAppViewportHeight() {
    const vv = window.visualViewport;
    const h = Math.max(320, Math.round(vv ? vv.height : window.innerHeight));
    document.documentElement.style.setProperty('--app-height', `${h}px`);
    const delta = vv ? Math.max(0, Math.round((window.innerHeight - vv.height - (vv.offsetTop || 0)))) : 0;
    const vkBottom = vv ? Math.max(0, Math.round(window.innerHeight - (vv.offsetTop + vv.height))) : 0;
    const vvVisible = vv ? Math.round(vv.height + (vv.offsetTop || 0)) : Math.round(window.innerHeight);
    _baseViewportHeight = Math.max(_baseViewportHeight, vvVisible, Math.round(window.innerHeight));
    const effectiveKeyboardOffset = Math.max(delta, vkBottom);
    document.documentElement.style.setProperty('--vk-offset', `${delta}px`);
    document.documentElement.style.setProperty('--input-kb-offset', `${Math.max(0, delta)}px`);
    document.documentElement.style.setProperty('--vk-bottom', `${effectiveKeyboardOffset}px`);
    const keyboardOpen = isMobile() && effectiveKeyboardOffset > 64;
    document.body.classList.toggle('keyboard-open', keyboardOpen);
    document.body.classList.remove('composer-keyboard-active');
}

function ensureFocusedFieldVisible(el) {
    if (!el || !isMobile()) return;
    const vv = window.visualViewport;
    const viewH = vv ? vv.height : window.innerHeight;
    const rect = el.getBoundingClientRect();
    const targetBottom = viewH - 10;
    const overlap = rect.bottom - targetBottom;
    if (overlap <= 0) return;
    const scroller = el.closest('.settings-content, .help-main, .tab, .card, .settings-tab, #emojiPanelContent, .chat-area, .sidebar, .story-editor-shell');
    const shift = overlap + 26;
    try {
        if (scroller && typeof scroller.scrollBy === 'function') {
            scroller.scrollBy({ top: shift, behavior: 'smooth' });
        } else {
            window.scrollBy({ top: shift, behavior: 'smooth' });
        }
    } catch {}
}

function initMobileViewportBehavior() {
    updateAppViewportHeight();
    window.addEventListener('resize', updateAppViewportHeight, { passive: true });
    if (window.visualViewport) {
        window.visualViewport.addEventListener('resize', updateAppViewportHeight, { passive: true });
        window.visualViewport.addEventListener('scroll', updateAppViewportHeight, { passive: true });
    }
    const input = document.getElementById('messageInput');
    if (input) {
        input.addEventListener('focus', () => {
            if (isMobile()) document.body.classList.add('composer-force-up');
            setTimeout(() => {
                updateAppViewportHeight();
                scrollChatToBottom();
            }, 120);
        }, { passive: true });
        input.addEventListener('blur', () => {
            setTimeout(() => {
                document.body.classList.remove('keyboard-open');
                document.documentElement.style.setProperty('--vk-offset', '0px');
                document.documentElement.style.setProperty('--vk-bottom', '0px');
                document.body.classList.remove('composer-force-up');
            }, 140);
        }, { passive: true });
    }

    // Для экранов входа/регистрации и прочих форм: держим активное поле над клавиатурой.
    const formInputs = Array.from(document.querySelectorAll('input, textarea')).filter((el) => {
        if (!el || el.id === 'messageInput') return false;
        if (el.type === 'hidden' || el.disabled) return false;
        return true;
    });
    formInputs.forEach((el) => {
        el.addEventListener('focus', () => {
            setTimeout(() => {
                updateAppViewportHeight();
                try {
                    el.scrollIntoView({ block: 'center', behavior: 'smooth' });
                } catch {}
                ensureFocusedFieldVisible(el);
            }, 140);
        }, { passive: true });
    });

    document.addEventListener('focusin', (e) => {
        const t = e.target;
        if (!(t instanceof HTMLElement)) return;
        const isField = t.matches('input, textarea, [contenteditable="true"]');
        if (!isField) return;
        setTimeout(() => {
            updateAppViewportHeight();
            ensureFocusedFieldVisible(t);
        }, 90);
        setTimeout(() => ensureFocusedFieldVisible(t), 220);
    }, { passive: true });

    if (window.visualViewport) {
        window.visualViewport.addEventListener('resize', () => {
            const active = document.activeElement;
            if (active instanceof HTMLElement && active.matches('input, textarea, [contenteditable="true"]')) {
                ensureFocusedFieldVisible(active);
            }
        }, { passive: true });
    }
}

function enforceMobileSearchLayout() {
    if (!isMobile()) return;
    const sidebar = document.querySelector('.sidebar');
    const searchWrap = document.querySelector('.sidebar .discover-search');
    const strip = document.querySelector('.sidebar .discover-strip');
    const input = document.getElementById('searchUser');
    if (sidebar) {
        sidebar.style.paddingTop = '2px';
    }
    if (searchWrap) {
        searchWrap.style.marginTop = '35px';
        searchWrap.style.marginBottom = '-35px';
        searchWrap.style.paddingTop = '0';
        searchWrap.style.paddingBottom = '0';
    }
    if (strip) {
        strip.style.gap = '2px';
        strip.style.paddingTop = '0';
        strip.style.paddingBottom = '0';
    }
    if (input) {
        input.style.height = '32px';
        input.style.minHeight = '32px';
        input.style.lineHeight = '32px';
        input.style.margin = '0';
    }
}

function initMobileEdgeBackGesture() {
    const chatArea = document.getElementById('chatArea');
    if (!chatArea) return;
    let startX = 0;
    let startY = 0;
    let tracking = false;
    chatArea.addEventListener('touchstart', (e) => {
        if (!isMobile() || !chatArea.classList.contains('active-mobile')) return;
        const t = e.touches?.[0];
        if (!t) return;
        startX = t.clientX;
        startY = t.clientY;
        tracking = startX <= 24;
    }, { passive: true });
    chatArea.addEventListener('touchend', (e) => {
        if (!tracking) return;
        tracking = false;
        const t = e.changedTouches?.[0];
        if (!t) return;
        const dx = t.clientX - startX;
        const dy = Math.abs(t.clientY - startY);
        if (dx > 70 && dy < 40) {
            window.goBackToChats();
        }
    }, { passive: true });
}

// Патчим openChat для мобайла
const _origOpenChat = window.openChat;
window.openChat = async function(targetId) {
    await _origOpenChat(targetId);
    if (isMobile()) {
        document.querySelector('.sidebar').classList.add('hidden-mobile');
        document.getElementById('folderBar')?.classList.add('hidden-mobile');
        document.getElementById('chatArea').classList.add('active-mobile');
        document.getElementById('backToChats').style.display = 'flex';
    }
    updateComposerButtons();
};

window.goBackToChats = function(e) {
    if (e?.stopPropagation) e.stopPropagation();
    closeEmojiPanel();
    document.querySelector('.sidebar').classList.remove('hidden-mobile');
    document.getElementById('folderBar')?.classList.remove('hidden-mobile');
    document.getElementById('chatArea').classList.remove('active-mobile');
    document.getElementById('backToChats').style.display = 'none';
    // Убираем активный класс у контактов
    document.querySelectorAll('.contact-item').forEach(i => i.classList.remove('active'));
    updateComposerButtons();
};

// При ресайзе до десктопа — сбросить состояние
window.addEventListener('resize', () => {
    if (window.innerWidth > MOBILE_BREAKPOINT) {
        document.querySelector('.sidebar')?.classList.remove('hidden-mobile');
        document.getElementById('folderBar')?.classList.remove('hidden-mobile');
        document.getElementById('chatArea')?.classList.remove('active-mobile');
        const btn = document.getElementById('backToChats');
        if (btn) btn.style.display = 'none';
    }
    updateComposerButtons();
});

async function createChatSession(targetUser) {
    try {
        // Получаем AES ключ для собеседника (используя ECDH)
        const aesKey = await getAESKeyForPeer(targetUser);
        // Сохраняем ключ в глобальный объект сессий, чтобы не пересчитывать его для каждого сообщения
        sessionAESKeys[targetUser] = aesKey;
        currentAES = aesKey; 
        return true;
    } catch (e) {
        console.error("Ошибка инициализации сессии:", e);
        showToast("Не удалось установить защищенное соединение", "error");
        return false;
    }
}

const _markReadThrottle = {};

async function markChatRead(peer) {
    if (!peer) return;
    const key = String(peer).toLowerCase();
    const now = Date.now();
    if (_markReadThrottle[key] && now - _markReadThrottle[key] < 800) return;
    _markReadThrottle[key] = now;

    try {
        await fetch('/api/mark_read', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ me: username, peer })
        });
    } catch {}

    const contactInMemory = (window._allContacts || []).find(c => c.username === peer);
    if (contactInMemory) {
        contactInMemory.unread_count = 0;
    }

    document.querySelector(`.contact-item[data-peer="${peer}"] .unread-badge`)?.remove();

    if (window._activeFolder === 'inbox') {
        const folder = getAllFolders().find(f => f.id === 'inbox');
        if (folder) renderContactsList(filterContactsByFolder(window._allContacts, folder));
    }

    if (socket) socket.emit("message_read", { reader: username, peer });
}

window.startChat = async (targetUser) => {
    console.log("Запуск чата с:", targetUser);
    
    // 1. Сначала подготавливаем сессию (ключи шифрования)
    // Если у вас уже есть функция createChatSession, используем её
    try {
        const success = await createChatSession(targetUser);
        if (!success) {
            showToast("Не удалось установить защищенное соединение", "error");
            return;
        }

        // 2. Вызываем основную функцию открытия чата (которая отрисовывает UI)
        if (typeof window.openChat === 'function') {
            window.openChat(targetUser);
        } else {
            // Если openChat не определен, реализуем логику переключения здесь
            window.currentChat = targetUser;
            const chatHeader = document.getElementById("chatWith");
            if (chatHeader) chatHeader.innerText = targetUser;
            
            // Очищаем и загружаем историю
            const msgContainer = document.getElementById("messages");
            if (msgContainer) msgContainer.innerHTML = "";
            
            loadHistory(targetUser);
        }
    } catch (err) {
        console.error("Ошибка при старте чата:", err);
        showToast("Ошибка при открытии чата", "error");
    }
};

async function loadHistory(friendName) {
    const messagesEl = document.getElementById('messages');

    // Шаг 1: узнаём timestamp последнего прочтения
    let lastReadTs = 0;
    try {
        const lr     = await fetch(`/api/last_read?me=${username}&peer=${friendName}`);
        const lrData = await lr.json();
        lastReadTs   = parseFloat(lrData.last_read) || 0;
    } catch {}

    // Шаг 2: грузим историю (с офлайн fallback)
    const fetched = await fetchHistoryWithOffline(friendName);
    const history = Array.isArray(fetched?.history) ? fetched.history : [];

    let dividerInserted = false;

    for (const packet of history) {
        // Если это сообщение от собеседника И оно новее last_read → вставляем разделитель
        if (
            !dividerInserted &&
            lastReadTs > 0 &&
            packet.from !== username
        ) {
            // ID формат: msg_{timestamp_ms} — делим на 1000 чтобы сравнивать с lastReadTs (секунды)
            let msgTs = 0;
            if (packet.id && packet.id.includes('_')) {
                try {
                    const raw = parseFloat(packet.id.split('_').pop());
                    // Значение > 1e11 означает миллисекунды — нормализуем в секунды
                    msgTs = raw > 1e11 ? raw / 1000 : raw;
                } catch {}
            }
            if (!msgTs) {
                msgTs = parseFloat(packet.timestamp || 0);
            }

            if (msgTs > lastReadTs) {
                // Вставляем разделитель ПЕРЕД этим сообщением
                dividerInserted = true;
                const divider   = document.createElement('div');
                divider.className = 'unread-divider';
                divider.id        = 'unreadDivider';
                divider.innerHTML = '<span class="unread-label">Непрочитанные</span>';
                messagesEl.appendChild(divider);
            }
        }

        await decryptAndAppend(packet);
    }

    // Шаг 3: помечаем как прочитанное и обновляем данные в памяти
    markChatRead(friendName).catch(() => {});

    // Шаг 4: скроллим к разделителю или вниз
    setTimeout(() => {
        const divider = document.getElementById('unreadDivider');
        if (divider) {
            divider.scrollIntoView({ block: 'center', behavior: 'smooth' });
        } else {
            scrollChatToBottom();
        }
    }, 150);

    refreshContactPreview(friendName);
}

function updateOnlineIndicator(peerUsername, isOnline, lastSeen) {
    // Обновляем точку в списке контактов
    const contactEl = document.querySelector(`.contact-item[data-peer="${peerUsername}"]`);
    if (contactEl) {
        const dot = contactEl.querySelector('.status-dot');
        if (dot) {
            dot.style.background = isOnline ? '#4ade80' : '#6b7280';
            dot.style.boxShadow = isOnline ? '0 0 4px #4ade80' : 'none';
        }
    }
    if (window.currentChat === peerUsername) {
        const subEl = document.getElementById('chatHeaderSub');
        if (subEl && !peerUsername.startsWith('group_')) {
            if (isOnline) {
                subEl.textContent = 'в сети';
                subEl.style.color = '#4ade80';
            } else {
                let txt = 'не в сети';
                if (lastSeen) {
                    const d = new Date(lastSeen * 1000);
                    const diff = Date.now() - d;
                    if (diff < 60000) txt = 'был(а) только что';
                    else if (diff < 3600000) txt = `был(а) ${Math.floor(diff/60000)} мин. назад`;
                    else txt = `был(а) в ${d.getHours().toString().padStart(2,'0')}:${d.getMinutes().toString().padStart(2,'0')}`;
                }
                subEl.textContent = txt;
                subEl.style.color = '';
            }
        }
    }
}

function setMessageStatus(msgId, status) {
    const msgEl = document.querySelector(`.msg[data-id="${msgId}"]`);
    if (!msgEl) return;
    let statusEl = msgEl.querySelector('.msg-status');
    if (!statusEl) {
        const meta = msgEl.querySelector('.msg-meta');
        if (meta) {
            statusEl = document.createElement('span');
            statusEl.className = 'msg-status';
            meta.appendChild(statusEl);
        }
    }
    if (statusEl) statusEl.className = `msg-status ${status}`;
}

function addPendingMessage(text, tempId) {
    const msgDiv = document.getElementById('messages');
    if (!msgDiv) return;
    const div = document.createElement('div');
    div.className = 'msg msg-me';
    div.setAttribute('data-id', tempId);
    div.setAttribute('data-pending-id', tempId);
    div.style.opacity = '0.6';
    const safeText = String(text).replace(/</g, '&lt;').replace(/>/g, '&gt;');
    div.innerHTML = `
        <span class="msg-info">Вы</span>
        <div class="msg-text">${safeText}</div>
        <div class="msg-meta">
            <span class="msg-time" style="display:flex;align-items:center;gap:4px;">
                <span style="width:12px;height:12px;border:2px solid #888;border-top-color:var(--accent);border-radius:50%;animation:spin 0.8s linear infinite;display:inline-block;"></span>
                Отправляется...
            </span>
        </div>`;
    msgDiv.appendChild(div);
    scrollChatToBottom();
    return div;
}

function addPendingFile(file, tempId) {
    const msgDiv = document.getElementById('messages');
    if (!msgDiv) return;
    const isImage = file.type.startsWith('image/');
    const div = document.createElement('div');
    div.className = 'msg msg-me';
    div.setAttribute('data-id', tempId);
    div.setAttribute('data-pending-id', tempId);
    div.style.opacity = '0.6';
    const safeFileName = file.name.replace(/</g, '&lt;');
    let preview = '';
    if (isImage) {
        preview = `<img src="${URL.createObjectURL(file)}" style="max-width:200px;max-height:160px;border-radius:10px;display:block;margin-bottom:6px;filter:blur(2px);">`;
    }
    div.innerHTML = `
        <span class="msg-info">Вы</span>
        <div style="min-width:160px;">
            ${preview}
            <div style="display:flex;align-items:center;gap:8px;">
                <span style="font-size:20px;">${isImage ? '🖼' : '📄'}</span>
                <span style="font-size:13px;">${safeFileName}</span>
            </div>
            <div style="width:100%;height:3px;background:rgba(255,255,255,0.1);border-radius:2px;margin-top:6px;">
                <div id="upfill_${tempId}" style="height:100%;background:var(--accent);border-radius:2px;width:0%;transition:width 0.3s;"></div>
            </div>
        </div>
        <div class="msg-meta">
            <span class="msg-time" style="display:flex;align-items:center;gap:4px;">
                <span style="width:12px;height:12px;border:2px solid #888;border-top-color:var(--accent);border-radius:50%;animation:spin 0.8s linear infinite;display:inline-block;"></span>
                Загружается...
            </span>
        </div>`;
    msgDiv.appendChild(div);
    scrollChatToBottom();
    return div;
}

function formatMediaTime(s) {
    const n = Number(s || 0);
    if (!Number.isFinite(n) || n < 0) return '0:00';
    const t = Math.max(0, Math.floor(n));
    const m = Math.floor(t / 60);
    const sec = t % 60;
    return `${m}:${String(sec).padStart(2, '0')}`;
}

function hardenVideoElementUi(videoEl) {
    if (!videoEl) return;
    videoEl.controls = false;
    videoEl.removeAttribute('controls');
    videoEl.disablePictureInPicture = true;
    videoEl.setAttribute('disablePictureInPicture', '');
    videoEl.setAttribute('disableRemotePlayback', '');
    videoEl.setAttribute('x-webkit-airplay', 'deny');
    videoEl.setAttribute('playsinline', '');
    videoEl.setAttribute('webkit-playsinline', '');
    videoEl.controlsList = 'nodownload noplaybackrate noremoteplayback nofullscreen';
    videoEl.oncontextmenu = (e) => e.preventDefault();
    // Не блокируем touchstart/touchend: иначе тап по кружку/видео не срабатывает на мобильных.
    videoEl.addEventListener('gesturestart', (e) => e.preventDefault(), { passive: false });
    try {
        const tracks = videoEl.textTracks || [];
        for (let i = 0; i < tracks.length; i++) {
            tracks[i].mode = 'disabled';
        }
    } catch {}
}

function addPendingVoiceMessage(tempId) {
    const msgDiv = document.getElementById('messages');
    if (!msgDiv) return null;
    const div = document.createElement('div');
    div.className = 'msg msg-me';
    div.setAttribute('data-id', tempId);
    div.setAttribute('data-pending-id', tempId);
    div.style.opacity = '0.72';
    div.innerHTML = `
      <span class="msg-info">Вы</span>
      <div class="msg-voice pending">
        <button class="voice-play-btn">🎙</button>
        <div class="voice-track">
          <div class="voice-wave-bars">${Array.from({ length: 18 }).map(() => '<span></span>').join('')}</div>
          <div class="voice-progress"><div class="voice-progress-fill" style="width:28%"></div></div>
        </div>
        <div class="voice-time">0:00</div>
      </div>
      <div class="msg-meta"><span class="msg-time">Отправка...</span></div>
    `;
    msgDiv.appendChild(div);
    scrollChatToBottom();
    return div;
}

function addPendingVideoNoteMessage(tempId) {
    const msgDiv = document.getElementById('messages');
    if (!msgDiv) return null;
    const div = document.createElement('div');
    div.className = 'msg msg-me';
    div.setAttribute('data-id', tempId);
    div.setAttribute('data-pending-id', tempId);
    div.style.opacity = '0.72';
    div.innerHTML = `
      <span class="msg-info">Вы</span>
      <div class="video-note-pending">⭕</div>
      <div class="msg-meta"><span class="msg-time">Отправка...</span></div>
    `;
    msgDiv.appendChild(div);
    scrollChatToBottom();
    return div;
}

// Обновляет только один контакт в DOM без полного перерисования
function refreshContactPreview(peer) {
    const chatId = peer.startsWith('group_') ? peer : [username, peer].sort().join('_');
    const previews = loadMsgPreviews();
    const preview  = previews[chatId];
    if (!preview) return;
    const contactEl = document.querySelector(`.contact-item[data-peer="${peer}"] .contact-last-msg`);
    if (contactEl) contactEl.textContent = preview;
}

function getPacketTsMs(packet) {
    if (packet?.timestamp) {
        const n = Number(packet.timestamp);
        if (Number.isFinite(n) && n > 0) return n > 1e12 ? n : n * 1000;
    }
    const pid = String(packet?.id || '');
    if (pid.includes('_')) {
        const raw = Number(pid.split('_').pop());
        if (Number.isFinite(raw) && raw > 0) return raw > 1e12 ? raw : raw * 1000;
    }
    return Date.now();
}

function getDayKeyAndLabel(tsMs) {
    const d = new Date(tsMs);
    const key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
    const today = new Date();
    const td = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
    const y = new Date(Date.now() - 86400000);
    const yd = `${y.getFullYear()}-${String(y.getMonth()+1).padStart(2,'0')}-${String(y.getDate()).padStart(2,'0')}`;
    if (key === td) return { key, label: 'Сегодня' };
    if (key === yd) return { key, label: 'Вчера' };
    return { key, label: d.toLocaleDateString('ru-RU', { weekday: 'short', day: 'numeric', month: 'long' }) };
}

function ensureDayDividerForPacket(packet) {
    const messagesEl = document.getElementById('messages');
    if (!messagesEl) return;
    const { key, label } = getDayKeyAndLabel(getPacketTsMs(packet));
    const allDays = messagesEl.querySelectorAll('.day-divider');
    const lastDay = allDays.length ? allDays[allDays.length - 1] : null;
    const lastKey = lastDay?.dataset?.dayKey || '';
    if (lastKey === key) return;
    const day = document.createElement('div');
    day.className = 'day-divider';
    day.dataset.dayKey = key;
    day.innerHTML = `<span>${label}</span>`;
    messagesEl.appendChild(day);
}


async function decryptAndAppend(packet) {
    ensureDayDividerForPacket(packet);
    if (packet.type === "call_event" || packet.type === "system_event") {
        const text = stripSystemPrefix(packet.text || "📞 Событие звонка");
        addMessageToScreen("system", text, packet.id, null, packet.time, false, true);
        return;
    }
    if (!packet.cipher) return;

    const isGroup = packet.to && packet.to.startsWith("group_");
    const chatId = isGroup ? packet.to : (packet.from === username ? packet.to : packet.from);
    if (!chatId) return;

    try {
        let aesKey = sessionAESKeys[chatId];
        if (!aesKey) {
            try {
                aesKey = await getAESKeyForPeer(chatId);
                sessionAESKeys[chatId] = aesKey;
            } catch (keyErr) {
                // Нет ключа — чужой чат, тихо пропускаем
                console.warn(`[decrypt] Нет ключа для "${chatId}"`, keyErr.message);
                return;
            }
        }

        let decryptedText;
        try {
            decryptedText = await cryptoMod.decrypt(aesKey, packet.cipher);
        } catch (decryptErr) {
            // Ключи не совпадают — тихо пропускаем, не показываем ошибку
            console.warn(`[decrypt] Не удалось расшифровать ${packet.id}`, decryptErr.message);
            return;
        }

        let contentToDisplay;
        if (decryptedText.startsWith("__FILE__")) {
            try {
                contentToDisplay = JSON.parse(decryptedText.replace("__FILE__", ""));
            } catch {
                console.warn(`[decrypt] Битые метаданные файла ${packet.id}`);
                return;
            }
        } else {
            contentToDisplay = decryptedText;
        }

        addMessageToScreen(
            packet.from,
            contentToDisplay,
            packet.id,
            packet.reply_to,
            packet.time,
            packet.edited,
            false,
            { forwardedFrom: packet.forwarded_from || '', reactions: packet.reactions || {} }
        );
        scrollChatToBottom();

    } catch (e) {
        console.error("[decrypt] Неожиданная ошибка:", e);
    }
}

window.sendMessage = async () => {
    const input = document.getElementById("messageInput");
    const text = input.value.trim();

    // Берем данные откуда угодно (локально или из window)
    const activeChat = window.currentChat || (typeof currentChat !== 'undefined' ? currentChat : null);
    const activeAES = window.currentAES || (typeof currentAES !== 'undefined' ? currentAES : null);
    const activeReplyId = window.replyId || (typeof replyId !== 'undefined' ? replyId : null);
    const activeEditId = window.editMsgId || (typeof editMsgId !== 'undefined' ? editMsgId : null);

    // Проверка
    if (!text) return;
    if (!activeChat || !activeAES) {
        console.error("Данные чата отсутствуют:", { activeChat, activeAES });
        showToast("Ошибка: чат не готов к отправке", "error");
        return;
    }
    if (String(activeChat).startsWith('group_')) {
        const canSendText = await canSendByGroupPermission(activeChat, 'can_send_messages');
        if (!canSendText) {
            showToast('В этой группе вам запрещено писать сообщения', 'error');
            return;
        }
        const hasLink = /(https?:\/\/|www\.)\S+/i.test(text);
        if (hasLink) {
            const canSendLinks = await canSendByGroupPermission(activeChat, 'can_send_links');
            if (!canSendLinks) {
                showToast('В этой группе вам запрещено отправлять ссылки', 'error');
                return;
            }
        }
    }

    // ОЧИСТКА
    input.value = "";
    window.replyId = null;
    updateComposerButtons();

    try {
        const encrypted = await cryptoMod.encrypt(activeAES, text);
        const msg_id = "msg_" + Date.now(); // Добавим префикс для надежности

        if (activeEditId) {
            socket.emit("edit_message", {
                id: activeEditId,
                chat_id: activeChat,
                cipher: encrypted
            });
            window.editMsgId = null; 
            if (typeof editMsgId !== 'undefined') editMsgId = null;
        } else {
            const packet = {
                client_id: msg_id,   // клиентский ID для удаления pending
                from: username,
                to: activeChat,
                cipher: encrypted,
                type: "text",
                has_link: /(https?:\/\/|www\.)\S+/i.test(text),
                ...(activeReplyId && { reply_to: activeReplyId }) 
            };
            
            addPendingMessage(text, msg_id);
            socket.emit("send_message", packet);
        }

        
        if (typeof replyId !== 'undefined') replyId = null;
        
        const preview = document.getElementById("reply-preview");
        if (preview) preview.style.display = "none";
        
    } catch (err) {
        console.error("Ошибка при отправке:", err);
        showToast("Ошибка шифрования");
    }
    syncMyContacts();
};

/**
 * ==========================================
 * 10. WEB-SOCKETS (REAL-TIME СИНХРОНИЗАЦИЯ)
 * ==========================================
 */
if (socket) {
    // Регистрируемся как онлайн
    socket.emit("user_online", { username });
    socket.on("connect", () => socket.emit("user_online", { username }));

    // Онлайн/оффлайн статусы
    socket.on("user_status", (data) => {
        const u = (data.username || "").toLowerCase();
        if (data.online) _onlineUsers.add(u);
        else _onlineUsers.delete(u);
        updateOnlineIndicator(u, data.online, data.last_seen);
    });

    // Подтверждение доставки от сервера
    socket.on("message_ack", (data) => {
        // Убираем pending по client_id
        if (data.client_id) {
            const pendingEl = document.querySelector(`[data-pending-id="${data.client_id}"]`);
            if (pendingEl) pendingEl.remove();
        }
        // Ставим галочку "доставлено" на реальное сообщение
        setMessageStatus(data.id, "delivered");
    });

    // Прочитано — data.reader прочитал сообщения от data.by_whom
    socket.on("chat_read", (data) => {
        // by_whom = тот кто написал (нас интересует когда by_whom === мы)
        // reader = тот кто прочитал (нам нужен текущий открытый чат)
        if (data.by_whom === username) {
            const readerLow = (data.reader || "").toLowerCase();
            if (window.currentChat && window.currentChat.toLowerCase() === readerLow) {
                document.querySelectorAll('.msg.msg-me .msg-status').forEach(el => {
                    el.className = 'msg-status read';
                });
            }
        }
    });
    socket.on("new_message", async (packet) => {
        const activeChat = window.currentChat || (typeof currentChat !== 'undefined' ? currentChat : null);
        
        const isGroup = packet.to && packet.to.startsWith("group_");
        const chatId = isGroup ? packet.to : (packet.from === username ? packet.to : packet.from);
        if (!chatId) return;
    
        if (chatId === activeChat) {
            // Удаляем pending-пузырь по client_id (если это наше сообщение)
            if (packet.client_id) {
                const pendingEl = document.querySelector(`[data-pending-id="${packet.client_id}"]`);
                if (pendingEl) pendingEl.remove();
            }
            await decryptAndAppend(packet);
            scrollChatToBottom();
            refreshContactPreview(chatId);
            if (packet.from !== username) {
                markChatRead(chatId).catch(() => {});
            }
        } else {
            if (packet.from !== username) {
                const fromSystem = String(packet.from || '').toLowerCase() === 'system';
                if (fromSystem) {
                    const txt = stripSystemPrefix(String(packet.text || '').trim());
                    if (txt) {
                        if (!isMobile()) showToast(txt);
                        window.pushDesktopNotification('Системное событие', txt, { tag: `sys_${chatId}` });
                    }
                } else {
                    if (!isMobile()) {
                        showToast("Новое сообщение" + (isGroup ? " в группе" : " от " + packet.from));
                    }
                    window.pushDesktopNotification(
                        isGroup ? 'Новое сообщение в группе' : `Сообщение от ${packet.from}`,
                        isGroup ? 'Откройте чат, чтобы прочитать сообщение' : 'Откройте чат, чтобы прочитать сообщение',
                        { tag: `msg_${chatId}` }
                    );
                }
        
                // Обновляем счётчик в памяти — нужно для папки "Входящие"
                const contactInMemory = (window._allContacts || []).find(c => c.username === chatId);
                if (contactInMemory) {
                    contactInMemory.unread_count = (contactInMemory.unread_count || 0) + 1;
                }
        
                const peerEl = document.querySelector(`.contact-item[data-peer="${chatId}"]`);
                if (peerEl) {
                    let badge = peerEl.querySelector('.unread-badge');
                    if (!badge) {
                        badge = document.createElement('div');
                        badge.className = 'unread-badge';
                        const meta = peerEl.querySelector('.contact-meta');
                        if (meta) meta.appendChild(badge);
                    }
                    const cur = parseInt(badge.textContent) || 0;
                    badge.textContent = cur + 1 > 99 ? '99+' : String(cur + 1);
                } else {
                    if (typeof syncMyContacts === 'function') await syncMyContacts();
                }
        
                // Обновляем папку "Входящие" если она активна
                if (window._activeFolder === 'inbox') {
                    const folder = getAllFolders().find(f => f.id === 'inbox');
                    if (folder) renderContactsList(filterContactsByFolder(window._allContacts, folder));
                }
            }
            refreshContactPreview(chatId);
        }
    });

    socket.on("message_edited", async (data) => {
        const msgEl = document.querySelector(`.msg[data-id="${data.id}"]`);
        if (msgEl) {
            try {
                const text = await cryptoMod.decrypt(currentAES, data.cipher);
                const textEl = msgEl.querySelector('.msg-text');
                if (textEl) {
                    textEl.innerHTML = formatRichMessageHtml(text);
                }
                msgEl._rawContent = text;
                if (!msgEl.querySelector('.edit-label')) {
                    msgEl.querySelector('.msg-meta').insertAdjacentHTML('afterbegin', '<span class="edit-label">изменено</span>');
                }
            } catch(e) { console.error("Edit decrypt error"); }
        }
    });

    socket.on('message_deleted', (data) => {
        const msgElement = document.querySelector(`.msg[data-id="${data.id}"]`);
        if (msgElement) {
            const isLast = !msgElement.nextElementSibling || !msgElement.nextElementSibling.classList.contains('msg');
            msgElement.style.transition = "all 0.3s ease";
            msgElement.style.opacity = "0";
            msgElement.style.transform = "scale(0.5)";
            setTimeout(() => {
                msgElement.remove();
                // После удаления обновляем превью из нового последнего сообщения
                if (isLast && window.currentChat) {
                    const msgs = document.querySelectorAll('#messages .msg');
                    if (msgs.length > 0) {
                        const lastMsg = msgs[msgs.length - 1];
                        const lastText = lastMsg.querySelector('.msg-text')?.textContent || '';
                        const isMe = lastMsg.classList.contains('msg-me');
                        const chatId = getChatIdForPeer(window.currentChat);
                        const prefix = isMe ? 'Вы: ' : '';
                        const newPreview = prefix + (lastText.length > 50 ? lastText.slice(0, 48) + '…' : lastText);
                        saveMsgPreview(chatId, newPreview);
                        refreshContactPreview(window.currentChat);
                    } else {
                        // Сообщений не осталось
                        const chatId = getChatIdForPeer(window.currentChat);
                        saveMsgPreview(chatId, 'Нет сообщений');
                        refreshContactPreview(window.currentChat);
                    }
                }
            }, 300);
        }
    });

    socket.on("pin_updated", (data) => {
        const chatId = String(data.chat_id || '');
        const activeChat = window.currentChat ? getChatIdForPeer(window.currentChat) : '';
        if (chatId && activeChat && chatId === activeChat) {
            loadChatPins(window.currentChat).catch(() => {});
        }
    });

    socket.on("reaction_updated", (data) => {
        const chatId = String(data?.chat_id || '').toLowerCase();
        const activeChat = window.currentChat ? String(getChatIdForPeer(window.currentChat) || '').toLowerCase() : '';
        if (chatId && activeChat && chatId !== activeChat) return;
        applyReactionsToMessage(String(data?.id || ''), data?.reactions || {});
    });

    socket.on("force_logout_others", async (data) => {
        // Проверяем: мой ли это ник и НЕ моя ли это активная вкладка
        if (data.username === username && data.initiator !== sessionId) {
            
            // Показываем кастомную модалку вместо alert
            await showCustomModal(
                "Сессия завершена", 
                "Доступ к аккаунту на этом устройстве был ограничен владельцем.", 
                false, // нет инпута
                false  // нет кнопки отмены (только ОК)
            );
    
            // После того как пользователь нажал ОК:
            indexedDB.deleteDatabase("LevartVault");
            clearAuthSession(username);
            window.location.replace("/info");
        } 
        else if (data.username === username) {
            // Ничего не делаем: кнопка мультивхода временно скрыта из интерфейса.
        }
    });

    // Слушаем просто изменение статуса (когда кто-то другой ВКЛЮЧИЛ)
    socket.on("backup_status_changed", (data) => {
        if (data.username === username) {
            // Кнопка мультивхода временно скрыта из интерфейса.
        }
    });
    socket.on("stories_updated", () => {
        loadStoriesFeed().catch(() => {});
    });
    socket.on("group_members_updated", async (data) => {
        const gid = String(data?.group_id || '');
        if (!gid) return;
        await syncMyContacts();
        if (window._currentGroupId === gid) {
            openGroupPanel(gid).catch(() => {});
        }
        if (window.currentChat === gid && data?.removed) {
            showToast('Вы удалены из группы', 'error');
            document.getElementById('messages').innerHTML = '';
            document.getElementById('input-area').style.display = 'none';
            window.currentChat = null;
        }
    });
    socket.on("group_key_needed", async (data) => {
        const gid = String(data?.group_id || '');
        const member = String(data?.member || '').toLowerCase();
        if (!gid || !member) return;
        const ok = await tryGrantGroupKeyToMember(gid, member);
        if (ok && window._currentGroupId === gid) {
            openGroupPanel(gid).catch(() => {});
        }
    });
    socket.on("group_key_updated", async (data) => {
        const gid = String(data?.group_id || '');
        if (!gid) return;
        if (window.currentChat === gid) {
            try {
                const aes = await getAESKeyForPeer(gid);
                sessionAESKeys[gid] = aes;
                window.currentAES = aes;
                const inputArea = document.getElementById('input-area');
                if (inputArea) inputArea.style.display = 'flex';
                showToast('Доступ к ключу группы получен');
            } catch (e) {
                logSilent("group_key_updated", e);
            }
        }
    });
}

/**
 * ==========================================
 * 11. ВСПОМОГАТЕЛЬНЫЙ UI
 * ==========================================
 */
async function decryptToPreview(url, key, imgElement) {
    try {
        const blob = await getDecryptedMediaBlob(url, key, "image/jpeg");
        const blobUrl = URL.createObjectURL(blob);
        
        imgElement.src = blobUrl;
        imgElement.onload = () => {
            imgElement.style.filter = "none";
            imgElement.style.opacity = "1";
            const loader = imgElement.parentElement.querySelector('.loader-ring');
            if (loader) loader.remove();
        };
    } catch (e) {
        console.error("Preview decrypt error:", e);
        imgElement.alt = "Ошибка загрузки";
    }
}

async function mediaCacheRequest(url, key, mime) {
    const txt = `${url}|${key}|${mime}`;
    const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(txt));
    const hex = Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, "0")).join("");
    return new Request(`/__media_cache__/${hex}`);
}

function decodeFileKeyB64(fileKeyB64) {
    const src = String(fileKeyB64 || '').trim().replace(/-/g, '+').replace(/_/g, '/');
    const pad = src.length % 4 ? '='.repeat(4 - (src.length % 4)) : '';
    return new Uint8Array(atob(src + pad).split("").map(c => c.charCodeAt(0)));
}

function inferMimeFromName(name = '', fallback = 'application/octet-stream') {
    const n = String(name || '').toLowerCase();
    if (n.endsWith('.jpg') || n.endsWith('.jpeg')) return 'image/jpeg';
    if (n.endsWith('.png')) return 'image/png';
    if (n.endsWith('.webp')) return 'image/webp';
    if (n.endsWith('.gif')) return 'image/gif';
    if (n.endsWith('.mp4')) return 'video/mp4';
    if (n.endsWith('.webm')) return 'video/webm';
    if (n.endsWith('.mov')) return 'video/quicktime';
    if (n.endsWith('.m4v')) return 'video/x-m4v';
    if (n.endsWith('.mp3')) return 'audio/mpeg';
    if (n.endsWith('.m4a')) return 'audio/mp4';
    if (n.endsWith('.aac')) return 'audio/aac';
    if (n.endsWith('.ogg') || n.endsWith('.oga')) return 'audio/ogg';
    if (n.endsWith('.wav')) return 'audio/wav';
    return fallback;
}

function resolveMediaMime(fileData = {}, fallback = 'application/octet-stream') {
    const byMime = String(fileData.mime || '').trim();
    if (byMime) return byMime;
    return inferMimeFromName(fileData.name || '', fallback);
}

async function getDecryptedMediaBlob(url, fileKeyB64, mimeType = "application/octet-stream") {
    const cacheReq = await mediaCacheRequest(url, fileKeyB64, mimeType);
    const cacheName = MEDIA_CACHE_NAME(username);
    if ('caches' in window) {
        try {
            const cache = await caches.open(cacheName);
            const hit = await cache.match(cacheReq);
            if (hit) {
                const b = await hit.blob();
                if (b && b.size > 0) return b;
            }
        } catch {}
    }
    const response = await fetch(url, { cache: 'no-store' });
    if (!response.ok) throw new Error("media_fetch_failed");
    const encryptedData = await response.arrayBuffer();
    if (!encryptedData || encryptedData.byteLength <= 12) throw new Error("media_payload_invalid");
    const iv = new Uint8Array(encryptedData.slice(0, 12));
    const data = encryptedData.slice(12);
    const rawKey = decodeFileKeyB64(fileKeyB64);
    const cryptoKey = await window.crypto.subtle.importKey("raw", rawKey, { name: "AES-GCM" }, false, ["decrypt"]);
    const decrypted = await window.crypto.subtle.decrypt({ name: "AES-GCM", iv }, cryptoKey, data);
    const blob = new Blob([decrypted], { type: mimeType });
    if ('caches' in window) {
        try {
            const cache = await caches.open(cacheName);
            await cache.put(cacheReq, new Response(blob.clone(), { headers: { "Content-Type": mimeType } }));
        } catch {}
    }
    return blob;
}

function getMessageKind(msgElement) {
    if (!msgElement) return 'text';
    const kindAttr = String(msgElement.dataset.msgKind || '').trim().toLowerCase();
    if (kindAttr) return kindAttr;
    if (msgElement.classList.contains('msg-system-event')) return 'system';
    if (msgElement.querySelector('.msg-voice')) return 'voice';
    const fileWrapper = msgElement.querySelector('.file-wrapper');
    if (fileWrapper) {
        const t = String(fileWrapper.dataset.type || '').trim().toLowerCase();
        if (t) return t;
    }
    if (msgElement.querySelector('.msg-sticker')) return 'sticker';
    if (msgElement.querySelector('.msg-gif')) return 'gif';
    return 'text';
}

function getReplyKindLabel(msgElement) {
    switch (getMessageKind(msgElement)) {
        case 'video_note': return 'кружок';
        case 'voice': return 'голосовое';
        case 'video': return 'видео';
        case 'image': return 'фото';
        case 'sticker': return 'стикер';
        case 'gif': return 'GIF';
        case 'file': return 'файл';
        default: return 'сообщение';
    }
}

// Вспомогательная функция для получения превью сообщения
function getMessagePreviewText(msgElement) {
    const kind = getMessageKind(msgElement);
    const fileWrapper = msgElement?.querySelector?.('.file-wrapper');
    if (kind === 'video_note') return '🎥 Кружок';
    if (kind === 'voice') return '🎤 Голосовое';
    if (kind === 'video') return `🎬 ${fileWrapper?.dataset?.name || 'Видео'}`;
    if (kind === 'image') return `🖼️ ${fileWrapper?.dataset?.name || 'Фото'}`;
    if (kind === 'file') return `📄 ${fileWrapper?.dataset?.name || 'Файл'}`;
    if (kind === 'sticker') return '💟 Стикер';
    if (kind === 'gif') return '🎞️ GIF';
    if (kind === 'system') return 'Системное сообщение';
    const textEl = msgElement?.querySelector?.('.msg-text');
    const text = String(textEl ? textEl.innerText : '').trim();
    return text || 'Сообщение';
}

const REACTION_EMOJIS = ['👍', '❤️', '😂', '🔥', '😮', '😢', '👏', '🎉'];

function normalizeReactionsMap(raw) {
    const out = {};
    if (!raw || typeof raw !== 'object') return out;
    Object.entries(raw).forEach(([emoji, users]) => {
        if (!emoji) return;
        const arr = Array.isArray(users)
            ? users.map((u) => String(u || '').trim().toLowerCase()).filter(Boolean)
            : [];
        if (arr.length) out[emoji] = arr;
    });
    return out;
}

function toggleReactionInMap(raw, emoji, actor) {
    const map = normalizeReactionsMap(raw);
    const key = String(emoji || '').trim();
    const me = String(actor || '').trim().toLowerCase();
    if (!key || !me) return map;
    const users = Array.isArray(map[key]) ? [...map[key]] : [];
    const has = users.includes(me);
    const next = has ? users.filter((u) => u !== me) : [...users, me];
    if (next.length) map[key] = next;
    else delete map[key];
    return map;
}

function buildReactionButtonsHtml(messageId) {
    const safeId = String(messageId || '').replace(/"/g, '&quot;');
    return REACTION_EMOJIS
        .map((emoji) => `<button type="button" onclick="reactToMessage('${safeId}','${emoji}', this)">${emoji}</button>`)
        .join('');
}

function buildMessageReactionsHtml(messageId, rawReactions) {
    const reactions = normalizeReactionsMap(rawReactions);
    const entries = Object.entries(reactions);
    if (!entries.length) return '';
    const chips = entries.map(([emoji, users]) => {
        const me = users.includes(String(username || '').toLowerCase());
        const cls = me ? 'msg-reaction-chip mine' : 'msg-reaction-chip';
        const count = users.length;
        return `<button type="button" class="${cls}" onclick="reactToMessage('${String(messageId || '').replace(/"/g, '&quot;')}','${emoji}', this)">${emoji}<span>${count}</span></button>`;
    }).join('');
    return `<div class="msg-reactions">${chips}</div>`;
}

window.reactToMessage = (messageId, emoji, btn) => {
    if (!socket || !window.currentChat) return;
    const chatId = getChatIdForPeer(window.currentChat);
    const msgEl = _resolveMessageElementById(messageId);
    let localRaw = {};
    try {
        localRaw = msgEl?.dataset?.reactions ? JSON.parse(msgEl.dataset.reactions) : {};
    } catch {}
    const optimistic = toggleReactionInMap(localRaw, emoji, username);
    applyReactionsToMessage(messageId, optimistic);
    socket.emit('toggle_reaction', {
        id: String(messageId || ''),
        chat_id: String(chatId || ''),
        username: String(username || '').toLowerCase(),
        emoji: String(emoji || '')
    });
    const menu = btn?.closest?.('.msg-dropdown');
    if (menu) menu.style.display = 'none';
};

function applyReactionsToMessage(messageId, rawReactions) {
    const msgEl = _resolveMessageElementById(messageId);
    if (!msgEl) return;
    const normalized = normalizeReactionsMap(rawReactions);
    msgEl.dataset.reactions = JSON.stringify(normalized);
    msgEl.classList.toggle('msg-has-reactions', Object.keys(normalized).length > 0);
    const old = msgEl.querySelector('.msg-reactions');
    if (old) old.remove();
    const html = buildMessageReactionsHtml(messageId, rawReactions);
    if (!html) return;
    const meta = msgEl.querySelector('.msg-meta');
    if (meta) {
        meta.insertAdjacentHTML('beforebegin', html);
    } else {
        msgEl.insertAdjacentHTML('beforeend', html);
    }
}

function isEmojiOnlyText(s) {
    const t = String(s || '').trim();
    if (!t || t.length > 16) return false;
    const cleaned = t.replace(/[\s\u200d\ufe0f]/g, '');
    if (!cleaned) return false;
    const hasEmoji = /[\u2600-\u27BF]|[\uD83C-\uDBFF][\uDC00-\uDFFF]/.test(cleaned);
    if (!hasEmoji) return false;
    return /^[\u2600-\u27BF\u200d\ufe0f\uD83C-\uDBFF\uDC00-\uDFFF]+$/.test(cleaned);
}

function formatRichMessageHtml(rawText) {
    const KNOWN_CODE_LANGS = new Set([
        'python','py','javascript','js','typescript','ts','java','kotlin','swift','go','rust','c','cpp','csharp','cs',
        'php','ruby','rb','lua','sql','bash','sh','powershell','ps1','html','xml','css','scss','json','yaml','yml',
        'toml','ini','dockerfile','nginx','markdown','md'
    ]);
    const esc = String(rawText || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    const blocks = [];
    let txt = esc.replace(/```([a-zA-Z0-9_+\-]+)?\n?([\s\S]*?)```/g, (_, lang, code) => {
        const id = `@@CODEBLOCK_${blocks.length}@@`;
        const safeLangRaw = String(lang || '').trim();
        const safeLang = KNOWN_CODE_LANGS.has(safeLangRaw.toLowerCase()) ? safeLangRaw : '';
        const body = safeLang ? code : `${safeLangRaw ? `${safeLangRaw}\n` : ''}${code}`;
        blocks.push(
            `<div class="msg-codeblock-wrap">` +
            `${safeLang ? `<div class="msg-codeblock-lang">${safeLang}</div>` : ''}` +
            `<pre class="msg-codeblock"><code>${body}</code></pre>` +
            `</div>`
        );
        return id;
    });

    const formatInline = (source) => {
        const inlineCodes = [];
        let s = String(source || '').replace(/`([^`\n]+?)`/g, (_, code) => {
            const id = `@@INLINECODE_${inlineCodes.length}@@`;
            inlineCodes.push(`<code class="msg-code">${code}</code>`);
            return id;
        });
        s = s.replace(/\*\*([^*\n][\s\S]*?)\*\*/g, '<strong>$1</strong>');
        s = s.replace(/__([^_\n][\s\S]*?)__/g, '<u>$1</u>');
        s = s.replace(/\*([^*\n][\s\S]*?)\*/g, '<em>$1</em>');
        s = s.replace(/~~([^~\n][\s\S]*?)~~/g, '<s>$1</s>');
        s = s.replace(/\|\|([^|\n][\s\S]*?)\|\|/g, '<span class="msg-spoiler">$1</span>');
        s = s.replace(/(^|\s)@([a-zA-Z0-9_]{2,32})/g, '$1<span class="msg-mention">@$2</span>');
        s = s.replace(/(^|[\s(])((https?:\/\/|www\.)[^\s<]+)/gi, (m, pre, url) => {
            const href = url.startsWith('www.') ? `https://${url}` : url;
            return `${pre}<a href="${href}" target="_blank" rel="noopener noreferrer" class="msg-link">${url}</a>`;
        });
        inlineCodes.forEach((html, i) => {
            s = s.replace(`@@INLINECODE_${i}@@`, html);
        });
        return s;
    };

    const lines = txt.split('\n');
    const out = [];
    let i = 0;

    while (i < lines.length) {
        const line = lines[i];

        if (/^@@CODEBLOCK_\d+@@$/.test(line.trim())) {
            out.push(line.trim());
            i += 1;
            continue;
        }

        if (/^\s*(?:-{3,}|\*{3,}|_{3,})\s*$/.test(line)) {
            out.push('<div class="msg-divider"></div>');
            i += 1;
            continue;
        }

        const h = line.match(/^\s*(#{1,3})\s+(.+)$/);
        if (h) {
            const lvl = h[1].length;
            out.push(`<span class="msg-h${lvl}">${formatInline(h[2])}</span>`);
            i += 1;
            continue;
        }

        if (/^\s*&gt;\s?/.test(line)) {
            const q = [];
            while (i < lines.length && /^\s*&gt;\s?/.test(lines[i])) {
                q.push(formatInline(lines[i].replace(/^\s*&gt;\s?/, '')));
                i += 1;
            }
            out.push(`<span class="msg-quote">${q.join('<br>')}</span>`);
            continue;
        }

        if (/^\s*[-*]\s+/.test(line)) {
            const items = [];
            while (i < lines.length && /^\s*[-*]\s+/.test(lines[i])) {
                items.push(`<li>${formatInline(lines[i].replace(/^\s*[-*]\s+/, ''))}</li>`);
                i += 1;
            }
            out.push(`<ul class="msg-list">${items.join('')}</ul>`);
            continue;
        }

        if (/^\s*\d+\.\s+/.test(line)) {
            const items = [];
            while (i < lines.length && /^\s*\d+\.\s+/.test(lines[i])) {
                items.push(`<li>${formatInline(lines[i].replace(/^\s*\d+\.\s+/, ''))}</li>`);
                i += 1;
            }
            out.push(`<ol class="msg-list msg-olist">${items.join('')}</ol>`);
            continue;
        }

        out.push(formatInline(line));
        i += 1;
    }

    txt = out.join('<br>');
    blocks.forEach((html, idx) => {
        txt = txt.replace(`@@CODEBLOCK_${idx}@@`, html);
    });
    return txt;
}

function getEmojiAnimationClass(s) {
    const t = String(s || '');
    if (/❤️|💖|💕|💘|💗|💓|💞|💝/.test(t)) return 'emoji-anim-heart';
    if (/🔥|💥|✨|⭐|🌟|⚡/.test(t)) return 'emoji-anim-burst';
    if (/😂|🤣|😆|😹/.test(t)) return 'emoji-anim-bounce';
    if (/😢|😭|🥹/.test(t)) return 'emoji-anim-shake';
    return 'emoji-anim-float';
}

function getEmojiCountClass(s) {
    const cleaned = String(s || '').replace(/[\s\u200d\ufe0f]/g, '');
    const n = Array.from(cleaned).length;
    if (n <= 1) return 'emoji-count-1';
    if (n === 2) return 'emoji-count-2';
    return 'emoji-count-3';
}

function mountAnimatedSticker(root) {
    if (!root) return;
    const imgs = root.querySelectorAll('img[data-sticker-frames]');
    imgs.forEach((img) => {
        if (img.dataset.animMounted === '1') return;
        let frames = [];
        try { frames = JSON.parse(img.dataset.stickerFrames || '[]'); } catch {}
        if (!Array.isArray(frames) || frames.length < 2) return;
        let i = 0;
        img.dataset.animMounted = '1';
        const delay = Math.max(60, Number(img.dataset.stickerDelay || 150));
        setInterval(() => {
            i = (i + 1) % frames.length;
            img.src = frames[i];
        }, delay);
    });
}

function stripSystemPrefix(text) {
    const raw = String(text || '');
    return raw.replace(/^\s*system\s*:\s*/i, '').trim();
}

function addMessageToScreen(sender, text, messageId, replyToId = null, time = "", isEdited = false, forceSystem = false, extraMeta = {}) {
    const msgDiv = document.getElementById("messages");
    const isSystemEvent = !!forceSystem || String(sender || '').toLowerCase() === 'system';
    const isMe = (sender === username);
    const isGroupChat = String(window.currentChat || '').startsWith('group_');
    const showGroupPeerInfo = isGroupChat && !isMe && !isSystemEvent;
    const div = document.createElement("div");
    div.className = isSystemEvent ? 'msg msg-system-event' : `msg ${isMe ? 'msg-me' : 'msg-them'}`;
    if (showGroupPeerInfo) div.classList.add('msg-group-peer');
    div.setAttribute('data-id', messageId);
    div._rawSender = sender;
    div._rawContent = text;

    // Обновляем превью
    if (window.currentChat) {
        updateMsgPreview(sender, text, getChatIdForPeer(window.currentChat));
    }

    let contentHtml = "";
    let isFile = false;
    let isVoiceFile = false;
    let isVideoNoteFile = false;
    let messageKind = 'text';
    let hideCopyAction = true;
    let canEditMessage = false;

    let replyHtml = "";
    if (replyToId) {
        div.classList.add('msg-has-reply');
        const parentMsg = document.querySelector(`.msg[data-id="${replyToId}"]`);
        const parentText = (parentMsg ? getMessagePreviewText(parentMsg) : "Сообщение").replace(/\s+/g, ' ').trim();
        const previewLimit = 16;
        const replyBaseText = parentText || 'Сообщение';
        const replyPreview = replyBaseText.length > previewLimit
            ? `${replyBaseText.slice(0, previewLimit)}…`
            : replyBaseText;
        const safeReplyId = String(replyToId).replace(/"/g, '&quot;');
        replyHtml = `<button type="button" class="reply-content" data-reply-to="${safeReplyId}">${replyPreview}</button>`;
    }

    if (typeof text === 'object' && text.url && text.file_key) {
        isFile = true;
        const fileData = text;
        const mediaFallback = fileData.type === 'voice'
            ? 'audio/webm'
            : (fileData.type === 'video' || fileData.type === 'video_note')
                ? 'video/mp4'
                : (fileData.type === 'image' ? 'image/jpeg' : 'application/octet-stream');
        const mediaMime = resolveMediaMime(fileData, mediaFallback);
        const normalizedFileName = String(fileData.name || 'file');
        const safeFileName = normalizedFileName.replace(/"/g, '&quot;');

        if (fileData.type === "image") {
            messageKind = 'image';
            const tempImgId = "prev_" + messageId.replace(/\W/g, '');
            contentHtml = `
                <div class="file-wrapper image-attach" data-url="${fileData.url}" data-key="${fileData.file_key}" data-name="${safeFileName}" data-type="image">
                    <div class="image-container telegram-style">
                        <div class="loader-ring"></div>
                        <img id="${tempImgId}" class="file-preview-img blurred" src="" alt="${safeFileName}">
                    </div>
                    <div class="file-name-sub">${safeFileName}</div>
                </div>`;
            setTimeout(() => {
                const el = document.getElementById(tempImgId);
                if (el) decryptToPreview(fileData.url, fileData.file_key, el);
            }, 50);

        } else if (fileData.type === "video" || fileData.type === "video_note") {
            const tempVidId = "vid_" + messageId.replace(/\W/g, '');
            const isNote = fileData.type === "video_note";
            messageKind = isNote ? 'video_note' : 'video';
            if (isNote) isVideoNoteFile = true;
            contentHtml = `
                <div class="file-wrapper video-attach ${isNote ? 'video-note-wrap' : ''}" data-url="${fileData.url}" data-key="${fileData.file_key}" data-name="${safeFileName}" data-type="${isNote ? 'video_note' : 'video'}" data-mime="${mediaMime || 'video/mp4'}">
                    <div class="image-container telegram-style video-preview-shell" style="min-height:${isNote ? '0' : '160px'};">
                        <div class="loader-ring" id="loader_${tempVidId}"></div>
                        <video id="${tempVidId}" class="file-preview-vid"
                               muted playsinline preload="metadata"
                               style="${isNote ? 'width:100%;height:100%;object-fit:cover;display:none;border-radius:50%;transform:scaleX(-1);' : 'max-width:100%;max-height:380px;object-fit:contain;display:none;border-radius:10px;'}"></video>
                        ${isNote ? '' : `<div class="video-overlay" id="overlay_${tempVidId}" style="display:none;"><button class="video-play-btn">▶</button></div>`}
                        ${isNote ? '<div class="video-note-badge">Кружок</div>' : ''}
                    </div>
                    ${isNote ? `<div class="video-note-custom-controls" id="vnc_${tempVidId}">
                      <div class="video-note-progress" id="vnp_${tempVidId}">
                        <div class="video-note-progress-fill" id="vnpf_${tempVidId}"></div>
                        <input type="range" class="video-note-slider" id="vns_${tempVidId}" min="0" max="1000" value="0" step="1" aria-label="Перемотка кружка">
                      </div>
                      <div class="video-note-time" id="vnt_${tempVidId}">0:00</div>
                    </div>` : ''}
                    <div class="file-name-sub">${safeFileName}</div>
                </div>`;

            setTimeout(async () => {
                const loaderEl = document.getElementById(`loader_${tempVidId}`);
                const vidEl = document.getElementById(tempVidId);
                const overlayEl = document.getElementById(`overlay_${tempVidId}`);
                if (!vidEl) return;
                try {
                    const mime = mediaMime || 'video/mp4';
                    const blob = await getDecryptedMediaBlob(fileData.url, fileData.file_key, mime);
                    const blobUrl = URL.createObjectURL(blob);

                    hardenVideoElementUi(vidEl);
                    vidEl.playsInline = true;
                    let loadedHandled = false;
                    const onLoaded = () => {
                        if (loadedHandled) return;
                        loadedHandled = true;
                        if (loaderEl) loaderEl.remove();
                        vidEl.style.display = 'block';
                        if (overlayEl) overlayEl.style.display = 'flex';
                        try { vidEl.currentTime = 0.01; } catch {}
                        if (isNote) {
                            const progress = document.getElementById(`vnp_${tempVidId}`);
                            const progressFill = document.getElementById(`vnpf_${tempVidId}`);
                            const slider = document.getElementById(`vns_${tempVidId}`);
                            const timeEl = document.getElementById(`vnt_${tempVidId}`);
                            const controlsEl = document.getElementById(`vnc_${tempVidId}`);
                            controlsEl?.addEventListener('click', (ev) => ev.stopPropagation());
                            if (timeEl) timeEl.textContent = formatMediaTime(vidEl.duration || 0);
                            vidEl.ontimeupdate = () => {
                                if (timeEl) timeEl.textContent = formatMediaTime(vidEl.currentTime || 0);
                                if (progressFill && vidEl.duration) {
                                    const pct = Math.max(0, Math.min(100, (vidEl.currentTime / vidEl.duration) * 100));
                                    progressFill.style.width = `${pct}%`;
                                }
                                if (slider && vidEl.duration) {
                                    slider.value = String(Math.max(0, Math.min(1000, Math.round((vidEl.currentTime / vidEl.duration) * 1000))));
                                }
                            };
                            vidEl.onended = () => {
                                if (progressFill) progressFill.style.width = '100%';
                                if (slider) slider.value = '1000';
                                const noteWrap = vidEl.closest('.file-wrapper');
                                if (noteWrap?.classList.contains('note-expanded')) {
                                    toggleVideoNoteInline(noteWrap);
                                }
                            };
                            progress?.addEventListener('click', (ev) => {
                                ev.stopPropagation();
                                const rect = progress.getBoundingClientRect();
                                if (!rect.width || !vidEl.duration) return;
                                const k = Math.max(0, Math.min(1, (ev.clientX - rect.left) / rect.width));
                                vidEl.currentTime = vidEl.duration * k;
                                if (slider) slider.value = String(Math.round(k * 1000));
                            });
                            slider?.addEventListener('input', (ev) => {
                                ev.stopPropagation();
                                if (!vidEl.duration) return;
                                const k = Number(slider.value || 0) / 1000;
                                vidEl.currentTime = Math.max(0, Math.min(vidEl.duration, vidEl.duration * k));
                            });
                        }
                    };
                    vidEl.addEventListener('loadedmetadata', onLoaded, { once: true });
                    vidEl.addEventListener('loadeddata', onLoaded, { once: true });
                    vidEl.addEventListener('canplay', onLoaded, { once: true });
                    vidEl.addEventListener('error', () => {
                        if (loaderEl) loaderEl.innerHTML = '<span style="color:var(--danger);font-size:11px;">Ошибка</span>';
                    }, { once: true });
                    vidEl.src = blobUrl;
                    if (vidEl.readyState >= 1) onLoaded();
                } catch (e) {
                    if (loaderEl) loaderEl.innerHTML = '<span style="color:var(--danger);font-size:11px;">Ошибка</span>';
                }
            }, 50);

        } else if (fileData.type === "voice") {
            isVoiceFile = true;
            messageKind = 'voice';
            hideCopyAction = true;
            const safeTranscript = String(fileData.transcript || '').replace(/</g, '&lt;').replace(/>/g, '&gt;');
            contentHtml = `
                <div class="msg-voice" data-vurl="${fileData.url}" data-vkey="${fileData.file_key}" data-vmime="${mediaMime || 'audio/webm'}">
                    <button class="voice-play-btn">▶</button>
                    <div class="voice-track">
                        <div class="voice-wave-bars">${Array.from({ length: 18 }).map(() => '<span></span>').join('')}</div>
                        <div class="voice-progress">
                          <div class="voice-progress-fill"></div>
                          <input type="range" class="voice-slider" min="0" max="1000" value="0" step="1" aria-label="Перемотка голосового">
                        </div>
                    </div>
                    <div class="voice-time">0:00</div>
                    <button class="voice-transcribe-btn ${safeTranscript ? 'done' : ''}" title="Расшифровать голосовое">↘</button>
                    <div class="voice-transcript ${safeTranscript ? '' : 'empty'}">${safeTranscript ? `📝 ${safeTranscript}` : ''}</div>
                    <audio preload="none" src="" style="display:none;"></audio>
                </div>
            `;
            setTimeout(async () => {
                const wrap = div.querySelector('.msg-voice[data-vurl]');
                const audio = wrap?.querySelector('audio');
                const playBtn = wrap?.querySelector('.voice-play-btn');
                const progressFill = wrap?.querySelector('.voice-progress-fill');
                const slider = wrap?.querySelector('.voice-slider');
                const timeEl = wrap?.querySelector('.voice-time');
                const transcriptEl = wrap?.querySelector('.voice-transcript');
                const transcribeBtn = wrap?.querySelector('.voice-transcribe-btn');
                if (!audio) return;
                try {
                    const voiceBlob = await getDecryptedMediaBlob(wrap.dataset.vurl, wrap.dataset.vkey, wrap.dataset.vmime || 'audio/webm');
                    wrap._voiceBlob = voiceBlob;
                    audio.src = URL.createObjectURL(voiceBlob);
                    audio.load();
                    audio.onloadedmetadata = () => {
                        if (timeEl) timeEl.textContent = formatMediaTime(audio.duration);
                        if (slider) slider.value = '0';
                    };
                    audio.onerror = () => {
                        showToast('Не удалось воспроизвести голосовое', 'error');
                    };
                    audio.ontimeupdate = () => {
                        if (timeEl) timeEl.textContent = formatMediaTime(audio.currentTime);
                        if (progressFill && audio.duration) {
                            progressFill.style.width = `${Math.max(0, Math.min(100, (audio.currentTime / audio.duration) * 100))}%`;
                        }
                        if (slider && audio.duration) {
                            slider.value = String(Math.max(0, Math.min(1000, Math.round((audio.currentTime / audio.duration) * 1000))));
                        }
                    };
                    audio.onended = () => {
                        if (playBtn) playBtn.textContent = '▶';
                        if (timeEl) timeEl.textContent = formatMediaTime(audio.duration);
                        if (progressFill) progressFill.style.width = '100%';
                        if (slider) slider.value = '1000';
                    };
                    if (slider) {
                        slider.oninput = () => {
                            if (!audio.duration) return;
                            const k = Number(slider.value || 0) / 1000;
                            audio.currentTime = Math.max(0, Math.min(audio.duration, audio.duration * k));
                        };
                    }
                    if (playBtn) {
                        playBtn.onclick = () => {
                            if (audio.paused) {
                                audio.play().then(() => {
                                    playBtn.textContent = '⏸';
                                }).catch(() => {
                                    showToast('Формат голосового не поддерживается на этом устройстве', 'error');
                                });
                            } else {
                                audio.pause();
                                playBtn.textContent = '▶';
                            }
                        };
                    }
                    if (transcribeBtn && transcriptEl) {
                        transcribeBtn.onclick = async () => {
                            if (transcribeBtn.classList.contains('loading')) return;
                            if (transcriptEl.textContent.trim()) {
                                transcriptEl.classList.toggle('open');
                                return;
                            }
                            if (!wrap._voiceBlob) {
                                showToast('Голосовое еще загружается', 'error');
                                return;
                            }
                            if (Number(audio.duration || 0) > 120) {
                                showToast('Для расшифровки доступно голосовое до 2 минут', 'error');
                                return;
                            }
                            transcribeBtn.classList.add('loading');
                            transcribeBtn.textContent = '…';
                            try {
                                const transcript = await requestAudioTranscription(wrap._voiceBlob, 'ru');
                                if (!transcript) {
                                    showToast('Речь не распознана', 'error');
                                    return;
                                }
                                transcriptEl.textContent = `📝 ${transcript}`;
                                transcriptEl.classList.remove('empty');
                                transcriptEl.classList.add('open');
                                transcribeBtn.classList.add('done');
                            } catch {
                                showToast('Не удалось расшифровать', 'error');
                            } finally {
                                transcribeBtn.classList.remove('loading');
                                transcribeBtn.textContent = '↘';
                            }
                        };
                    }
                } catch (e) {
                    console.error('Voice decrypt/playback init failed:', e);
                    if (playBtn) playBtn.disabled = true;
                    showToast('Не удалось загрузить голосовое', 'error');
                }
            }, 20);
        } else {
            messageKind = 'file';
            contentHtml = `
                <div class="file-wrapper doc-attach" data-url="${fileData.url}" data-key="${fileData.file_key}" data-name="${safeFileName}" data-type="file">
                    <div class="file-icon">📄</div>
                    <div class="file-details">
                        <div class="file-name">${safeFileName}</div>
                        <div class="file-action">Нажмите, чтобы скачать</div>
                    </div>
                </div>`;
        }
    } else {
        // Стикеры и GIF
        if (typeof text === 'string' && text.trim().startsWith('{')) {
            try {
                const data = JSON.parse(text.trim());
                if (!data.__STICKER__ && !data.__GIF__) throw new Error('not special');
                if (data.__STICKER__) {
                    messageKind = 'sticker';
                    const isAnimatedSticker = Array.isArray(data.frames) && data.frames.length > 1;
                    const stickerSize = isAnimatedSticker ? 156 : 118;
                    const inner = data.src
                        ? `<img src="${data.src}" style="width:${stickerSize}px;height:${stickerSize}px;object-fit:contain;display:block;" ${isAnimatedSticker ? `data-sticker-frames='${JSON.stringify(data.frames).replace(/'/g, '&#39;')}' data-sticker-delay='${Number(data.delay || 150)}'` : ''}>`
                        : `<span style="font-size:72px;line-height:1;display:block;text-align:center;">${data.emoji || '🎭'}</span>`;
                    contentHtml = `<div class="msg-sticker">${inner}</div>`;
                    // Стикерам не нужен фон пузыря
                    div.classList.add('msg-sticker-wrap', 'msg-media-plain');
                    hideCopyAction = true;
                } else if (data.__GIF__) {
                    messageKind = 'gif';
                    contentHtml = `<div class="msg-gif">
                        <img src="${data.url}" alt="${data.title || 'GIF'}"
                             style="max-width:220px;width:100%;border-radius:12px;display:block;"
                             loading="lazy">
                        ${data.title ? `<div style="font-size:10px;opacity:0.5;text-align:center;margin-top:2px;">${data.title}</div>` : ''}
                    </div>`;
                    div.classList.add('msg-media-plain');
                    hideCopyAction = true;
                }
            } catch(e) {
                // Если парсинг не вышел — показываем как обычный текст
                const safeText = String(text).replace(/</g, "&lt;").replace(/>/g, "&gt;");
                contentHtml = `<div class="msg-text">${safeText}</div>`;
                messageKind = 'text';
                hideCopyAction = false;
                canEditMessage = true;
            }
        }
        
        // Обычный текст (если contentHtml не установлен выше)
        if (!contentHtml) {
            const plainText = String(text || '');
            const onlyEmoji = isEmojiOnlyText(plainText);
            messageKind = onlyEmoji ? 'emoji' : 'text';
            contentHtml = onlyEmoji
                ? `<div class="msg-text"><span class="msg-big-emoji ${getEmojiAnimationClass(plainText)} ${getEmojiCountClass(plainText)}">${plainText.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</span></div>`
                : `<div class="msg-text">${formatRichMessageHtml(plainText)}</div>`;
            if (onlyEmoji) {
                div.classList.add('msg-media-plain');
                hideCopyAction = true;
                canEditMessage = false;
            } else {
                hideCopyAction = false;
                canEditMessage = true;
            }
        }
    }

    if (!isSystemEvent && (isVoiceFile || isVideoNoteFile)) {
        div.classList.add('msg-media-plain');
        if (isVideoNoteFile) hideCopyAction = true;
    }

    const forwardedFrom = String(extraMeta?.forwardedFrom || '').trim();
    const forwardedFromAttr = forwardedFrom.replace(/</g, '&lt;').replace(/"/g, '&quot;');
    const forwardedLabel = forwardedFrom
        ? `<button type="button" class="msg-forwarded msg-forwarded-link" data-forwarded-from="${forwardedFromAttr}">Переслано от @${forwardedFrom.replace(/</g, '&lt;')}</button>`
        : '';
    const reactionsHtml = buildMessageReactionsHtml(messageId, extraMeta?.reactions || {});
    const reactionButtonsHtml = buildReactionButtonsHtml(messageId);

    if (isSystemEvent) {
        messageKind = 'system';
        const safeSystemText = stripSystemPrefix(text).replace(/</g, '&lt;').replace(/>/g, '&gt;');
        div.innerHTML = `
            <div class="sys-event-text">${safeSystemText}</div>
            <div class="sys-event-time">${time || ''}</div>
        `;
    } else {
        div.innerHTML = `
            <div class="swipe-reply-icon">↩️</div>
            ${showGroupPeerInfo ? `<div class="msg-peerline"><span class="msg-peer-avatar">${String(sender || '?').charAt(0).toUpperCase()}</span><span class="msg-info">${sender}</span></div>` : ''}
            ${forwardedLabel}
            ${replyHtml}
            ${contentHtml}
            ${reactionsHtml}
            <div class="msg-meta">
                ${isEdited ? '<span class="edit-label">изменено</span>' : ''}
                <span class="msg-time">${time}</span>
                ${isMe ? '<span class="msg-status delivered" style="margin-left:4px;font-size:12px;color:#60a5fa;"></span>' : ''}
            </div>
            <div class="msg-dropdown">
                <div class="msg-react-picker">${reactionButtonsHtml}</div>
                ${hideCopyAction ? '' : `<button onclick="copyMessageFromMenu('${messageId}', this)">📋 Копировать</button>`}
                <button onclick="openForwardMessageModal('${messageId}', this)">↪️ Переслать</button>
                <button onclick="prepareReply('${messageId}', '${sender}', this)">↩️ Ответить</button>
                <button onclick="openPinScopeModal('${messageId}', this)">📌 Закрепить</button>
                ${isMessagePinned(messageId) ? `<button onclick="openUnpinScopeChooser('${messageId}', this)">⛔ Открепить</button>` : ''}
                ${(isMe && canEditMessage) ? `<button onclick="prepareEdit('${messageId}', this)">✏️ Изменить</button>` : ''}
                ${isMe ? `<button onclick="deleteMessage(this, '${messageId}')">🗑️ Удалить</button>` : ''}
            </div>
        `;
    }
    div.dataset.msgKind = messageKind;

    msgDiv.appendChild(div);
    const initialReactions = normalizeReactionsMap(extraMeta?.reactions || {});
    div.dataset.reactions = JSON.stringify(initialReactions);
    if (Object.keys(initialReactions).length > 0) {
        div.classList.add('msg-has-reactions');
    }
    if (!isSystemEvent && !isFile) {
        const textEl = div.querySelector('.msg-text');
        if (textEl) {
            const txt = String(textEl.innerText || '').trim();
            const cs = window.getComputedStyle(textEl);
            const lh = parseFloat(cs.lineHeight || '0');
            const byHeight = lh > 0 && textEl.scrollHeight > (lh * 1.9);
            const byExplicitBreak = txt.includes('\n');
            const byInlineLength = txt.length >= 32 && !byHeight && !byExplicitBreak;
            if (byHeight || byExplicitBreak) {
                div.classList.add('msg-long-text');
            }
            if (byInlineLength) {
                div.classList.add('msg-long-inline');
            }
        }
    }
    const replyRefEl = div.querySelector('.reply-content[data-reply-to]');
    if (replyRefEl) {
        replyRefEl.addEventListener('click', (e) => {
            e.stopPropagation();
            const targetId = String(replyRefEl.getAttribute('data-reply-to') || '').trim();
            if (!targetId) return;
            scrollToMessage(targetId);
        });
    }
    mountAnimatedSticker(div);
    const forwardedLink = div.querySelector('.msg-forwarded-link[data-forwarded-from]');
    if (forwardedLink) {
        forwardedLink.addEventListener('click', (e) => {
            e.stopPropagation();
            const target = String(forwardedLink.getAttribute('data-forwarded-from') || '').trim().toLowerCase();
            if (!target) return;
            openUserInfo(target);
        });
    }

    // Обработчики файлов
    if (isFile && !isSystemEvent) {
        const fileWrapper = div.querySelector('.file-wrapper');
        if (fileWrapper) {
            const ftype = fileWrapper.dataset.type || (fileWrapper.classList.contains('image-attach') ? 'image' : 'file');
            if (ftype === 'image') {
                fileWrapper.style.cursor = 'pointer';
                fileWrapper.onclick = () => openMediaViewer(fileWrapper.dataset.url, fileWrapper.dataset.key, fileWrapper.dataset.name, 'image');
            } else if (ftype === 'video' || ftype === 'video_note') {
                fileWrapper.style.cursor = 'pointer';
                if (ftype === 'video_note') {
                    fileWrapper.onclick = () => toggleVideoNoteInline(fileWrapper);
                } else {
                    fileWrapper.onclick = () => openMediaViewer(fileWrapper.dataset.url, fileWrapper.dataset.key, fileWrapper.dataset.name, 'video');
                }
            } else {
                fileWrapper.style.cursor = 'pointer';
                fileWrapper.onclick = () => window.decryptAndDownload(fileWrapper.dataset.url, fileWrapper.dataset.key, fileWrapper.dataset.name);
            }
        }
    }

    // Свайп влево для ответа
    if (!isSystemEvent) {
        initSwipeToReply(div, messageId, sender);
        div.addEventListener('contextmenu', (e) => {
            if (e.target && e.target.closest && e.target.closest('.msg-dropdown')) return;
            e.preventDefault();
            openMsgMenuAtPointer(div, e.clientX, e.clientY);
        });
        const touchCapable = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);
        if (isMobile() || touchCapable) {
            div.addEventListener('click', (e) => {
                const t = e.target;
                if (!(t instanceof Element)) return;
                if (t.closest('.msg-dropdown, .reply-content, .file-wrapper, .voice-play-btn, .voice-slider, .video-note-slider, .video-note-progress, a, button, input, textarea, video, audio')) return;
                e.preventDefault();
                e.stopPropagation();
                const r = div.getBoundingClientRect();
                openMsgMenuAtPointer(div, Math.max(8, r.right - 10), Math.max(8, r.top + 12));
            });
        }
    }

    if (typeof handleScrollLogic === 'function') handleScrollLogic(isMe);
}

function initSwipeToReply(msgEl, messageId, sender) {
    let startX = 0, currentX = 0, startY = 0, isDragging = false, moved = false;
    const threshold = 60;
    const isInteractiveTarget = (target) => {
        if (!target || !(target instanceof Element)) return false;
        return !!target.closest(
            'button, input, textarea, select, a, video, audio, ' +
            '.voice-play-btn, .voice-slider, .video-note-slider, .video-note-progress, ' +
            '.msg-menu-btn, .msg-dropdown, .file-wrapper, .call-btn'
        );
    };
    const begin = (x, y) => {
        startX = x;
        currentX = x;
        startY = y;
        isDragging = true;
        moved = false;
        msgEl.classList.add('swiping');
    };
    const move = (x, y) => {
        if (!isDragging) return;
        const dx = startX - x;
        const dy = Math.abs(startY - y);
        if (dy > 36 && dx < threshold * 0.5) {
            end(false);
            return;
        }
        if (Math.abs(dx) > 6) moved = true;
        currentX = x;
        if (!moved) return;
        if (dx > 0 && dx < threshold * 1.6) {
            msgEl.style.transform = `translateX(${-dx}px)`;
            const icon = msgEl.querySelector('.swipe-reply-icon');
            if (icon) icon.style.opacity = Math.min(dx / threshold, 1);
        }
    };
    const end = (allowReply = true) => {
        if (!isDragging) return;
        isDragging = false;
        msgEl.classList.remove('swiping');
        const diff = startX - currentX;
        if (allowReply && moved && diff >= threshold) {
            const btn = msgEl.querySelector('.msg-dropdown button');
            prepareReply(messageId, sender, btn || msgEl);
            if (navigator.vibrate) navigator.vibrate(40);
        }
        msgEl.style.transform = '';
        const icon = msgEl.querySelector('.swipe-reply-icon');
        if (icon) icon.style.opacity = '0';
    };
    msgEl.addEventListener('pointerdown', (e) => {
        if (e.button !== undefined && e.button !== 0) return;
        if (isInteractiveTarget(e.target)) return;
        begin(e.clientX, e.clientY);
        msgEl.setPointerCapture?.(e.pointerId);
    });
    msgEl.addEventListener('pointermove', (e) => move(e.clientX, e.clientY));
    msgEl.addEventListener('pointerup', () => end(true));
    msgEl.addEventListener('pointercancel', () => end(false));
}


window.toggleMsgMenu = (event, btn) => {
    event.stopPropagation();
    
    const menu = btn.nextElementSibling;
    if (!menu) return;

    // Закрываем другие меню
    document.querySelectorAll('.msg-dropdown').forEach(m => {
        if (m !== menu) m.style.display = 'none';
    });

    const isOpened = menu.style.display === 'flex';
    menu.style.display = isOpened ? 'none' : 'flex';

    if (!isOpened) {
        // Настройки отображения
        menu.style.visibility = 'hidden';
        menu.style.display = 'flex';
        
        // 1. Сначала сбрасываем всё в стандарт (справа под кнопкой)
        menu.style.top = '25px';
        menu.style.bottom = 'auto';
        menu.style.left = 'auto';
        menu.style.right = '0';

        // Берем границы контейнера чата и самого меню
        const chatContainer = document.getElementById('messages'); 
        const chatRect = chatContainer.getBoundingClientRect();
        const menuRect = menu.getBoundingClientRect();

        // 2. Проверка нижней границы: если меню уходит ниже дна чата
        if (menuRect.bottom > chatRect.bottom) {
            menu.style.top = 'auto';
            menu.style.bottom = '25px'; // Прыгает наверх над кнопкой
        }

        // 3. Проверка левой границы: если сообщение слева и меню вылезло за левый край чата
        if (menuRect.left < chatRect.left) {
            menu.style.left = '0';
            menu.style.right = 'auto';
        }

        menu.style.visibility = 'visible';
    }
};

function openMsgMenuAtPointer(msgEl, clientX, clientY) {
    if (!msgEl) return;
    const menu = msgEl.querySelector('.msg-dropdown');
    if (!menu) return;

    document.querySelectorAll('.msg-dropdown').forEach(m => {
        if (m !== menu) m.style.display = 'none';
    });

    const wasOpen = menu.style.display === 'flex';
    if (wasOpen) {
        menu.style.display = 'none';
        return;
    }

    menu.style.display = 'flex';
    menu.style.visibility = 'hidden';
    menu.style.position = 'fixed';
    menu.style.left = '0px';
    menu.style.top = '0px';
    menu.style.right = 'auto';
    menu.style.bottom = 'auto';

    const rect = menu.getBoundingClientRect();
    const vw = window.innerWidth || document.documentElement.clientWidth || 0;
    const vh = window.innerHeight || document.documentElement.clientHeight || 0;
    const pad = 8;

    let x = Number(clientX || 0);
    let y = Number(clientY || 0);
    if (!Number.isFinite(x) || x <= 0) x = pad;
    if (!Number.isFinite(y) || y <= 0) y = pad;
    if (x + rect.width + pad > vw) x = Math.max(pad, vw - rect.width - pad);
    if (y + rect.height + pad > vh) y = Math.max(pad, vh - rect.height - pad);

    menu.style.left = `${x}px`;
    menu.style.top = `${y}px`;
    menu.style.visibility = 'visible';
}

document.addEventListener('click', (e) => {
    const t = e.target;
    const anyOpen = !!document.querySelector('.msg-dropdown[style*="display: flex"], .msg-dropdown[style*="display:flex"]');
    if (anyOpen && (isMobile() || navigator.maxTouchPoints > 0)) {
        if (!(t instanceof Element) || !t.closest('.msg-dropdown')) {
            document.querySelectorAll('.msg-dropdown').forEach(d => d.style.display = 'none');
            return;
        }
    }
    if (t instanceof Element) {
        if (t.closest('.msg-dropdown')) return;
        if (t.closest('.msg')) return;
    }
    document.querySelectorAll('.msg-dropdown').forEach(d => d.style.display = 'none');
});

function initMobileTapMessageMenu() {
    const messagesEl = document.getElementById('messages');
    if (!messagesEl) return;

    const isInteractive = (target) => {
        if (!(target instanceof Element)) return false;
        return !!target.closest('.msg-dropdown, .reply-content, .file-wrapper, .voice-play-btn, .voice-slider, .video-note-slider, .video-note-progress, a, button, input, textarea, video, audio');
    };

    const nearestMsgByY = (clientY) => {
        const list = Array.from(messagesEl.querySelectorAll('.msg:not(.msg-system-event)'));
        let best = null;
        let bestDist = Number.POSITIVE_INFINITY;
        for (const el of list) {
            const r = el.getBoundingClientRect();
            const centerY = r.top + r.height / 2;
            const dist = Math.abs(centerY - clientY);
            if (dist < bestDist) {
                bestDist = dist;
                best = el;
            }
        }
        return bestDist <= 56 ? best : null;
    };

    messagesEl.addEventListener('click', (e) => {
        if (!(isMobile() || navigator.maxTouchPoints > 0)) return;
        const t = e.target;
        if (isInteractive(t)) return;
        const opened = Array.from(document.querySelectorAll('.msg-dropdown')).some((el) => el.style.display === 'flex');
        if (opened) {
            document.querySelectorAll('.msg-dropdown').forEach(d => d.style.display = 'none');
            e.preventDefault();
            e.stopPropagation();
            return;
        }

        let msgEl = (t instanceof Element) ? t.closest('.msg:not(.msg-system-event)') : null;
        if (!msgEl) {
            const y = ('clientY' in e) ? e.clientY : 0;
            msgEl = nearestMsgByY(y);
        }
        if (!msgEl) return;

        e.preventDefault();
        e.stopPropagation();
        const r = msgEl.getBoundingClientRect();
        openMsgMenuAtPointer(msgEl, Math.max(8, r.right - 10), Math.max(8, r.top + 12));
    }, { passive: false });
}

window.prepareReply = (msgId, sender, btn) => {
    editMsgId = null; 
    window.editMsgId = null;
    replyId = msgId;
    window.replyId = msgId;
    
    const msgElement = btn.closest('.msg');
    const msgText = getMessagePreviewText(msgElement);
    const kindLabel = getReplyKindLabel(msgElement);
    const replyTitle = kindLabel === 'сообщение' ? 'Ответ на сообщение' : `Ответ на ${kindLabel}`;
    
    const preview = document.getElementById("reply-preview");
    document.getElementById("reply-text").innerHTML = `<b>${replyTitle}:</b> ${msgText}`;
    preview.style.display = "block";
    document.getElementById("messageInput").focus();
};

window.prepareEdit = (msgId, btn) => {
    replyId = null;
    editMsgId = msgId;
    const msgEl = btn.closest('.msg');
    let msgText = '';
    if (msgEl && typeof msgEl._rawContent === 'string') {
        msgText = msgEl._rawContent;
    } else {
        msgText = msgEl?.querySelector('.msg-text')?.innerText || '';
    }
    const safePreview = String(msgText).replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const preview = document.getElementById("reply-preview");
    document.getElementById("reply-text").innerHTML = `<b>Редактирование:</b> ${safePreview}`;
    preview.style.display = "block";
    document.getElementById("messageInput").value = msgText;
    document.getElementById("messageInput").focus();
    updateComposerButtons();
};

window.cancelReply = () => {
    replyId = null; 
    editMsgId = null;
    window.replyId = null;
    window.editMsgId = null;
    document.getElementById("reply-preview").style.display = "none";
    document.getElementById("messageInput").value = "";
    updateComposerButtons();
};

window.deleteMessage = async (btn, messageId) => {
    // 1. Закрываем выпадающее меню сразу
    const dropdown = btn.closest('.msg-dropdown');
    if (dropdown) dropdown.style.display = 'none';

    // 2. Берем ID чата из твоей переменной
    const chat_id = window.currentChat; 

    if (!chat_id) {
        showToast("Ошибка: не удалось определить чат", "error");
        console.error("currentChat is empty. Make sure openChat sets window.currentChat");
        return;
    }

    // 3. Твоя кастомная модалка (ждем ответа через await)
    const confirmed = await showCustomModal(
        "Удаление", 
        "Вы уверены, что хотите удалить это сообщение для всех?", 
        false, 
        true
    );

    // В твоей функции showCustomModal при нажатии "ОК" возвращается true
    if (confirmed === true) {
        console.log(`[Socket] Удаляем сообщение ${messageId} в чате ${chat_id}`);
        socket.emit('delete_message', {
            id: messageId,
            chat_id: chat_id
        });
    }
};


function _resolveMessageElementById(rawId) {
    const id = String(rawId || '').trim();
    if (!id) return null;
    let target = document.querySelector(`.msg[data-id="${id}"]`);
    if (target) return target;
    if (!id.startsWith('msg_')) {
        target = document.querySelector(`.msg[data-id="msg_${id}"]`);
        if (target) return target;
    } else {
        const shortId = id.slice(4);
        if (shortId) {
            target = document.querySelector(`.msg[data-id="${shortId}"]`);
            if (target) return target;
        }
    }
    const all = Array.from(document.querySelectorAll('.msg[data-id]'));
    return all.find((el) => {
        const cur = String(el.getAttribute('data-id') || '');
        return cur.endsWith(id) || id.endsWith(cur);
    }) || null;
}

window.scrollToMessage = (id) => {
    const target = _resolveMessageElementById(id);
    if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "center" });
        target.classList.remove('highlight-pulse');
        setTimeout(() => target.classList.add('highlight-pulse'), 20);
        setTimeout(() => target.classList.remove('highlight-pulse'), 1450);
    } else {
        showToast("Сообщение не найдено в текущей истории");
    }
};

const msgDiv = document.getElementById("messages");
const badge = document.getElementById("newMsgBadge");
const scrollBtn = document.getElementById("scrollDownBtn");

// Следим за скроллом
msgDiv.onscroll = () => {
    const isAtBottom = msgDiv.scrollHeight - msgDiv.scrollTop <= msgDiv.clientHeight + 100;
    if (isAtBottom) {
        badge.style.display = "none";
        scrollBtn.style.display = "none";
    } else {
        scrollBtn.style.display = "flex";
    }
};

window.scrollChatToBottom = () => {
    msgDiv.scrollTo({ top: msgDiv.scrollHeight, behavior: 'smooth' });
    badge.style.display = "none";
};

// В конце функции addMessageToScreen:
function handleScrollLogic(isMe) {
    const isAtBottom = msgDiv.scrollHeight - msgDiv.scrollTop <= msgDiv.clientHeight + 150;

    if (isMe) {
        // Если отправил я — всегда скроллим вниз
        scrollChatToBottom();
    } else {
        // Если отправил другой
        if (isAtBottom) {
            scrollChatToBottom();
        } else {
            // Показываем плашку, если пользователь листал историю
            badge.style.display = "block";
        }
    }
}

async function persistPrivateKeyBeforeLogout() {
    const u = String(localStorage.getItem("username") || "").toLowerCase();
    if (!u) return true;

    let password = localStorage.getItem(PASSWORD_CACHE_KEY(u)) || "";
    if (!password) {
        const input = await showCustomModal(
            "Сохранение ключа",
            "Введите пароль, чтобы зашифровать приватный ключ перед выходом:",
            true,
            true
        );
        password = String(input || "").trim();
        if (!password) return false;
    }

    try {
        const keys = await getMyPersistentKeys();
        if (!keys?.privateKey) return true;
        const privJwk = await cryptoMod.exportECDHPrivateKey(keys.privateKey);
        const encryptedPrivateKey = await cryptoMod.encryptWithPassword(password, JSON.stringify(privJwk));
        await saveKeyToDB(u, encryptedPrivateKey);
        localStorage.setItem(PASSWORD_CACHE_KEY(u), password);
        // На выходе всегда сохраняем зашифрованный приватный ключ на сервере
        // (users.json -> enc_priv_key), чтобы не потерять аккаунт после последнего выхода.
        try {
            await fetch("/backup_key", {
                method: "POST",
                headers: authJsonHeaders(),
                body: JSON.stringify({
                    username: u,
                    key: encryptedPrivateKey,
                    initiator: sessionId
                })
            });
        } catch (e) {
            logSilent("persistPrivateKeyBeforeLogout.backup_key", e);
        }
        return true;
    } catch (e) {
        console.error("Ошибка сохранения приватного ключа перед выходом:", e);
        return false;
    }
}

window.logout = async () => {
    if (!(await showCustomModal("Выход", UI_TEXT.LOGOUT_CONFIRM, false))) return;
    const ok = await persistPrivateKeyBeforeLogout();
    if (!ok) {
        showToast("Выход отменен: ключ не сохранен", "error");
        return;
    }
    clearAuthSession(localStorage.getItem("username"));
    window.location.replace(ROUTES.INFO);
};

function showCustomModal(title, text, showInput = false, showCancel = true) {
    return new Promise((resolve) => {
        const modal = document.getElementById("customModal");
        const titleEl = document.getElementById("modalTitle");
        const textEl = document.getElementById("modalText");
        const input = document.getElementById("modalInput");
        const confirmBtn = document.getElementById("modalConfirm");
        const cancelBtn = document.getElementById("modalCancel");
        
        if (!modal) {
            resolve(null);
            return;
        }
        
        titleEl.textContent = title;
        textEl.textContent = text;
        
        input.style.display = showInput ? "block" : "none";
        input.value = "";
        
        cancelBtn.style.display = showCancel ? "inline-block" : "none";
        
        // Показываем модальное окно
        modal.style.display = "flex";
        
        // Убираем все старые обработчики
        confirmBtn.replaceWith(confirmBtn.cloneNode(true));
        cancelBtn.replaceWith(cancelBtn.cloneNode(true));
        
        // Получаем новые элементы после замены
        const newConfirmBtn = document.getElementById("modalConfirm");
        const newCancelBtn = document.getElementById("modalCancel");
        
        // Добавляем обработчики
        newConfirmBtn.addEventListener('click', () => {
            modal.style.display = "none";
            resolve(showInput ? input.value.trim() : true);
        });
        
        newCancelBtn.addEventListener('click', () => {
            modal.style.display = "none";
            resolve(false);
        });
        
        // Фокус на инпут
        if (showInput) {
            setTimeout(() => input.focus(), 100);
        }
    });
}

async function promptModal(title, text) {
    const val = await showCustomModal(title, text, true, true);
    if (val === false || val === null) return "";
    return String(val).trim();
}

async function confirmModal(title, text) {
    const ok = await showCustomModal(title, text, false, true);
    return !!ok;
}

async function updateBackupButtonState() {
    if (!username) return;
    try {
        const res = await fetch(`/check_backup/${username}`);
        const data = await res.json();
        const btn = document.querySelector(".btn-backup");
        if (!btn) return;

        // data.has_backup на сервере должен проверять наличие "enc_priv_key"
        if (data.has_backup) {
            btn.innerText = "🔒 Запретить вход с других устройств";
            btn.style.background = "#ef4444"; 
            btn.onclick = disableMultiDevice;
        } else {
            btn.innerText = "🔓 Разрешить вход с других устройств";
            btn.style.background = ""; // Сброс к дефолтному синему
            btn.onclick = enableMultiDevice;
        }
    } catch (e) { console.error("Ошибка синхронизации кнопки", e); }
}

// Обновленная функция отключения
window.disableMultiDevice = async function() {
    const password = await showCustomModal("ОПАСНО", "Введите пароль для отключения облачного ключа и кика всех устройств:", true);
    if (!password) return;

    const passHash = await sha256(password);
    try {
        const res = await fetch("/disable_backup", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ 
                username: username, 
                password: passHash, 
                initiator: sessionId // Отправляем наш ID
            })
        });

        if (!res.ok) showToast("Неверный пароль", "error");
        // Сама кнопка обновится через сокет (force_logout_others) выше
    } catch (e) { showToast("Ошибка сервера", "error"); }
};

// Исправляем также функцию включения (backup_key)
window.enableMultiDevice = async function() {
    const password = await showCustomModal("Включение входа", "Введите пароль:", true);
    if (!password) return;
    const passHash = await sha256(password);

    try {
        const localKey = await getKeyFromDB(username);
        const res = await fetch("/backup_key", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ 
                username: username, 
                password: passHash,
                enc_priv_key: localKey // Имя поля теперь совпадает с твоим
            })
        });
        
        if ((await res.json()).status === "ok") {
            showToast("Вход разрешен");
            updateBackupButtonState();
        } else {
            showToast("Ошибка пароля", "error");
        }
    } catch (e) { showToast("Ошибка", "error"); }
};


/**
 * ==========================================
 * 12. ЛОГИКА ВЛОЖЕНИЙ (ATTACHMENTS)
 * ==========================================
 */

// Управление меню скрепки
function initAttachmentMenu() {
    const attachBtn = document.getElementById('attachBtn');
    const attachMenu = document.getElementById('attachMenu');

    if (!attachBtn || !attachMenu) return;

    attachBtn.onclick = (e) => {
        e.stopPropagation();
        attachMenu.classList.toggle('active');
    };

    // Закрытие при клике по любому месту экрана
    document.addEventListener('click', () => {
        attachMenu.classList.remove('active');
    });
}

// Вызов скрытого инпута
window.triggerFileInput = (accept) => {
    const input = document.getElementById('fileInput');
    if (input) {
        input.accept = accept;
        input.value = ''; // Сброс, чтобы можно было выбрать тот же файл дважды
        input.click();
    }
};

// Обработка выбора файла
window.handleFileSelect = async (event) => {
    const file = event.target.files[0];
    if (!file) return;
    if (String(window.currentChat || '').startsWith('group_')) {
        const canMedia = await canSendByGroupPermission(window.currentChat, 'can_send_media');
        if (!canMedia) {
            showToast('В этой группе вам запрещено отправлять медиа', 'error');
            event.target.value = "";
            return;
        }
    }

    try {
        showToast("Шифрование и подготовка...");

        // 1. Генерируем ключ для файла
        const fileKey = await window.crypto.subtle.generateKey(
            { name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]
        );
        const iv = window.crypto.getRandomValues(new Uint8Array(12));
        const fileBuffer = await file.arrayBuffer();

        // 2. Шифруем контент файла
        const encryptedContent = await window.crypto.subtle.encrypt(
            { name: "AES-GCM", iv: iv }, fileKey, fileBuffer
        );

        // 3. Формируем бинарный пакет (IV + данные)
        const combined = new Uint8Array(iv.length + encryptedContent.byteLength);
        combined.set(iv);
        combined.set(new Uint8Array(encryptedContent), iv.length);

        // 4. Отправляем файл на сервер (в папку uploads)
        const formData = new FormData();
        formData.append('file', new Blob([combined], { type: file.type || 'application/octet-stream' }), file.name || `upload_${Date.now()}`);
        formData.append('type', file.type.startsWith('image/') ? 'images' : 'files');

        console.log("Отправка файла на /upload...");
        const res = await fetch('/upload', { method: 'POST', body: formData });
        let uploadResult = {};
        try { uploadResult = await res.json(); } catch { uploadResult = {}; }
        if (!res.ok) {
            const msg = uploadResult?.error || `upload_http_${res.status}`;
            throw new Error(msg);
        }

        if (uploadResult.status === 'ok') {
            console.log("Файл загружен, URL:", uploadResult.url);

            // 5. Экспортируем ключ файла, чтобы собеседник мог его расшифровать
            const exportedKey = await window.crypto.subtle.exportKey("raw", fileKey);
            const keyB64 = btoa(String.fromCharCode(...new Uint8Array(exportedKey)));

            // 6. СОЗДАЕМ МЕТАДАННЫЕ (Это и есть наше сообщение)
            const fileMeta = {
                url: uploadResult.url,
                name: file.name,
                file_key: keyB64,
                type: file.type.startsWith('image/') ? 'image' : file.type.startsWith('video/') ? 'video' : 'file',
                mime: file.type
            };

            // Префикс __FILE__ обязателен, чтобы addMessageToScreen понял, что это файл
            const fileMessageText = "__FILE__" + JSON.stringify(fileMeta);

            // 7. Шифруем это сообщение ключом чата
            const chatAES = window.currentAES || sessionAESKeys[window.currentChat];
            if (!chatAES) {
                showToast("Ошибка: ключ чата не найден", "error");
                return;
            }

            const encryptedCipher = await cryptoMod.encrypt(chatAES, fileMessageText);

            // 8. КРИТИЧЕСКИЙ ШАГ: Отправляем через сокет. 
            // Именно это событие заставит сервер сделать запись в messages.json!
            console.log("Отправка сокет-события send_message для записи в историю...");
            socket.emit("send_message", {
                from: username,
                to: window.currentChat,
                cipher: encryptedCipher,
                type: "file",
                media_kind: file.type.startsWith('image/') ? 'image' : file.type.startsWith('video/') ? 'video' : 'file',
                reply_to: window.replyId || null
            });

            if (window.cancelReply) window.cancelReply();
            showToast("Файл успешно отправлен и сохранен");
        }
    } catch (e) {
        console.error("Критическая ошибка в handleFileSelect:", e);
        showToast("Ошибка при отправке", "error");
    } finally {
        event.target.value = ""; // Очистка
    }
};

/**
 * ШИФРОВАНИЕ И ЗАГРУЗКА ФАЙЛА
 */
async function uploadFileE2E(file) {
    if (!currentChat || !currentAES) { showToast("Откройте чат", "error"); return; }
    if (String(currentChat).startsWith('group_')) {
        const canMedia = await canSendByGroupPermission(currentChat, 'can_send_media');
        if (!canMedia) { showToast('В этой группе вам запрещено отправлять медиа', 'error'); return; }
    }
    const tempId = "pending_" + Date.now();
    const pendingEl = addPendingFile(file, tempId);
    try {
        const fileKey = await window.crypto.subtle.generateKey(
            { name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]
        );
        const iv = window.crypto.getRandomValues(new Uint8Array(12));
        const encryptedContent = await window.crypto.subtle.encrypt(
            { name: "AES-GCM", iv: iv }, fileKey, await file.arrayBuffer()
        );
        const combined = new Uint8Array(iv.length + encryptedContent.byteLength);
        combined.set(iv);
        combined.set(new Uint8Array(encryptedContent), iv.length);

        const formData = new FormData();
        formData.append('file', new Blob([combined], { type: file.type || 'application/octet-stream' }), file.name || `upload_${Date.now()}`);
        formData.append('type', file.type.startsWith('image/') ? 'images' : 'files');

        const json = await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', '/upload');
            xhr.upload.onprogress = (e) => {
                if (e.lengthComputable) {
                    const fill = document.getElementById(`upfill_${tempId}`);
                    if (fill) fill.style.width = Math.round(e.loaded / e.total * 100) + '%';
                }
            };
            xhr.onload = () => {
                try {
                    const parsed = JSON.parse(xhr.responseText || '{}');
                    if (xhr.status < 200 || xhr.status >= 300) {
                        reject(new Error(parsed?.error || `upload_http_${xhr.status}`));
                        return;
                    }
                    resolve(parsed);
                } catch {
                    reject(new Error(`upload_http_${xhr.status}`));
                }
            };
            xhr.onerror = reject;
            xhr.send(formData);
        });

        if (json.status === 'ok') {
            const exportedKey = await window.crypto.subtle.exportKey("raw", fileKey);
            const keyB64 = btoa(String.fromCharCode(...new Uint8Array(exportedKey)));
            let fileType = file.type.startsWith('image/') ? 'image' : file.type.startsWith('video/') ? 'video' : 'file';
            const finalMessage = "__FILE__" + JSON.stringify({
                url: json.url, name: file.name, file_key: keyB64, type: fileType, mime: file.type
            });
            const encryptedCipher = await cryptoMod.encrypt(currentAES || sessionAESKeys[currentChat], finalMessage);
            if (pendingEl) pendingEl.remove();
            socket.emit("send_message", { from: username, to: currentChat, cipher: encryptedCipher, type: "file", media_kind: fileType });
        } else {
            if (pendingEl) pendingEl.remove();
            showToast("Ошибка загрузки", "error");
        }
    } catch(e) {
        if (pendingEl) pendingEl.remove();
        showToast("Ошибка отправки файла", "error");
    }
}

async function uploadEncryptedBlobAndMeta(blob, fileName, mimeType, extraMeta = {}) {
    const fileKey = await window.crypto.subtle.generateKey(
        { name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]
    );
    const iv = window.crypto.getRandomValues(new Uint8Array(12));
    const encryptedContent = await window.crypto.subtle.encrypt(
        { name: "AES-GCM", iv },
        fileKey,
        await blob.arrayBuffer()
    );
    const combined = new Uint8Array(iv.length + encryptedContent.byteLength);
    combined.set(iv);
    combined.set(new Uint8Array(encryptedContent), iv.length);
    const formData = new FormData();
    const folder = mimeType.startsWith('image/') ? 'images' : 'files';
    formData.append('file', new Blob([combined], { type: mimeType || 'application/octet-stream' }), fileName || `upload_${Date.now()}`);
    formData.append('type', folder);
    const res = await fetch('/upload', { method: 'POST', body: formData });
    let uploaded = {};
    try { uploaded = await res.json(); } catch { uploaded = {}; }
    if (!res.ok) throw new Error(uploaded?.error || `upload_http_${res.status}`);
    if (uploaded.status !== 'ok') throw new Error('upload_failed');
    const exportedKey = await window.crypto.subtle.exportKey("raw", fileKey);
    const keyB64 = btoa(String.fromCharCode(...new Uint8Array(exportedKey)));
    return {
        url: uploaded.url,
        name: fileName,
        file_key: keyB64,
        type: mimeType.startsWith('image/') ? 'image' : mimeType.startsWith('video/') ? 'video' : 'file',
        mime: mimeType,
        ...extraMeta
    };
}

async function sendEncryptedFileMeta(meta, clientId = "") {
    if (!window.currentChat || !currentAES) { showToast('Откройте чат', 'error'); return; }
    if (String(window.currentChat).startsWith('group_')) {
        const kind = String(meta?.type || '').toLowerCase();
        if (!await canSendByGroupPermission(window.currentChat, 'can_send_media')) { showToast('В этой группе вам запрещено отправлять медиа', 'error'); return; }
        if (kind === 'voice' && !await canSendByGroupPermission(window.currentChat, 'can_send_voice')) { showToast('В этой группе вам запрещены голосовые', 'error'); return; }
        if (kind === 'video_note' && !await canSendByGroupPermission(window.currentChat, 'can_send_video_notes')) { showToast('В этой группе вам запрещены кружки', 'error'); return; }
    }
    const text = "__FILE__" + JSON.stringify(meta);
    const encrypted = await cryptoMod.encrypt(currentAES, text);
    socket.emit("send_message", { from: username, to: window.currentChat, cipher: encrypted, type: "file", media_kind: String(meta?.type || '').toLowerCase(), ...(clientId ? { client_id: clientId } : {}) });
}

async function requestAudioTranscription(blob, lang = 'ru') {
    const fd = new FormData();
    fd.append('audio', blob, `voice_${Date.now()}.webm`);
    fd.append('lang', lang);
    const res = await fetch('/api/transcribe_audio', { method: 'POST', body: fd });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'transcribe_failed');
    return String(data.transcript || '').trim();
}

let _voiceRecorderState = null;
let _videoNoteRecorderState = null;
let _recordHudState = null;
let _videoNoteRecorderUiState = null;
let _activeVideoNoteWrapper = null;

function pickSupportedRecorderMime(candidates = []) {
    if (!window.MediaRecorder) return '';
    for (const type of candidates) {
        try {
            if (type && MediaRecorder.isTypeSupported(type)) return type;
        } catch {}
    }
    return '';
}

function fileExtForMime(mime = '', fallback = 'bin') {
    const m = String(mime || '').toLowerCase();
    if (m.includes('webm')) return 'webm';
    if (m.includes('mp4')) return 'mp4';
    if (m.includes('ogg')) return 'ogg';
    if (m.includes('wav')) return 'wav';
    if (m.includes('mpeg')) return 'mp3';
    return fallback;
}

function showRecordHud(kind = 'voice', onStop = null) {
    hideRecordHud();
    const hud = document.createElement('div');
    hud.id = 'recordHud';
    hud.className = 'record-hud';
    hud.innerHTML = `
      <div class="record-left">
        <div class="record-dot"></div>
        <div class="record-text">${kind === 'video_note' ? 'Запись кружка' : 'Запись голосового'}</div>
      </div>
      <div class="record-time" id="recordHudTime">0:00</div>
      <button class="record-stop-btn" id="recordHudStopBtn" title="Остановить запись">■ Стоп</button>
    `;
    document.body.appendChild(hud);
    const started = Date.now();
    const timer = setInterval(() => {
        const el = document.getElementById('recordHudTime');
        if (!el) return;
        el.textContent = formatMediaTime((Date.now() - started) / 1000);
    }, 250);
    const stopBtn = document.getElementById('recordHudStopBtn');
    if (stopBtn && typeof onStop === 'function') {
        stopBtn.onclick = () => onStop();
    }
    _recordHudState = { timer, started };
}

function hideRecordHud() {
    if (_recordHudState?.timer) clearInterval(_recordHudState.timer);
    _recordHudState = null;
    document.getElementById('recordHud')?.remove();
}

function openVideoNoteRecorderModal(stream, onStop) {
    closeVideoNoteRecorderModal();
    const modal = document.createElement('div');
    modal.id = 'videoNoteRecordModal';
    modal.className = 'video-note-record-modal';
    modal.innerHTML = `
      <div class="video-note-record-card">
        <div class="video-note-record-title">Запись кружка</div>
        <div class="video-note-record-preview-wrap">
          <video id="videoNoteRecordPreview" autoplay muted playsinline></video>
        </div>
        <div class="video-note-record-time" id="videoNoteRecordTime">0:00 / 1:00</div>
        <div class="video-note-record-actions">
          <button id="videoNoteRecordStopBtn" class="video-note-record-stop">■ Завершить</button>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    const preview = document.getElementById('videoNoteRecordPreview');
    if (preview) preview.srcObject = stream;
    const stopBtn = document.getElementById('videoNoteRecordStopBtn');
    if (stopBtn) stopBtn.onclick = () => typeof onStop === 'function' && onStop();
    _videoNoteRecorderUiState = { modal };
}

function updateVideoNoteRecorderTime(seconds) {
    const el = document.getElementById('videoNoteRecordTime');
    if (!el) return;
    el.textContent = `${formatMediaTime(seconds)} / 1:00`;
}

function closeVideoNoteRecorderModal() {
    document.getElementById('videoNoteRecordModal')?.remove();
    _videoNoteRecorderUiState = null;
}

function toggleVideoNoteInline(fileWrapper) {
    if (!fileWrapper) return;
    const noteContainer = fileWrapper.querySelector('.image-container.video-preview-shell');
    const video = fileWrapper.querySelector('video.file-preview-vid');
    if (!noteContainer || !video) return;
    const startTicker = (wrap) => {
        const vd = wrap?.querySelector('video.file-preview-vid');
        const pf = wrap?.querySelector('.video-note-progress-fill');
        const sl = wrap?.querySelector('.video-note-slider');
        const te = wrap?.querySelector('.video-note-time');
        if (!vd) return;
        if (vd._noteRafId) cancelAnimationFrame(vd._noteRafId);
        const tick = () => {
            if (!wrap.classList.contains('note-expanded')) return;
            const dur = Number(vd.duration || 0);
            const cur = Number(vd.currentTime || 0);
            if (te) te.textContent = formatMediaTime(cur);
            if (pf && dur > 0) {
                const pct = Math.max(0, Math.min(100, (cur / dur) * 100));
                pf.style.width = `${pct}%`;
            }
            if (sl && dur > 0) {
                sl.value = String(Math.max(0, Math.min(1000, Math.round((cur / dur) * 1000))));
            }
            if (!vd.paused && !vd.ended) {
                vd._noteRafId = requestAnimationFrame(tick);
            }
        };
        vd._noteRafId = requestAnimationFrame(tick);
    };
    const stopTicker = (wrap) => {
        const vd = wrap?.querySelector('video.file-preview-vid');
        if (vd?._noteRafId) cancelAnimationFrame(vd._noteRafId);
        if (vd) vd._noteRafId = 0;
    };
    const collapse = (wrap) => {
        if (!wrap) return;
        const nc = wrap.querySelector('.image-container.video-preview-shell');
        const vd = wrap.querySelector('video.file-preview-vid');
        const pf = wrap.querySelector('.video-note-progress-fill');
        const sl = wrap.querySelector('.video-note-slider');
        const te = wrap.querySelector('.video-note-time');
        nc?.classList.remove('video-note-expanded');
        wrap.classList.remove('note-expanded');
        stopTicker(wrap);
        if (vd) {
            vd.pause();
            vd.currentTime = 0;
            vd.muted = true;
        }
        if (pf) pf.style.width = '0%';
        if (sl) sl.value = '0';
        if (te && vd) te.textContent = formatMediaTime(vd.duration || 0);
    };
    if (_activeVideoNoteWrapper && _activeVideoNoteWrapper !== fileWrapper) {
        collapse(_activeVideoNoteWrapper);
    }
    const expanded = noteContainer.classList.toggle('video-note-expanded');
    fileWrapper.classList.toggle('note-expanded', expanded);
    if (expanded) {
        video.muted = false;
        video.volume = 1;
        hardenVideoElementUi(video);
        if (!video.dataset.noteTickerBound) {
            video.addEventListener('pause', () => stopTicker(fileWrapper));
            video.addEventListener('play', () => startTicker(fileWrapper));
            video.addEventListener('ended', () => stopTicker(fileWrapper));
            video.dataset.noteTickerBound = '1';
        }
        video.play().then(() => {
            startTicker(fileWrapper);
        }).catch(() => {
            showToast('Не удалось воспроизвести кружок на этом устройстве', 'error');
            collapse(fileWrapper);
        });
        _activeVideoNoteWrapper = fileWrapper;
    } else {
        collapse(fileWrapper);
        if (_activeVideoNoteWrapper === fileWrapper) _activeVideoNoteWrapper = null;
    }
}

window.recordVoiceMessage = async () => {
    if (_videoNoteRecorderState?.rec?.state === 'recording') {
        showToast('Сначала завершите запись кружка', 'error');
        return;
    }
    if (_voiceRecorderState?.rec?.state === 'recording') {
        _voiceRecorderState.rec.stop();
        return;
    }
    try {
        const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
        const voiceMime = pickSupportedRecorderMime([
            'audio/webm;codecs=opus',
            'audio/webm',
            'audio/mp4',
            'audio/ogg;codecs=opus',
            'audio/ogg'
        ]);
        const rec = new MediaRecorder(stream, voiceMime ? { mimeType: voiceMime } : undefined);
        const chunks = [];
        let transcript = '';
        const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
        let speechRec = null;
        if (SR) {
            speechRec = new SR();
            speechRec.lang = 'ru-RU';
            speechRec.continuous = true;
            speechRec.interimResults = true;
            speechRec.onresult = (ev) => {
                let acc = '';
                for (let i = 0; i < ev.results.length; i++) acc += ev.results[i][0]?.transcript || '';
                transcript = acc.trim();
            };
            try { speechRec.start(); } catch {}
        }
        rec.ondataavailable = (e) => { if (e.data?.size) chunks.push(e.data); };
        rec.onstop = async () => {
            stream.getTracks().forEach((t) => t.stop());
            try { speechRec?.stop(); } catch {}
            if (_voiceRecorderState?.autoStopTimer) clearTimeout(_voiceRecorderState.autoStopTimer);
            hideRecordHud();
            const blobMime = rec.mimeType || voiceMime || 'audio/webm';
            const blob = new Blob(chunks, { type: blobMime });
            if (blob.size < 800) return;
            if (!transcript) {
                try {
                    transcript = await requestAudioTranscription(blob, 'ru');
                } catch {
                    transcript = '';
                }
            }
            const pendingId = `voice_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
            addPendingVoiceMessage(pendingId);
            try {
                const ext = fileExtForMime(blobMime, 'webm');
                const meta = await uploadEncryptedBlobAndMeta(blob, `voice_${Date.now()}.${ext}`, blobMime, { type: 'voice', transcript: transcript.slice(0, 1200) });
                await sendEncryptedFileMeta(meta, pendingId);
                showToast('Голосовое отправлено');
            } catch {
                document.querySelector(`[data-pending-id="${pendingId}"]`)?.remove();
                showToast('Ошибка отправки голосового', 'error');
            }
            _voiceRecorderState = null;
        };
        rec.start(220);
        const autoStopTimer = setTimeout(() => {
            if (_voiceRecorderState?.rec === rec && rec.state === 'recording') rec.stop();
        }, 120000);
        _voiceRecorderState = { rec, autoStopTimer };
        showRecordHud('voice', () => {
            if (rec.state === 'recording') rec.stop();
        });
    } catch {
        hideRecordHud();
        showToast('Нет доступа к микрофону', 'error');
    }
};

window.recordVideoNote = async () => {
    if (_voiceRecorderState?.rec?.state === 'recording') {
        showToast('Сначала завершите запись голосового', 'error');
        return;
    }
    if (_videoNoteRecorderState?.rec?.state === 'recording') {
        _videoNoteRecorderState.rec.stop();
        return;
    }
    try {
        const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: { width: 480, height: 480 } });
        const noteMime = pickSupportedRecorderMime([
            'video/webm;codecs=vp9,opus',
            'video/webm;codecs=vp8,opus',
            'video/webm',
            'video/mp4'
        ]);
        const rec = new MediaRecorder(stream, noteMime ? { mimeType: noteMime } : undefined);
        const chunks = [];
        rec.ondataavailable = (e) => { if (e.data?.size) chunks.push(e.data); };
        rec.onstop = async () => {
            stream.getTracks().forEach((t) => t.stop());
            if (_videoNoteRecorderState?.timeTicker) clearInterval(_videoNoteRecorderState.timeTicker);
            if (_videoNoteRecorderState?.autoStopTimer) clearTimeout(_videoNoteRecorderState.autoStopTimer);
            closeVideoNoteRecorderModal();
            const blobMime = rec.mimeType || noteMime || 'video/webm';
            const blob = new Blob(chunks, { type: blobMime });
            if (blob.size < 2000) return;
            const pendingId = `vnote_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
            addPendingVideoNoteMessage(pendingId);
            try {
                const ext = fileExtForMime(blobMime, 'webm');
                const meta = await uploadEncryptedBlobAndMeta(blob, `circle_${Date.now()}.${ext}`, blobMime, { type: 'video_note' });
                await sendEncryptedFileMeta(meta, pendingId);
                showToast('Кружок отправлен');
            } catch {
                document.querySelector(`[data-pending-id="${pendingId}"]`)?.remove();
                showToast('Ошибка отправки кружка', 'error');
            }
            _videoNoteRecorderState = null;
        };
        rec.start(280);
        openVideoNoteRecorderModal(stream, () => {
            if (rec.state === 'recording') rec.stop();
        });
        const started = Date.now();
        const timeTicker = setInterval(() => {
            updateVideoNoteRecorderTime((Date.now() - started) / 1000);
        }, 250);
        const autoStopTimer = setTimeout(() => {
            if (_videoNoteRecorderState?.rec === rec && rec.state === 'recording') rec.stop();
        }, 60000);
        _videoNoteRecorderState = { rec, timeTicker, autoStopTimer };
    } catch {
        closeVideoNoteRecorderModal();
        showToast('Нет доступа к камере/микрофону', 'error');
    }
};


/**
 * ==========================================
 * 13. РАСШИФРОВКА И ОТОБРАЖЕНИЕ ФАЙЛОВ
 * ==========================================
 */

// Явно делаем функции глобальными для onclick
// ВАЖНО: Привязываем к window, так как вызываем из onclick в HTML-строке
window.decryptAndDownload = async (url, fileKeyB64, fileName) => {
    try {
        console.log("Запуск расшифровки для:", fileName);

        const blob = await getDecryptedMediaBlob(url, fileKeyB64, "application/octet-stream");
        const blobUrl = URL.createObjectURL(blob);
        
        const link = document.createElement("a");
        link.href = blobUrl;
        link.download = fileName;
        document.body.appendChild(link);
        link.click();
        
        // Чистим за собой
        document.body.removeChild(link);
        URL.revokeObjectURL(blobUrl);
        
        console.log("Файл успешно сохранен");
    } catch (e) {
        console.error("Ошибка расшифровки:", e);
        showToast("Не удалось расшифровать файл. Либо он поврежден, либо ключ неверен.", "error");
    }
};



// Функция для "мягкой" расшифровки превью (чтобы показать блюр)
window.decryptToImage = async (url, fileKeyB64, imgElementId) => {
    const imgElement = document.getElementById(imgElementId);
    if (!imgElement) return;

    try {
        // 1. Скачиваем зашифрованный файл
        const res = await fetch(url);
        if (!res.ok) throw new Error("Файл не найден на сервере");
        const arrayBuffer = await res.arrayBuffer();
        const data = new Uint8Array(arrayBuffer);

        // 2. Извлекаем IV (первые 12 байт) и само тело файла
        const iv = data.slice(0, 12);
        const encryptedData = data.slice(12);

        // 3. Импортируем ключ файла
        const rawKey = new Uint8Array(atob(fileKeyB64).split("").map(c => c.charCodeAt(0)));
        const fileKey = await window.crypto.subtle.importKey(
            "raw", rawKey, { name: "AES-GCM" }, false, ["decrypt"]
        );

        // 4. Расшифровываем
        const decrypted = await window.crypto.subtle.decrypt(
            { name: "AES-GCM", iv: iv }, fileKey, encryptedData
        );

        // 5. Создаем Blob и вставляем в <img>
        const blobUrl = URL.createObjectURL(new Blob([decrypted]));
        imgElement.src = blobUrl;
        imgElement.style.filter = "none"; // Убираем блюр
    } catch (e) {
        console.error("Ошибка дешифровки изображения:", e);
        imgElement.src = "/static/error-image.png"; // Можно поставить иконку ошибки
    }
};

window.openImageViewer = async (url, fileKeyB64, fileName) => {
    const viewer = document.getElementById('imageViewer');
    const fullImg = document.getElementById('fullImage');
    const nameLabel = document.getElementById('viewerFileName');
    const downloadBtn = document.getElementById('viewerDownloadBtn');

    viewer.style.display = 'flex';
    nameLabel.innerText = fileName;
    fullImg.src = ""; // Очищаем старое

    try {
        const blob = await getDecryptedMediaBlob(url, fileKeyB64, "image/jpeg");
        const blobUrl = URL.createObjectURL(blob);
        fullImg.src = blobUrl;

        // Настраиваем кнопку скачивания внутри просмотрщика
        downloadBtn.onclick = () => {
            const link = document.createElement('a');
            link.href = blobUrl;
            link.download = fileName;
            link.click();
        };
    } catch (e) {
        showToast("Ошибка при открытии фото", "error");
    }
};

window.closeImageViewer = () => {
    document.getElementById('imageViewer').style.display = 'none';
    document.getElementById('fullImage').src = "";
};

/**
 * ==================
 * ЛОГИКА ЧАТОВ
 * ==================
 */

let selectedGroupUsers = []; 

// Открытие модального окна
window.openCreateGroupModal = () => {
    const modal = document.getElementById('groupModal');
    if (modal) {
        modal.style.display = 'flex';
        selectedGroupUsers = [];
        const nameInput = document.getElementById('groupName');
        const area = document.getElementById('selectedUsersArea');
        if (nameInput) nameInput.value = '';
        if (area) area.innerHTML = '';
        // Показываем контакты сразу при открытии
        setTimeout(() => window.searchForGroup(), 50);
    }
};

// Закрытие модального окна
window.closeGroupModal = () => {
    const modal = document.getElementById('groupModal');
    if (modal) {
        modal.style.display = 'none';
        document.getElementById('groupUserSearch').value = "";
    }
};

function renderSelectedBadges() {
    const area = document.getElementById('selectedUsersArea');
    area.innerHTML = '';
    selectedGroupUsers.forEach(user => {
        const badge = document.createElement('div');
        // Стилизуем под твой дизайн
        badge.style = "display:inline-flex; align-items:center; background:var(--accent); color:white; padding:4px 12px; border-radius:20px; margin:4px; font-size:14px;";
        badge.innerHTML = `
            ${String(user.display_name || user.username).replace(/</g, '&lt;')}
            <span onclick="window.removeGroupUser('${user.username}')" style="cursor:pointer; margin-left:8px; font-weight:bold; opacity:0.7;">×</span>
        `;
        area.appendChild(badge);
    });
}

// Удаление из списка
window.removeGroupUser = (name) => {
    selectedGroupUsers = selectedGroupUsers.filter(u => u.username !== name);
    renderSelectedBadges();
};

// Делаем функцию доступной для HTML
window.searchForGroup = async () => {
    const query = (document.getElementById("groupUserSearch")?.value || "").trim();
    const list = document.getElementById("groupSearchList");
    if (!list) return;
    list.innerHTML = "";

    try {
        let friends = [];
        let others = [];
        if (!query) {
            // По дефолту — только контакты с которыми был диалог
            const known = (window._allContacts || []).filter(c => !c.is_group);
            friends = known.slice(0, 10);
        } else {
            const res = await fetch(`/search?q=${encodeURIComponent(query)}&me=${encodeURIComponent(username)}`);
            const found = await res.json();
            friends = (Array.isArray(found) ? found : []).filter(r => !r.is_group && (r.is_friend || r.has_chatted)).slice(0, 10);
            others = (Array.isArray(found) ? found : []).filter(r => !r.is_group && !r.is_friend && !r.has_chatted).slice(0, 10);
        }
        const sections = [
            { title: 'Знакомые', data: friends },
            { title: 'Остальные', data: others }
        ];
        sections.forEach((section) => {
            const uniq = new Map();
            section.data.forEach((c) => {
                const u = String(c.username || '').toLowerCase();
                if (!u || u === String(username || '').toLowerCase()) return;
                if (!uniq.has(u)) uniq.set(u, c);
            });
            const data = [...uniq.values()];
            if (!data.length) return;
            const head = document.createElement('div');
            head.className = 'search-category-header';
            head.textContent = section.title;
            list.appendChild(head);
            data.forEach(u => {
            if (selectedGroupUsers?.find(x => (x.username || x) === u.username)) return;
            const item = document.createElement("div");
            item.className = "user-item";
            const dn = getPreferredDisplayName(u, u.username);
            const avatarText = dn.charAt(0).toUpperCase();
            item.innerHTML = `
                <div style="display:flex;align-items:center;gap:10px;width:100%;">
                    <div style="width:34px;height:34px;border-radius:50%;background:linear-gradient(135deg,var(--accent),#8b5cf6);display:flex;align-items:center;justify-content:center;font-size:15px;font-weight:700;color:white;flex-shrink:0;">${u.avatar ? `<img src="${u.avatar}" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">` : avatarText}</div>
                    <div style="flex:1;overflow:hidden;">
                        <div style="font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${dn}</div>
                        <div style="font-size:11px;color:var(--text-dim);">@${u.username}</div>
                    </div>
                </div>
            `;
            item.onclick = () => {
                if (!window.selectedGroupUsers) window.selectedGroupUsers = [];
                selectedGroupUsers.push({ username: u.username, display_name: getPreferredDisplayName(u, u.username) || u.username });
                renderSelectedBadges();
                list.innerHTML = "";
                document.getElementById("groupUserSearch").value = "";
            };
            list.appendChild(item);
            });
        });
    } catch(e) { console.error(e); }
};

window.finalizeGroupCreation = async () => {
    const nameInput = document.getElementById("groupName");
    const name = nameInput.value.trim();
    
    // 1. Валидация
    if (!name) {
        showToast("Введите название группы", "error");
        return;
    }

    if (!selectedGroupUsers || selectedGroupUsers.length === 0) {
        showToast("Выберите хотя бы одного участника", "error");
        return;
    }

    try {
        // 2. Получаем свой приватный ключ из IndexedDB (для ECDH)
        const myKeys = await getMyPersistentKeys();
        if (!myKeys || !myKeys.privateKey) {
            showToast("Ваш приватный ключ не найден. Перевойдите в аккаунт.", "error");
            return;
        }

        // 3. Формируем список участников (выбранные + создатель)
        const allToEncrypt = [...selectedGroupUsers];
        if (!allToEncrypt.some(u => u.username === username)) {
            allToEncrypt.push({ username: username });
        }

        // 4. Генерируем случайный ключ группы (AES-256)
        const rawGroupKey = window.crypto.getRandomValues(new Uint8Array(32));
        const groupKeyB64 = btoa(String.fromCharCode(...rawGroupKey));

        const encryptedKeys = {};

        // 5. Шифруем ключ группы для каждого участника
        for (const member of allToEncrypt) {
            try {
                // Получаем публичный ключ участника с сервера
                const userPubResp = await fetch(`/api/user_pubkey/${member.username}`);
                const userPubData = await userPubResp.json();

                if (userPubData.public_key) {
                    // ВАЖНО: передаем myKeys.privateKey третьим аргументом, 
                    // чтобы cryptoMod не искал его в localStorage
                    encryptedKeys[member.username] = await cryptoMod.encryptGroupKey(
                        userPubData.public_key, 
                        groupKeyB64,
                        myKeys.privateKey
                    );
                } else {
                    console.warn(`У пользователя ${member.username} нет публичного ключа`);
                }
            } catch (err) {
                console.error(`Ошибка шифрования для ${member.username}:`, err);
            }
        }

        // 6. Отправка на сервер
        const response = await fetch("/api/create_group", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                name: name,
                desc: document.getElementById('groupDesc')?.value.trim() || '',
                avatar: window.currentGroupAvatar || '',
                members: allToEncrypt.map(m => m.username),
                owner: username,
                keys: encryptedKeys
            })
        });

        const res = await response.json();
        
        if (res.success) {
            showToast("Группа успешно создана!");
            closeGroupModal();
            
            // Очистка полей и состояния
            nameInput.value = "";
            selectedGroupUsers = [];
            const area = document.getElementById("selectedUsersArea");
            if (area) area.innerHTML = "";

            // Обновляем список чатов в боковой панели
            if (typeof loadMyFriends === 'function') await loadMyFriends();
            if (typeof syncMyContacts === 'function') await syncMyContacts();
        } else {
            showToast("Ошибка сервера: " + res.error, "error");
        }

    } catch (e) {
        console.error("Критическая ошибка создания группы:", e);
        showToast("Ошибка создания: " + e.message, "error");
    }
};

// ------------------------
// Функции настроек профиля
// ------------------------

// Обработка загрузки аватарки
document.addEventListener('DOMContentLoaded', () => {
    const avatarUpload = document.getElementById('avatarUpload');
    const bioTextarea = document.getElementById('settingsBio');
    const bioCounter = document.getElementById('bioCounter');
    
    if (avatarUpload) {
        avatarUpload.addEventListener('change', async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            
            // Проверка типа файла
            if (!file.type.startsWith('image/')) {
                showToast('Выберите изображение', 'error');
                return;
            }
            
            // Проверка размера (макс 5MB для аватарки)
            if (file.size > 5 * 1024 * 1024) {
                showToast('Изображение слишком большое (макс. 5MB)', 'error');
                return;
            }
            
            // Читаем как base64
            const reader = new FileReader();
            reader.onload = (e) => {
                const base64 = e.target.result;
                
                // Показываем превью
                const avatarImg = document.getElementById('avatarImage');
                const avatarLetter = document.getElementById('avatarLetter');
                
                avatarImg.src = base64;
                avatarImg.style.display = 'block';
                avatarLetter.style.display = 'none';
                
                // Сохраняем в переменную для отправки
                window.currentAvatarBase64 = base64;
            };
            reader.readAsDataURL(file);
        });
    }
    
    // Счетчик символов для описания
    if (bioTextarea && bioCounter) {
        bioTextarea.addEventListener('input', () => {
            const length = bioTextarea.value.length;
            bioCounter.textContent = `${length} / 190`;
            
            if (length >= 190) {
                bioCounter.style.color = '#ef4444';
            } else {
                bioCounter.style.color = 'var(--text-dim)';
            }
        });
    }
});

// Открытие настроек БЕЗ запроса пароля
window.openSettings = async () => {
    const modal = document.getElementById('settingsModal');
    if (!modal) return;
    try {
        const res = await fetch(`/api/user_profile/${username}`);
        const profile = await res.json();

        document.getElementById('settingsUsername').value = username;
        document.getElementById('usernamePreview').textContent = `@${username}`;

        // Имя/Фамилия — теперь хранятся открыто
        document.getElementById('settingsFirstName').value = profile.first_name || '';
        document.getElementById('settingsLastName').value = profile.last_name || '';
        document.getElementById('settingsBio').value = profile.bio || '';
        document.getElementById('settingsBirthdate').value = profile.birthdate || '';

        const namePreview = `${profile.first_name || ''} ${profile.last_name || ''}`.trim() || username;
        document.getElementById('displayNamePreview').textContent = namePreview;

        // Аватарка
        const avatarImg = document.getElementById('avatarImage');
        const avatarLetter = document.getElementById('avatarLetter');
        if (profile.avatar) {
            avatarImg.src = profile.avatar;
            avatarImg.style.display = 'block';
            avatarLetter.style.display = 'none';
            window.currentAvatarBase64 = profile.avatar;
        } else {
            avatarImg.style.display = 'none';
            avatarLetter.style.display = 'block';
            avatarLetter.textContent = username.charAt(0).toUpperCase();
            window.currentAvatarBase64 = '';
        }

        // Настройки конфиденциальности
        const privacyRes = await fetch(`/api/privacy_settings/${username}`);
        const privacy = await privacyRes.json();
        const sel = document.getElementById('birthdatePrivacy');
        if (sel && privacy.birthdate_visibility) sel.value = privacy.birthdate_visibility;
        const storySel = document.getElementById('storyPrivacy');
        if (storySel && privacy.story_visibility) storySel.value = privacy.story_visibility;

        modal.style.display = 'flex';
        modal.classList.remove('menu-open');
        switchSettingsTab('profile');
    } catch (err) {
        console.error('Ошибка загрузки профиля:', err);
        showToast('Не удалось загрузить данные профиля', 'error');
    }
};

window.closeSettings = () => {
    const modal = document.getElementById('settingsModal');
    if (modal) {
        modal.classList.remove('menu-open');
        modal.style.display = 'none';
    }
};

window.openHelpCenter = () => {
    const me = encodeURIComponent(username || localStorage.getItem('username') || '');
    window.open(`/help?me=${me}`, '_blank', 'noopener');
};

window.openCommunityForum = () => {
    const me = encodeURIComponent(username || localStorage.getItem('username') || '');
    window.open(`/help?me=${me}&tab=forum`, '_blank', 'noopener');
};

window.toggleSettingsMobileMenu = () => {
    const modal = document.getElementById('settingsModal');
    if (!modal) return;
    modal.classList.toggle('menu-open');
};

window.closeSettingsMobileMenu = () => {
    const modal = document.getElementById('settingsModal');
    if (!modal) return;
    modal.classList.remove('menu-open');
};

// Переключение вкладок
window.switchSettingsTab = (tabName) => {
    // Убираем active со всех кнопок
    document.querySelectorAll('.settings-nav-item').forEach(item => {
        item.classList.remove('active');
    });
    
    // Скрываем все вкладки
    document.querySelectorAll('.settings-tab').forEach(tab => {
        tab.style.display = 'none';
    });
    
    // Показываем нужную
    const targetTab = document.getElementById(`tab-${tabName}`);
    if (targetTab) targetTab.style.display = 'block';
    document.querySelector(`[data-tab="${tabName}"]`)?.classList.add('active');

    const settingsContent = document.querySelector('#settingsModal .settings-content');
    if (settingsContent) settingsContent.scrollTop = 0;
    if (targetTab) targetTab.scrollTop = 0;
};

// Обработчики кликов по навигации
document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.settings-nav-item').forEach(item => {
        item.addEventListener('click', () => {
            const tab = item.dataset.tab;
            if (!tab) return;
            switchSettingsTab(tab);
            if (window.innerWidth <= MOBILE_BREAKPOINT) {
                closeSettingsMobileMenu();
            }
            if (tab === 'sticker-editor') onStickerEditorTabOpen();
            if (tab === 'support')        { /* ничего */ }
            if (tab === 'folders')        renderFolderSettings();
        });
    });
});

// Фокус на input
window.focusInput = (inputId) => {
    document.getElementById(inputId).focus();
};

// // Обновление превью при вводе
// document.addEventListener('DOMContentLoaded', () => {
//     const firstNameInput = document.getElementById('settingsFirstName');
//     const lastNameInput = document.getElementById('settingsLastName');
//     const preview = document.getElementById('displayNamePreview');
    
//     if (firstNameInput && lastNameInput && preview) {
//         const updatePreview = () => {
//             const first = firstNameInput.value.trim();
//             const last = lastNameInput.value.trim();
//             preview.textContent = (first && last) ? `${first} ${last}` : 'Имя Фамилия';
//         };
        
//         firstNameInput.addEventListener('input', updatePreview);
//         lastNameInput.addEventListener('input', updatePreview);
//     }
// });


// Сброс формы
window.resetProfileForm = async () => {
    try {
        const res = await fetch(`/api/user_profile/${username}`);
        const profile = await res.json();
        document.getElementById('settingsFirstName').value = profile.first_name || '';
        document.getElementById('settingsLastName').value = profile.last_name || '';
        document.getElementById('settingsBio').value = profile.bio || '';
        document.getElementById('settingsBirthdate').value = profile.birthdate || '';
        const namePreview = `${profile.first_name || ''} ${profile.last_name || ''}`.trim() || username;
        document.getElementById('displayNamePreview').textContent = namePreview;
    } catch (e) {
        console.error('resetProfileForm error:', e);
    }
};
// Сохранение профиля С запросом пароля для шифрования
window.saveProfileSettings = async () => {
    const firstName = document.getElementById('settingsFirstName').value.trim();
    const lastName = document.getElementById('settingsLastName').value.trim();
    const bio = document.getElementById('settingsBio').value.trim();
    const birthdate = document.getElementById('settingsBirthdate').value;

    // if (!firstName || !lastName) {
    //     showToast('Заполните имя и фамилию', 'error');
    //     return;
    // }

    try {
        const res = await fetch('/api/update_profile', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username,
                first_name: firstName,
                last_name: lastName,
                bio,
                birthdate,
                avatar: window.currentAvatarBase64 || ''
            })
        });
        const result = await res.json();
        if (res.ok) {
            showToast('✅ Профиль обновлен!');
            document.getElementById('displayNamePreview').textContent = `${firstName} ${lastName}`;
            await syncMyContacts();
        } else {
            showToast(result.error || 'Ошибка', 'error');
        }
    } catch (err) {
        showToast('Ошибка сохранения', 'error');
    }
};

window.savePrivacySettings = async () => {
    const birthdateVisibility = document.getElementById('birthdatePrivacy').value;
    const storyVisibility = document.getElementById('storyPrivacy')?.value || 'friends';
    try {
        const res = await fetch('/api/update_privacy', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username,
                birthdate_visibility: birthdateVisibility,
                story_visibility: storyVisibility
            })
        });
        if (res.ok) showToast('✅ Настройки конфиденциальности сохранены!');
        else showToast('Ошибка сохранения', 'error');
    } catch (e) {
        showToast('Ошибка', 'error');
    }
};

// Смена пароля
window.changePassword = async () => {
    const current = document.getElementById('currentPassword').value;
    const newPass = document.getElementById('newPassword').value;
    const confirm = document.getElementById('confirmPassword').value;
    
    if (!current || !newPass || !confirm) {
        showToast('Заполните все поля', 'error');
        return;
    }
    
    if (newPass !== confirm) {
        showToast('Пароли не совпадают', 'error');
        return;
    }
    
    if (newPass.length < 6) {
        showToast('Пароль должен быть минимум 6 символов', 'error');
        return;
    }
    
    try {
        const currentHashed = await cryptoMod.hashPassword(current);
        
        // Проверяем текущий пароль
        const checkRes = await fetch('/check_password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password: currentHashed })
        });
        
        if (!checkRes.ok) {
            showToast('Неверный текущий пароль', 'error');
            return;
        }
        
        const newHashed = await cryptoMod.hashPassword(newPass);
        
        // Меняем пароль
        const res = await fetch('/api/change_password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username,
                old_password: currentHashed,
                new_password: newHashed
            })
        });
        
        if (res.ok) {
            showToast('✅ Пароль успешно изменен!');
            document.getElementById('currentPassword').value = '';
            document.getElementById('newPassword').value = '';
            document.getElementById('confirmPassword').value = '';
        } else {
            showToast('Ошибка при смене пароля', 'error');
        }
    } catch (err) {
        console.error('Ошибка смены пароля:', err);
        showToast('Ошибка', 'error');
    }
};

// Удаление аккаунта
window.deleteAccount = async () => {
    const confirmed = await showCustomModal(
        "⚠️ УДАЛЕНИЕ АККАУНТА",
        "Это действие необратимо! Все ваши данные будут удалены навсегда.",
        false,
        true
    );
    
    if (!confirmed) return;
    
    const password = await showCustomModal(
        "Подтвердите удаление",
        "Введите пароль для подтверждения:",
        true,
        true
    );
    
    if (!password) return;
    
    try {
        const hashedPassword = await cryptoMod.hashPassword(password);
        
        const res = await fetch('/api/delete_account', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password: hashedPassword })
        });
        
        if (res.ok) {
            showToast('Аккаунт удален');
            await clearSessionMediaCache(username);
            localStorage.clear();
            sessionStorage.clear();
            indexedDB.deleteDatabase("LevartVault");
            setTimeout(() => window.location.replace('/info'), 1000);
        } else {
            showToast('Неверный пароль', 'error');
        }
    } catch (err) {
        showToast('Ошибка удаления', 'error');
    }
};

// Загрузка аватара
async function uploadAvatar() {
    const file = document.getElementById("avatarUpload").files[0];
    const formData = new FormData();
    formData.append("avatar", file);
    formData.append("username", username);

    const res = await fetch("/api/upload_avatar", {
        method: "POST",
        body: formData
    });
    
    if (res.ok) {
        const data = await res.json();
        document.getElementById("profileAvatar").src = data.url;
        showToast("Аватар обновлен!");
    }
}


// -------------------------
// Информация о пользователе
// -------------------------

window.openUserInfo = async (targetUsername) => {
    if (!targetUsername || targetUsername.startsWith('group_')) return;
    const panel = document.getElementById('userInfoPanel');
    const chatArea = document.getElementById('chatArea');
    const meLower = String(username || '').trim().toLowerCase();
    const targetLower = String(targetUsername || '').trim().toLowerCase();
    const isSelfProfile = targetLower === meLower;

    try {
        const res = await fetch(`/api/user_profile/${targetUsername}?me=${encodeURIComponent(meLower)}`);
        const profile = await res.json();

        // Имя
        const fullName = `${profile.first_name || ''} ${profile.last_name || ''}`.trim() || targetUsername;
        document.getElementById('userInfoName').textContent = fullName;
        document.getElementById('userInfoUsername').textContent = `@${targetUsername}`;

        // Аватарка
        const avatarImg = document.getElementById('userInfoAvatar');
        const avatarLetter = document.getElementById('userInfoAvatarLetter');
        if (profile.avatar) {
            avatarImg.src = profile.avatar;
            avatarImg.onload = () => avatarImg.classList.add('loaded');
            if (avatarImg.complete) avatarImg.classList.add('loaded');
        } else {
            avatarImg.src = '';
            avatarImg.classList.remove('loaded');
            avatarLetter.textContent = fullName.charAt(0).toUpperCase();
        }

        // Bio
        const bioSection = document.getElementById('userInfoBioSection');
        const bioEl = document.getElementById('userInfoBio');
        if (isSelfProfile) {
            bioEl.textContent = profile.bio || '—';
            bioSection.style.display = 'block';
        } else if (profile.bio) {
            bioEl.textContent = profile.bio;
            bioSection.style.display = 'block';
        } else {
            bioSection.style.display = 'none';
        }

        // Дата рождения (с учётом настроек приватности)
        const bdSection = document.getElementById('userInfoBirthdateSection');
        const bdEl = document.getElementById('userInfoBirthdate');
        if (profile.birthdate && profile.birthdate_visible) {
            try {
                const parts = profile.birthdate.split('-');
                const months = ['января','февраля','марта','апреля','мая','июня','июля','августа','сентября','октября','ноября','декабря'];
                bdEl.textContent = `${parseInt(parts[2])} ${months[parseInt(parts[1])-1]} ${parts[0]}`;
            } catch(e) {
                bdEl.textContent = profile.birthdate;
            }
            bdSection.style.display = 'block';
        } else if (isSelfProfile) {
            bdEl.textContent = '—';
            bdSection.style.display = 'block';
        } else {
            bdSection.style.display = 'none';
        }

        // Сохраняем для кнопки
        window.currentViewingUser = targetUsername;
        const actionBtn = document.getElementById('userInfoActionBtn');
        if (actionBtn) {
            if (isSelfProfile) {
                actionBtn.textContent = '✏️ Изменить';
                actionBtn.onclick = () => {
                    closeUserInfo();
                    openSettings();
                    setTimeout(() => {
                        if (typeof switchSettingsTab === 'function') switchSettingsTab('profile');
                    }, 50);
                };
            } else {
                actionBtn.textContent = '💬 Написать';
                actionBtn.onclick = () => startChatFromInfo();
            }
        }

        // Загружаем медиа и файлы
        loadUserMedia(targetUsername);

        // Открываем панель
        panel.classList.add('open');
        chatArea.classList.add('with-panel');

    } catch (err) {
        console.error('Ошибка загрузки профиля:', err);
        showToast('Не удалось загрузить профиль', 'error');
    }
};

window.closeUserInfo = () => {
    document.getElementById('userInfoPanel').classList.remove('open');
    document.getElementById('chatArea').classList.remove('with-panel');
};

window.startChatFromInfo = () => {
    if (window.currentViewingUser) {
        window.openChat(window.currentViewingUser);
        closeUserInfo();
    }
};

window.switchInfoTab = (tab, btn) => {
    document.querySelectorAll('.info-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('infoMediaGrid').style.display = tab === 'media' ? 'grid' : 'none';
    document.getElementById('infoFilesList').style.display = tab === 'files' ? 'flex' : 'none';
};

async function loadPinnedForChat(targetElId, chatPeer) {
    const listEl = document.getElementById(targetElId);
    if (!listEl || !chatPeer) return;
    listEl.innerHTML = '<div class="folder-settings-count">Загрузка...</div>';
    try {
        const chatId = String(chatPeer).startsWith('group_') ? String(chatPeer) : [username, String(chatPeer)].sort().join('_');
        const res = await fetch(`/api/pinned_messages?me=${encodeURIComponent(username)}&chat_id=${encodeURIComponent(chatId)}`);
        const pins = await res.json();
        if (!Array.isArray(pins) || !pins.length) {
            listEl.innerHTML = '<div class="empty-state-note">Нет закрепленных сообщений</div>';
            return;
        }
        listEl.innerHTML = '';
        pins.forEach((p) => {
            const row = document.createElement('div');
            row.className = 'info-file-item';
            const title = `📌 ${getCompactPinPreviewText(p)}`;
            row.innerHTML = `
                <div class="info-file-icon">📌</div>
                <div style="display:flex;flex-direction:column;min-width:0;gap:2px;">
                    <div class="info-file-name">${title.replace(/</g, '&lt;')}</div>
                </div>
                <button class="btn-user-action danger" style="margin-left:auto;padding:4px 8px;font-size:11px;">Открепить</button>
            `;
            row.onclick = async () => {
                if (window.currentChat !== chatPeer) await window.openChat(chatPeer);
                setTimeout(() => scrollToMessage(p.msg_id), 120);
            };
            row.querySelector('button')?.addEventListener('click', async (e) => {
                e.stopPropagation();
                const ok = await confirmModal('Открепить сообщение', 'Вы точно хотите открепить это сообщение?');
                if (!ok) return;
                try {
                    const r = await fetch('/api/unpin_message', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ me: username, chat_id: chatId, pin_id: p.pin_id, scope: p.scope || 'self' })
                    });
                    if (!r.ok) throw new Error('unpin_failed');
                    showToast('Откреплено');
                    await loadPinnedForChat(targetElId, chatPeer);
                    if (window.currentChat === chatPeer) loadChatPins(chatPeer).catch(() => {});
                } catch {
                    showToast('Не удалось открепить', 'error');
                }
            });
            listEl.appendChild(row);
        });
    } catch {
        listEl.innerHTML = '<div class="empty-state-note">Ошибка загрузки закрепов</div>';
    }
}

async function loadUserMedia(targetUsername) {
    const mediaGrid = document.getElementById('infoMediaGrid');
    const filesList = document.getElementById('infoFilesList');
    if (!mediaGrid || !filesList) return;
    mediaGrid.innerHTML = '<div style="color:var(--text-dim);font-size:13px;padding:10px;grid-column:span 3;text-align:center;">Загрузка...</div>';
    filesList.innerHTML = '';

    try {
        const fetched = await fetchHistoryWithOffline(targetUsername);
        const history = Array.isArray(fetched?.history) ? fetched.history : [];

        const chatId = [username, targetUsername].sort().join('_');
        let aesKey = sessionAESKeys[chatId] || sessionAESKeys[targetUsername];
        if (!aesKey && String(window.currentChat || '').toLowerCase() === String(targetUsername || '').toLowerCase() && window.currentAES) {
            aesKey = window.currentAES;
            sessionAESKeys[chatId] = aesKey;
        }
        if (!aesKey) {
            try {
                aesKey = await getAESKeyForPeer(targetUsername);
                if (aesKey) sessionAESKeys[chatId] = aesKey;
            } catch {}
        }
        if (!aesKey) {
            mediaGrid.innerHTML = '<div class="empty-state-note empty-state-note-grid">Нет доступа к ключу</div>';
            filesList.innerHTML = '<div class="empty-state-note">Нет доступа к ключу</div>';
            return;
        }

        const normalizeFileData = (raw) => {
            if (!raw || typeof raw !== 'object') return null;
            const url = String(
                raw.url || raw.path || raw.fileUrl || raw.file_url ||
                raw.file?.url || raw.file?.path || ''
            ).trim();
            const fileKey = String(
                raw.file_key || raw.key || raw.fileKey ||
                raw.file?.key || raw.file?.file_key || ''
            ).trim();
            if (!url || !fileKey) return null;
            return {
                ...raw,
                url,
                file_key: fileKey,
                name: String(raw.name || raw.filename || 'file'),
                type: String(raw.type || raw.media_kind || 'file').toLowerCase(),
                mime: String(raw.mime || raw.file?.mime || '').trim()
            };
        };

        const mediaItems = [];
        const fileItems = [];

        for (const packet of (Array.isArray(history) ? history : [])) {
            try {
                const text = await cryptoMod.decrypt(aesKey, packet.cipher);
                if (text.startsWith('__FILE__')) {
                    const data = normalizeFileData(JSON.parse(text.replace('__FILE__', '')));
                    if (!data) continue;
                    const t = String(data.type || '').toLowerCase();
                    const m = String(data.mime || '').toLowerCase();
                    const isImageLike = t === 'image' || m.startsWith('image/');
                    const isVideoLike = t === 'video' || t === 'video_note' || m.startsWith('video/');
                    if (isImageLike || isVideoLike) {
                        mediaItems.push(data);
                    } else {
                        fileItems.push(data);
                    }
                }
            } catch(e) {}
        }

        mediaGrid.innerHTML = '';
        if (mediaItems.length === 0) {
            mediaGrid.innerHTML = '<div class="empty-state-note empty-state-note-grid">Нет изображений и видео</div>';
        } else {
            mediaItems.forEach(item => {
                const thumb = document.createElement('div');
                thumb.className = 'info-media-thumb';
                const img = document.createElement('img');
                img.alt = item.name;
                thumb.appendChild(img);
                if (item.type === 'video') {
                    decryptVideoFirstFrame(item.url, item.file_key, img);
                    thumb.onclick = () => window.openMediaViewer(item.url, item.file_key, item.name, 'video');
                } else if (item.type === 'video_note') {
                    decryptVideoFirstFrame(item.url, item.file_key, img);
                    thumb.onclick = () => window.openVideoNoteViewer(item.url, item.file_key, item.name, item.mime || 'video/webm');
                } else {
                    thumb.onclick = () => window.openImageViewer(item.url, item.file_key, item.name);
                    decryptToPreview(item.url, item.file_key, img);
                }
                mediaGrid.appendChild(thumb);
            });
        }

        filesList.innerHTML = '';
        if (fileItems.length === 0) {
            filesList.innerHTML = '<div class="empty-state-note">Нет файлов</div>';
        } else {
            fileItems.forEach(item => {
                const el = document.createElement('div');
                el.className = 'info-file-item';
                el.innerHTML = `<div class="info-file-icon">📄</div><div class="info-file-name">${item.name}</div>`;
                el.onclick = () => window.decryptAndDownload(item.url, item.file_key, item.name);
                filesList.appendChild(el);
            });
        }
    } catch(e) {
        mediaGrid.innerHTML = '<div class="empty-state-note empty-state-note-grid">Ошибка загрузки</div>';
        filesList.innerHTML = '<div class="empty-state-note">Ошибка загрузки</div>';
    }
}

window.switchGroupInfoTab = (tab, btn) => {
    document.querySelectorAll('#groupInfoPanel .info-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const media = document.getElementById('groupInfoMediaGrid');
    const files = document.getElementById('groupInfoFilesList');
    const pins = document.getElementById('groupInfoPinnedList');
    if (media) media.style.display = tab === 'media' ? 'grid' : 'none';
    if (files) files.style.display = tab === 'files' ? 'flex' : 'none';
    if (pins) pins.style.display = tab === 'pins' ? 'flex' : 'none';
    if (tab === 'pins' && window._currentGroupId) loadPinnedForChat('groupInfoPinnedList', window._currentGroupId).catch(() => {});
};

async function loadGroupMedia(groupId) {
    const mediaGrid = document.getElementById('groupInfoMediaGrid');
    const filesList = document.getElementById('groupInfoFilesList');
    if (!mediaGrid || !filesList) return;
    mediaGrid.innerHTML = '<div style="color:var(--text-dim);font-size:13px;padding:10px;grid-column:span 3;text-align:center;">Загрузка...</div>';
    filesList.innerHTML = '';
    try {
        const fetched = await fetchHistoryWithOffline(groupId);
        const history = Array.isArray(fetched?.history) ? fetched.history : [];
        let aesKey = sessionAESKeys[groupId] || null;
        if (!aesKey && String(window.currentChat || '') === String(groupId) && window.currentAES) {
            aesKey = window.currentAES;
            sessionAESKeys[groupId] = aesKey;
        }
        if (!aesKey) {
            try {
                aesKey = await getAESKeyForPeer(groupId);
                if (aesKey) sessionAESKeys[groupId] = aesKey;
            } catch {}
        }
        if (!aesKey) {
            mediaGrid.innerHTML = '<div class="empty-state-note empty-state-note-grid">Нет доступа к ключу группы</div>';
            filesList.innerHTML = '<div class="empty-state-note">Нет доступа к ключу группы</div>';
            return;
        }
        const mediaItems = [];
        const fileItems = [];
        const normalizeFileData = (raw) => {
            if (!raw || typeof raw !== 'object') return null;
            const url = String(
                raw.url || raw.path || raw.fileUrl || raw.file_url ||
                raw.file?.url || raw.file?.path || ''
            ).trim();
            const fileKey = String(
                raw.file_key || raw.key || raw.fileKey ||
                raw.file?.key || raw.file?.file_key || ''
            ).trim();
            if (!url || !fileKey) return null;
            return {
                ...raw,
                url,
                file_key: fileKey,
                name: String(raw.name || raw.filename || 'file'),
                type: String(raw.type || raw.media_kind || 'file').toLowerCase(),
                mime: String(raw.mime || raw.file?.mime || '').trim()
            };
        };
        for (const packet of (Array.isArray(history) ? history : [])) {
            try {
                const text = await cryptoMod.decrypt(aesKey, packet.cipher);
                if (!text.startsWith('__FILE__')) continue;
                const data = normalizeFileData(JSON.parse(text.replace('__FILE__', '')));
                if (!data) continue;
                const t = String(data.type || '').toLowerCase();
                const m = String(data.mime || '').toLowerCase();
                const isImageLike = t === 'image' || m.startsWith('image/');
                const isVideoLike = t === 'video' || t === 'video_note' || m.startsWith('video/');
                if (isImageLike || isVideoLike) mediaItems.push(data);
                else if (t !== 'voice' && t !== 'audio') fileItems.push(data);
            } catch {}
        }
        mediaGrid.innerHTML = '';
        if (!mediaItems.length) {
            mediaGrid.innerHTML = '<div class="empty-state-note empty-state-note-grid">Нет изображений и видео</div>';
        } else {
            mediaItems.forEach((item) => {
                try {
                    const thumb = document.createElement('div');
                    thumb.className = 'info-media-thumb';
                    const img = document.createElement('img');
                    img.alt = item.name || 'media';
                    thumb.appendChild(img);
                    if (item.type === 'video' || item.type === 'video_note') {
                        decryptVideoFirstFrame(item.url, item.file_key, img);
                        thumb.onclick = () => (item.type === 'video_note')
                            ? window.openVideoNoteViewer(item.url, item.file_key, item.name, item.mime || 'video/webm')
                            : window.openMediaViewer(item.url, item.file_key, item.name, 'video');
                    } else {
                        decryptToPreview(item.url, item.file_key, img);
                        thumb.onclick = () => window.openImageViewer(item.url, item.file_key, item.name);
                    }
                    mediaGrid.appendChild(thumb);
                } catch (e) {
                    logSilent("group_media_thumb_render", e);
                }
            });
        }
        filesList.innerHTML = '';
        if (!fileItems.length) {
            filesList.innerHTML = '<div class="empty-state-note">Нет файлов</div>';
        } else {
            fileItems.forEach((item) => {
                try {
                    const el = document.createElement('div');
                    el.className = 'info-file-item';
                    el.innerHTML = `<div class="info-file-icon">📄</div><div class="info-file-name">${item.name}</div>`;
                    el.onclick = () => window.decryptAndDownload(item.url, item.file_key, item.name);
                    filesList.appendChild(el);
                } catch (e) {
                    logSilent("group_file_item_render", e);
                }
            });
        }
    } catch (e) {
        logSilent("loadGroupMedia", e);
        mediaGrid.innerHTML = '<div class="empty-state-note empty-state-note-grid">Ошибка загрузки</div>';
        filesList.innerHTML = '<div class="empty-state-note">Ошибка загрузки</div>';
    }
}

// Плашка непрочитанных — вставляется при открытии чата
function insertUnreadDivider() {
    const container = document.getElementById('messages');
    const existing = container.querySelector('.unread-divider');
    if (existing) existing.remove();
    const divider = document.createElement('div');
    divider.className = 'unread-divider';
    divider.innerHTML = '<span class="unread-label">Непрочитанные сообщения</span>';
    divider.id = 'unreadDivider';
    container.appendChild(divider);
}

// Аватарка в настройках
document.addEventListener('DOMContentLoaded', () => {
    const avatarUpload = document.getElementById('avatarUpload');
    const bioTextarea = document.getElementById('settingsBio');
    const bioCounter = document.getElementById('bioCounter');

    if (avatarUpload) {
        avatarUpload.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (!file) return;
            if (!file.type.startsWith('image/')) { showToast('Выберите изображение', 'error'); return; }
            if (file.size > 5 * 1024 * 1024) { showToast('Макс. 5MB для аватарки', 'error'); return; }
            const reader = new FileReader();
            reader.onload = (ev) => {
                const base64 = ev.target.result;
                const avatarImg = document.getElementById('avatarImage');
                const avatarLetter = document.getElementById('avatarLetter');
                avatarImg.src = base64;
                avatarImg.style.display = 'block';
                avatarLetter.style.display = 'none';
                window.currentAvatarBase64 = base64;
            };
            reader.readAsDataURL(file);
        });
    }

    if (bioTextarea && bioCounter) {
        bioTextarea.addEventListener('input', () => {
            const len = bioTextarea.value.length;
            bioCounter.textContent = `${len} / 190`;
            bioCounter.className = 'char-counter' + (len >= 190 ? ' limit' : len >= 160 ? ' warn' : '');
        });
    }

    // Превью имени/фамилии в реальном времени
    ['settingsFirstName', 'settingsLastName'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', () => {
            const f = document.getElementById('settingsFirstName').value.trim();
            const l = document.getElementById('settingsLastName').value.trim();
            document.getElementById('displayNamePreview').textContent = `${f} ${l}`.trim() || 'Имя Фамилия';
        });
    });

    // Аватарка группы
    const groupAvatarInput = document.getElementById('groupAvatarInput');
    if (groupAvatarInput) {
        groupAvatarInput.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = (ev) => {
                const img = document.getElementById('groupAvatarPreview');
                const icon = document.getElementById('groupAvatarIcon');
                img.src = ev.target.result;
                img.style.display = 'block';
                icon.style.display = 'none';
                window.currentGroupAvatar = ev.target.result;
            };
            reader.readAsDataURL(file);
        });
    }
});

window.previewGroupAvatar = () => {}; // Обрабатывается выше


// ───────────────────────────────────────────────
// DROPDOWN ДЛЯ КОНТАКТА (⋮)
// ───────────────────────────────────────────────
function showContactDropdown(event, contact, parentEl) {
    // Закрываем все открытые дропдауны
    document.querySelectorAll('.contact-dropdown').forEach(d => d.remove());
    document.querySelectorAll('.contact-item.contact-dropdown-open').forEach((el) => {
        el.classList.remove('contact-dropdown-open');
    });

    const isGroup = contact.is_group || contact.username.startsWith('group_');
    const isFriend = contact.is_friend;
    const peer = contact.username;

    const dropdown = document.createElement('div');
    dropdown.className = 'contact-dropdown';

    const items = [];

    if (!isGroup) {
        items.push({ icon: '👤', label: 'Профиль', action: () => openUserInfo(peer) });
        items.push({ icon: '✏️', label: 'Изменить имя',   action: () => openNicknameModal(peer, contact.display_name) });
        items.push({ icon: isFriend ? '👤❌' : '👤✓',
                     label: isFriend ? 'Удалить из друзей' : 'Добавить в друзья',
                     action: () => isFriend ? removeFriendAction(peer) : addFriend(peer) });
        items.push({ sep: true });
        items.push({ icon: '🗑️', label: 'Удалить чат', danger: true, action: () => deleteChatAction(peer) });
        items.push({ icon: '🚫', label: 'Заблокировать', danger: true, action: () => blockUserAction(peer) });
    } else {
        // Группа
        items.push({ icon: 'ℹ️', label: 'Информация о группе', action: () => openGroupPanel(peer) });
        items.push({ sep: true });
        items.push({ icon: '🚪', label: 'Выйти из группы', danger: true, action: () => leaveGroupAction(peer) });
    }

    items.forEach(item => {
        if (item.sep) {
            const sep = document.createElement('div');
            sep.className = 'contact-dropdown-sep';
            dropdown.appendChild(sep);
            return;
        }
        const el = document.createElement('div');
        el.className = 'contact-dropdown-item' + (item.danger ? ' danger' : '');
        el.innerHTML = `<span style="font-size:16px;">${item.icon}</span> ${item.label}`;
        el.onclick = (e) => {
            e.stopPropagation();
            dropdown.remove();
            parentEl.classList.remove('contact-dropdown-open');
            item.action();
        };
        dropdown.appendChild(el);
    });

    parentEl.classList.add('contact-dropdown-open');
    parentEl.appendChild(dropdown);

    // Закрываем при клике вне
    const closeDropdown = () => {
        dropdown.remove();
        parentEl.classList.remove('contact-dropdown-open');
    };
    setTimeout(() => {
        document.addEventListener('click', closeDropdown, { once: true });
    }, 0);
}

// ─ Действия из дропдауна ─
async function removeFriendAction(peer) {
    const ok = await showCustomModal('Удалить из друзей', `Удалить @${peer} из друзей?`, false, true);
    if (!ok) return;
    await fetch('/remove_friend', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ me: username, friend: peer })
    });
    showToast('Удалено из друзей');
    await syncMyContacts();
}

async function deleteChatAction(peer) {
    const ok = await showCustomModal('Удалить чат', `Удалить переписку с @${peer}? Это действие необратимо.`, false, true);
    if (!ok) return;
    await fetch('/api/delete_chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ me: username, peer })
    });
    showToast('Чат удалён');
    // Если текущий чат — этот пользователь, сбросить
    if (window.currentChat === peer) {
        document.getElementById('messages').innerHTML = '';
        document.getElementById('input-area').style.display = 'none';
        document.getElementById('chatHeaderName').textContent = 'Levart Secure Chat';
        document.getElementById('chatHeaderSub').textContent = 'Выберите чат';
        document.getElementById('chatHeaderHint').textContent = '';
        document.getElementById('chatHeaderAvatar').textContent = '💬';
        window.currentChat = null;
    }
    await syncMyContacts();
}

async function leaveGroupAction(groupId) {
    const ok = await showCustomModal('Выйти из группы', 'Вы точно хотите выйти из этой группы?', false, true);
    if (!ok) return;
    await fetch('/api/leave_group', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ group_id: groupId, username })
    });
    showToast('Вы вышли из группы');
    if (window.currentChat === groupId) {
        document.getElementById('messages').innerHTML = '';
        document.getElementById('input-area').style.display = 'none';
        window.currentChat = null;
    }
    await syncMyContacts();
}

async function blockUserAction(peer) {
    showToast('Функция блокировки будет добавлена в следующем обновлении', 'info');
}

// ───────────────────────────────────────────────
// МОДАЛКА ПЕРЕИМЕНОВАНИЯ ЧАТА
// ───────────────────────────────────────────────
window.openNicknameModal = (peer, currentName) => {
    window._nicknamePeer = peer;
    const input = document.getElementById('nicknameInput');
    if (input) {
        // Ставим текущее кастомное имя если есть
        fetch(`/api/get_nickname?me=${username}&peer=${peer}`)
            .then(r => r.json())
            .then(d => { input.value = d.name || ''; });
    }
    document.getElementById('nicknameModal').style.display = 'flex';
    setTimeout(() => input && input.focus(), 100);
};

window.closeNicknameModal = () => {
    document.getElementById('nicknameModal').style.display = 'none';
};

window.saveNickname = async () => {
    const peer = window._nicknamePeer;
    const name = document.getElementById('nicknameInput').value.trim();
    await fetch('/api/set_nickname', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ me: username, peer, name })
    });
    closeNicknameModal();
    showToast(name ? `Имя изменено на "${name}"` : 'Имя сброшено');
    // Обновляем заголовок чата если открыт
    if (window.currentChat === peer) {
        const nameEl = document.getElementById('chatHeaderName');
        if (nameEl) nameEl.textContent = name || peer;
    }
    await syncMyContacts();
};

// ───────────────────────────────────────────────
// ПАНЕЛЬ ГРУППЫ
// ───────────────────────────────────────────────
window._currentGroupId = null;
window._currentGroupData = null;
window._currentGroupPermissions = null;

async function loadGroupPermissions(groupId) {
    try {
        const res = await fetch(`/api/group_permissions/${encodeURIComponent(groupId)}?me=${encodeURIComponent(username)}`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'perm_load_failed');
        window._currentGroupPermissions = data.permissions || {};
        const d = window._currentGroupPermissions.defaults || {};
        const set = (id, v) => { const el = document.getElementById(id); if (el) el.checked = !!v; };
        set('permSendMessages', d.can_send_messages);
        set('permSendLinks', d.can_send_links);
        set('permSendMedia', d.can_send_media);
        set('permSendStickers', d.can_send_stickers);
        set('permSendGifs', d.can_send_gifs);
        set('permSendVoice', d.can_send_voice);
        set('permSendVideoNotes', d.can_send_video_notes);
        set('permStartCalls', d.can_start_calls);
        set('permAddMembers', d.can_add_members);
        set('permRemoveMembers', d.can_remove_members);
        set('permPinMessages', d.can_pin_messages);
        set('permChangeInfo', d.can_change_info);
        set('permManagePermissions', d.can_manage_permissions);
    } catch {
        window._currentGroupPermissions = null;
    }
}

window.saveGroupDefaultPermissions = async () => {
    const groupId = window._currentGroupId;
    if (!groupId) return;
    try {
        const body = {
            group_id: groupId,
            username: String(username || '').toLowerCase(),
            permissions: {
                can_send_messages: !!document.getElementById('permSendMessages')?.checked,
                can_send_links: !!document.getElementById('permSendLinks')?.checked,
                can_send_media: !!document.getElementById('permSendMedia')?.checked,
                can_send_stickers: !!document.getElementById('permSendStickers')?.checked,
                can_send_gifs: !!document.getElementById('permSendGifs')?.checked,
                can_send_voice: !!document.getElementById('permSendVoice')?.checked,
                can_send_video_notes: !!document.getElementById('permSendVideoNotes')?.checked,
                can_start_calls: !!document.getElementById('permStartCalls')?.checked,
                can_add_members: !!document.getElementById('permAddMembers')?.checked,
                can_remove_members: !!document.getElementById('permRemoveMembers')?.checked,
                can_pin_messages: !!document.getElementById('permPinMessages')?.checked,
                can_change_info: !!document.getElementById('permChangeInfo')?.checked,
                can_manage_permissions: !!document.getElementById('permManagePermissions')?.checked
            }
        };
        const res = await fetch('/api/group_permissions/update', {
            method: 'POST',
            headers: authJsonHeaders(),
            body: JSON.stringify(body)
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'perm_save_failed');
        showToast('Права участников обновлены');
        window._currentGroupPermissions = data.permissions || {};
    } catch (e) {
        showToast(`Не удалось сохранить права (${String(e?.message || 'error')})`, 'error');
    }
};

window.toggleMemberWritePermission = async (groupId, member, canWrite) => {
    try {
        const target = String(member || '').toLowerCase();
        const res = await fetch('/api/group_permissions/update', {
            method: 'POST',
            headers: authJsonHeaders(),
            body: JSON.stringify({
                group_id: groupId,
                username: String(username || '').toLowerCase(),
                target,
                permissions: { can_send_messages: !!canWrite }
            })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'perm_member_failed');
        showToast(canWrite ? `@${member} может писать` : `@${member} ограничен в сообщениях`);
        window._currentGroupPermissions = data.permissions || {};
        await openGroupPanel(groupId);
    } catch (e) {
        showToast(`Не удалось обновить права участника (${String(e?.message || 'error')})`, 'error');
    }
};

window.toggleMemberMediaPermission = async (groupId, member, canMedia) => {
    try {
        const target = String(member || '').toLowerCase();
        const res = await fetch('/api/group_permissions/update', {
            method: 'POST',
            headers: authJsonHeaders(),
            body: JSON.stringify({
                group_id: groupId,
                username: String(username || '').toLowerCase(),
                target,
                permissions: { can_send_media: !!canMedia }
            })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'perm_member_media_failed');
        showToast(canMedia ? `@${member} может отправлять медиа` : `@${member} ограничен в медиа`);
        window._currentGroupPermissions = data.permissions || {};
        await openGroupPanel(groupId);
    } catch (e) {
        showToast(`Не удалось обновить права участника (${String(e?.message || 'error')})`, 'error');
    }
};

const GROUP_PERM_FIELDS = [
    ['can_send_messages', 'Писать сообщения'],
    ['can_send_links', 'Отправлять ссылки'],
    ['can_send_media', 'Отправлять медиа/файлы'],
    ['can_send_stickers', 'Отправлять стикеры'],
    ['can_send_gifs', 'Отправлять GIF'],
    ['can_send_voice', 'Отправлять голосовые'],
    ['can_send_video_notes', 'Отправлять кружки'],
    ['can_start_calls', 'Начинать звонки'],
    ['can_add_members', 'Добавлять участников'],
    ['can_remove_members', 'Удалять участников'],
    ['can_pin_messages', 'Закреплять для всех'],
    ['can_change_info', 'Менять название/описание'],
    ['can_manage_permissions', 'Управлять правами']
];

function getGroupMemberPermFlags(groupData, memberUsername) {
    const owner = String(groupData?.owner || '').toLowerCase();
    const uname = String(memberUsername || '').toLowerCase();
    const defaults = window._currentGroupPermissions?.defaults || {};
    const memberPerms = window._currentGroupPermissions?.members || {};
    const patch = memberPerms?.[uname] || {};
    const isOwner = uname === owner;
    const flag = (k, fallback = true) => (isOwner ? true : (Object.prototype.hasOwnProperty.call(patch, k) ? !!patch[k] : !!(k in defaults ? defaults[k] : fallback)));
    return {
        isOwner,
        canWrite: flag('can_send_messages', true),
        canMedia: flag('can_send_media', true),
        canPin: flag('can_pin_messages', false),
        canCalls: flag('can_start_calls', true),
        canAddMembers: flag('can_add_members', false),
        canRemoveMembers: flag('can_remove_members', false)
    };
}

function formatGroupMemberPermSummary(flags) {
    if (!flags) return 'Права участника';
    if (flags.isOwner) return 'Полный доступ';
    const parts = [];
    parts.push(flags.canWrite ? 'Пишет' : 'Только чтение');
    if (flags.canMedia) parts.push('Медиа');
    if (flags.canCalls) parts.push('Звонки');
    if (flags.canPin) parts.push('Закрепы');
    if (flags.canAddMembers || flags.canRemoveMembers) parts.push('Модерация');
    return parts.join(' • ');
}

function closeGroupMemberActionMenus() {
    document.querySelectorAll('.group-member-actions-menu').forEach((el) => el.remove());
}

window.openGroupMemberActionsMenu = (triggerBtn, payload) => {
    const p = payload || {};
    if (!triggerBtn) return;
    closeGroupMemberActionMenus();
    const menu = document.createElement('div');
    menu.className = 'group-member-actions-menu';
    const safeMember = String(p.member || '').replace(/</g, '&lt;');
    const actions = [];
    actions.push({ label: '👤 Профиль', onClick: () => { closeGroupPanel(); openUserInfo(p.member); } });
    actions.push({ label: '🛡️ Права участника', onClick: () => openMemberPermissionsEditor(p.groupId, p.member), disabled: !p.canManagePerms || p.isOwner });
    actions.push({ label: p.canWrite ? '🚫 Запретить писать' : '✅ Разрешить писать', onClick: () => toggleMemberWritePermission(p.groupId, p.member, !p.canWrite), disabled: !p.canManagePerms || p.isOwner });
    actions.push({ label: p.canMedia ? '🚫 Запретить медиа' : '✅ Разрешить медиа', onClick: () => toggleMemberMediaPermission(p.groupId, p.member, !p.canMedia), disabled: !p.canManagePerms || p.isOwner });
    actions.push({ label: 'ℹ️ Что можно делать', onClick: () => showCustomModal(`@${safeMember}`, `${formatGroupMemberPermSummary(p.flags || {})}. Здесь можно открыть профиль, изменить права или удалить участника.`) });
    actions.push({ label: '➖ Удалить из группы', danger: true, onClick: () => groupRemoveMemberAction(p.groupId, p.member), disabled: !p.canRemove || p.isOwner });

    actions.forEach((a) => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = `group-member-actions-item${a.danger ? ' danger' : ''}`;
        btn.textContent = a.label;
        if (a.disabled) {
            btn.disabled = true;
            btn.classList.add('disabled');
        }
        btn.onclick = (e) => {
            e.stopPropagation();
            if (btn.disabled) return;
            closeGroupMemberActionMenus();
            a.onClick?.();
        };
        menu.appendChild(btn);
    });
    document.body.appendChild(menu);
    const r = triggerBtn.getBoundingClientRect();
    const mw = 250;
    const mh = Math.min(window.innerHeight - 24, menu.offsetHeight || 260);
    const left = Math.max(8, Math.min(window.innerWidth - mw - 8, r.right - mw));
    const top = Math.max(8, Math.min(window.innerHeight - mh - 8, r.bottom + 6));
    menu.style.left = `${Math.round(left)}px`;
    menu.style.top = `${Math.round(top)}px`;
};

window.renderGroupEditMembers = (groupId, groupData, rows, canManagePerms, canAddMembers) => {
    const section = document.getElementById('groupEditMembersSection');
    const list = document.getElementById('groupEditMemberList');
    const addBtn = document.getElementById('groupEditAddMemberBtn');
    if (!section || !list || !addBtn) return;
    const editable = !!(canManagePerms || canAddMembers);
    section.style.display = editable ? 'block' : 'none';
    addBtn.style.display = canAddMembers ? 'inline-flex' : 'none';
    addBtn.onclick = () => groupAddMemberAction(groupId);
    list.innerHTML = '';
    if (!editable) return;
    (rows || []).forEach((row) => {
        const item = document.createElement('div');
        item.className = 'group-edit-member-item';
        const avatarHtml = row.avatar
            ? `<img src="${row.avatar}" alt="">`
            : `<span>${String(row.fullName || row.member || '?').charAt(0).toUpperCase()}</span>`;
        const roleText = row.isOwner ? 'Создатель' : formatGroupMemberPermSummary(row.flags);
        item.innerHTML = `
            <div class="group-edit-member-avatar">${avatarHtml}</div>
            <div class="group-edit-member-main">
                <div class="group-edit-member-name">${String(row.fullName || row.member || '').replace(/</g, '&lt;')}</div>
                <div class="group-edit-member-sub">@${String(row.member || '').replace(/</g, '&lt;')} • ${String(roleText || '').replace(/</g, '&lt;')}</div>
            </div>
            <button type="button" class="group-edit-member-menu-btn" title="Действия">⋮</button>
        `;
        item.querySelector('.group-edit-member-main')?.addEventListener('click', () => { closeGroupPanel(); openUserInfo(row.member); });
        item.querySelector('.group-edit-member-avatar')?.addEventListener('click', () => { closeGroupPanel(); openUserInfo(row.member); });
        item.querySelector('.group-edit-member-menu-btn')?.addEventListener('click', (e) => {
            e.stopPropagation();
            window.openGroupMemberActionsMenu(e.currentTarget, {
                groupId,
                member: row.member,
                fullName: row.fullName,
                isOwner: !!row.isOwner,
                canManagePerms: !!canManagePerms,
                canRemove: !!row.canRemove,
                canWrite: !!row.canWrite,
                canMedia: !!row.canMedia,
                flags: row.flags
            });
        });
        list.appendChild(item);
    });
};

document.addEventListener('click', () => closeGroupMemberActionMenus());

window.openGroupMemberRightsManager = () => {
    const data = window._currentGroupData;
    if (!data) return;
    const canManagePerms = _effectiveGroupPerm(window._currentGroupPermissions || {}, data.owner, username, 'can_manage_permissions');
    const canAddMembers = _effectiveGroupPerm(window._currentGroupPermissions || {}, data.owner, username, 'can_add_members');
    if (!canManagePerms && !canAddMembers) {
        showToast('Недостаточно прав', 'error');
        return;
    }
    const edit = document.getElementById('groupEditMode');
    if (edit && edit.style.display === 'none') {
        window.toggleGroupEdit();
    }
    setTimeout(() => {
        const sec = document.getElementById('groupEditMembersSection');
        sec?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 80);
};

window.openMemberPermissionsEditor = async (groupId, member) => {
    const perms = window._currentGroupPermissions || {};
    const d = perms.defaults || {};
    const m = (perms.members || {})[String(member || '').toLowerCase()] || {};
    const existing = document.getElementById('memberPermModal');
    existing?.remove();
    const modal = document.createElement('div');
    modal.id = 'memberPermModal';
    modal.className = 'modal-overlay';
    modal.style.display = 'flex';
    modal.innerHTML = `
      <div class="modal-content card member-perm-modal">
        <div class="member-perm-head">
          <h3>Права участника</h3>
          <button id="memberPermCloseTop" class="member-perm-close-top" title="Закрыть">✕</button>
        </div>
        <div class="member-perm-user">@${String(member || '').replace(/</g, '&lt;')}</div>
        <div class="member-perm-hint">Выбери, что участник может делать в группе.</div>
        <div class="member-perm-presets">
          <button type="button" class="member-perm-preset" data-preset="read_only">Только чтение</button>
          <button type="button" class="member-perm-preset" data-preset="basic">Обычный</button>
          <button type="button" class="member-perm-preset" data-preset="moderator">Модератор</button>
          <button type="button" class="member-perm-preset" data-preset="reset">Сброс</button>
        </div>
        <div id="memberPermList" class="member-perm-list"></div>
        <div class="modal-buttons">
          <button class="secondary" id="memberPermCancel">Отмена</button>
          <button id="memberPermSave">Сохранить</button>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    const list = modal.querySelector('#memberPermList');
    GROUP_PERM_FIELDS.forEach(([k, label]) => {
        const v = Object.prototype.hasOwnProperty.call(m, k) ? !!m[k] : !!d[k];
        const row = document.createElement('label');
        row.className = 'member-perm-item';
        row.innerHTML = `
            <input type="checkbox" data-perm="${k}" ${v ? 'checked' : ''}>
            <span>${label}</span>
        `;
        list.appendChild(row);
    });
    const presetMap = {
        read_only: {
            can_send_messages: false,
            can_send_links: false,
            can_send_media: false,
            can_send_stickers: false,
            can_send_gifs: false,
            can_send_voice: false,
            can_send_video_notes: false,
            can_start_calls: false,
            can_add_members: false,
            can_remove_members: false,
            can_pin_messages: false,
            can_change_info: false,
            can_manage_permissions: false
        },
        basic: {
            can_send_messages: true,
            can_send_links: true,
            can_send_media: true,
            can_send_stickers: true,
            can_send_gifs: true,
            can_send_voice: true,
            can_send_video_notes: true,
            can_start_calls: true,
            can_add_members: false,
            can_remove_members: false,
            can_pin_messages: false,
            can_change_info: false,
            can_manage_permissions: false
        },
        moderator: {
            can_send_messages: true,
            can_send_links: true,
            can_send_media: true,
            can_send_stickers: true,
            can_send_gifs: true,
            can_send_voice: true,
            can_send_video_notes: true,
            can_start_calls: true,
            can_add_members: true,
            can_remove_members: true,
            can_pin_messages: true,
            can_change_info: true,
            can_manage_permissions: true
        }
    };
    const applyPreset = (presetName) => {
        let source = null;
        if (presetName === 'reset') source = d;
        else source = presetMap[presetName] || null;
        if (!source) return;
        modal.querySelectorAll('input[data-perm]').forEach((el) => {
            const key = el.dataset.perm;
            if (!key) return;
            el.checked = !!source[key];
        });
    };
    modal.querySelectorAll('.member-perm-preset').forEach((btn) => {
        btn.addEventListener('click', () => applyPreset(btn.dataset.preset));
    });
    modal.querySelector('#memberPermCancel')?.addEventListener('click', () => modal.remove());
    modal.querySelector('#memberPermCloseTop')?.addEventListener('click', () => modal.remove());
    modal.querySelector('#memberPermSave')?.addEventListener('click', async () => {
        const patch = {};
        const target = String(member || '').toLowerCase();
        modal.querySelectorAll('input[data-perm]').forEach((el) => {
            patch[el.dataset.perm] = !!el.checked;
        });
        try {
            const res = await fetch('/api/group_permissions/update', {
                method: 'POST',
                headers: authJsonHeaders(),
                body: JSON.stringify({ group_id: groupId, username: String(username || '').toLowerCase(), target, permissions: patch })
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || 'perm_member_save_failed');
            window._currentGroupPermissions = data.permissions || {};
            modal.remove();
            showToast(`Права @${member} обновлены`);
            await openGroupPanel(groupId);
        } catch (e) {
            showToast(`Не удалось сохранить права участника (${String(e?.message || 'error')})`, 'error');
        }
    });
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
};

window.groupAddMemberAction = async (groupId) => {
    const member = (await promptModal('Добавить участника', 'Введите @username пользователя:')).replace(/^@+/, '').toLowerCase();
    if (!member) return;
    try {
        const res = await fetch('/api/group_add_member', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ group_id: groupId, username, member })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'add_failed');
        if (!data.already_member) {
            try { await tryGrantGroupKeyToMember(groupId, member); } catch {}
        }
        if (data.already_member) showToast('Пользователь уже в группе');
        else showToast('Участник добавлен');
        await syncMyContacts();
        await openGroupPanel(groupId);
    } catch {
        showToast('Не удалось добавить участника', 'error');
    }
};

window.groupRemoveMemberAction = async (groupId, member) => {
    const ok = await confirmModal('Удалить участника', `Удалить @${member} из группы?`);
    if (!ok) return;
    try {
        const res = await fetch('/api/group_remove_member', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ group_id: groupId, username, member })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'remove_failed');
        if (data.already_removed) showToast('Участник уже удален');
        else showToast('Участник удален');
        await syncMyContacts();
        await openGroupPanel(groupId);
    } catch {
        showToast('Не удалось удалить участника', 'error');
    }
};

window.openGroupPanel = async (groupId) => {
    window._currentGroupId = groupId;
    const panel = document.getElementById('groupInfoPanel');
    const chatArea = document.getElementById('chatArea');

    try {
        const res = await fetch(`/api/group_info/${groupId}`);
        const data = await res.json();
        window._currentGroupData = data;

        // Аватар
        const avatarImg = document.getElementById('groupPanelAvatar');
        const avatarLetter = document.getElementById('groupPanelAvatarLetter');
        if (data.avatar) {
            avatarImg.src = data.avatar;
            avatarImg.onload = () => avatarImg.classList.add('loaded');
            avatarImg.style.opacity = '1';
        } else {
            avatarImg.src = '';
            avatarImg.style.opacity = '0';
            avatarLetter.textContent = (data.name || 'G').charAt(0).toUpperCase();
        }

        document.getElementById('groupPanelName').textContent = data.name || 'Группа';

        const descSection = document.getElementById('groupPanelDescSection');
        const descEl = document.getElementById('groupPanelDesc');
        if (data.desc) {
            descEl.textContent = data.desc;
            descSection.style.display = 'block';
        } else {
            descSection.style.display = 'none';
        }
        const inviteEl = document.getElementById('groupInviteLink');
        if (inviteEl) inviteEl.value = data.invite_link || '';

        // Участники
        const uniqueMembers = [];
        const memberSeen = new Set();
        for (const m of (data.members || [])) {
            const ml = String(m || '').toLowerCase();
            if (!ml || memberSeen.has(ml)) continue;
            memberSeen.add(ml);
            uniqueMembers.push(ml);
        }
        document.getElementById('groupMemberCount').textContent = `(${uniqueMembers.length})`;
        const memberList = document.getElementById('groupMemberList');
        memberList.innerHTML = '';
        await loadGroupPermissions(groupId);
        const memberPerms = window._currentGroupPermissions?.members || {};
        const defaultPerms = window._currentGroupPermissions?.defaults || {};
        const canManagePerms = _effectiveGroupPerm(window._currentGroupPermissions || {}, data.owner, username, 'can_manage_permissions');
        const canRemoveMembersPerm = _effectiveGroupPerm(window._currentGroupPermissions || {}, data.owner, username, 'can_remove_members');
        const canAddMembersPerm = _effectiveGroupPerm(window._currentGroupPermissions || {}, data.owner, username, 'can_add_members');
        const editRows = [];
        const membersLower = uniqueMembers;
        let profilesMap = {};
        try {
            const batchRes = await fetch('/api/user_profiles_batch', {
                method: 'POST',
                headers: authJsonHeaders(),
                body: JSON.stringify({ me: String(username || '').toLowerCase(), users: membersLower })
            });
            const batchData = await batchRes.json();
            if (batchRes.ok && batchData?.profiles && typeof batchData.profiles === 'object') {
                profilesMap = batchData.profiles;
            }
        } catch (e) {
            logSilent("user_profiles_batch", e);
        }
        for (const mLower of uniqueMembers) {
            const pd = profilesMap?.[mLower] || {};
            const item = document.createElement('div');
            item.className = 'group-member-item';
            const fullN = `${pd.first_name || ''} ${pd.last_name || ''}`.trim() || mLower;
            const isOwner = mLower === String(data.owner || '').toLowerCase();
            const avatarDiv = document.createElement('div');
            avatarDiv.className = 'group-member-avatar';
            if (pd.avatar) {
                const img = document.createElement('img');
                img.src = pd.avatar;
                avatarDiv.appendChild(img);
            } else {
                avatarDiv.textContent = fullN.charAt(0).toUpperCase();
            }
            item.appendChild(avatarDiv);
            const canRemove = ((String(username || '').toLowerCase() === String(data.owner || '').toLowerCase()) || canRemoveMembersPerm) && !isOwner;
            const canWrite = isOwner ? true : (memberPerms?.[mLower]?.can_send_messages ?? defaultPerms?.can_send_messages ?? true);
            const canMedia = isOwner ? true : (memberPerms?.[mLower]?.can_send_media ?? defaultPerms?.can_send_media ?? true);
            const flags = getGroupMemberPermFlags(data, mLower);
            const summary = formatGroupMemberPermSummary(flags);
            item.innerHTML += `
                <div class="group-member-info">
                    <div class="group-member-name">${fullN}</div>
                    <div class="group-member-subline">@${mLower} • ${summary}</div>
                </div>
                ${isOwner ? '<div class="group-owner-badge">👑 Создатель</div>' : ''}
                ${(!isOwner && (canManagePerms || canRemove)) ? '<button type="button" class="group-member-menu-btn" title="Действия">⋮</button>' : ''}
            `;
            const menuBtn = item.querySelector('.group-member-menu-btn');
            if (menuBtn) {
                menuBtn.onclick = (ev) => {
                    ev.stopPropagation();
                    window.openGroupMemberActionsMenu(menuBtn, {
                        groupId,
                        member: mLower,
                        fullName: fullN,
                        isOwner: !!isOwner,
                        canManagePerms: !!canManagePerms,
                        canRemove: !!canRemove,
                        canWrite: !!canWrite,
                        canMedia: !!canMedia,
                        flags
                    });
                };
            }
            item.onclick = () => { closeGroupPanel(); openUserInfo(mLower); };
            memberList.appendChild(item);
            editRows.push({
                member: mLower,
                fullName: fullN,
                avatar: pd.avatar || '',
                isOwner,
                canRemove,
                canWrite,
                canMedia,
                flags
            });
        }
        window.renderGroupEditMembers(groupId, data, editRows, canManagePerms, canAddMembersPerm);

        // Кнопки действий
        const actions = document.getElementById('groupPanelActions');
        actions.innerHTML = '';
        const isOwner = data.owner === username;
        const canAddMembers = canAddMembersPerm;
        const canChangeInfo = _effectiveGroupPerm(window._currentGroupPermissions || {}, data.owner, username, 'can_change_info');
        const permSection = document.getElementById('groupPermSection');
        if (permSection) {
            const canOpenMemberRights = !!(canManagePerms || canAddMembersPerm);
            permSection.style.display = canOpenMemberRights ? 'block' : 'none';
            const saveBtn = permSection.querySelector('button[onclick="saveGroupDefaultPermissions()"]');
            const openBtn = permSection.querySelector('button[onclick="openGroupMemberRightsManager()"]');
            if (saveBtn) {
                saveBtn.style.display = canManagePerms ? 'inline-flex' : 'none';
            }
            if (openBtn) {
                openBtn.style.display = canOpenMemberRights ? 'inline-flex' : 'none';
            }
            permSection.querySelectorAll('input[type="checkbox"]').forEach((cb) => {
                cb.disabled = !canManagePerms;
            });
        }
        if (canAddMembers) {
            const addBtn = document.createElement('button');
            addBtn.className = 'btn-user-action';
            addBtn.textContent = '➕ Добавить участника';
            addBtn.onclick = () => groupAddMemberAction(groupId);
            actions.appendChild(addBtn);
        }

        if (isOwner) {
            const delBtn = document.createElement('button');
            delBtn.className = 'btn-user-action danger';
            delBtn.textContent = '🗑️ Удалить группу';
            delBtn.onclick = async () => {
                const ok = await showCustomModal('Удалить группу', 'Это удалит группу и всю историю навсегда.', false, true);
                if (!ok) return;
                await fetch('/api/delete_group', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ group_id: groupId, username })
                });
                showToast('Группа удалена');
                closeGroupPanel();
                if (window.currentChat === groupId) {
                    document.getElementById('messages').innerHTML = '';
                    document.getElementById('input-area').style.display = 'none';
                    window.currentChat = null;
                }
                await syncMyContacts();
            };
            actions.appendChild(delBtn);
        } else {
            const leaveBtn = document.createElement('button');
            leaveBtn.className = 'btn-user-action danger';
            leaveBtn.textContent = '🚪 Выйти из группы';
            leaveBtn.onclick = () => leaveGroupAction(groupId);
            actions.appendChild(leaveBtn);
        }

        // Показать/скрыть кнопку редактирования (инфо + права/участники)
        const canOpenEdit = !!(canChangeInfo || canManagePerms || canAddMembers);
        document.getElementById('groupEditBtn').style.display = canOpenEdit ? 'flex' : 'none';

        // Показываем панель
        document.getElementById('groupViewMode').style.display = 'block';
        document.getElementById('groupEditMode').style.display = 'none';
        loadGroupMedia(groupId).catch(() => {});
        loadPinnedForChat('groupInfoPinnedList', groupId).catch(() => {});
        document.querySelectorAll('#groupInfoPanel .info-tab').forEach((b) => b.classList.remove('active'));
        const firstTab = document.querySelector('#groupInfoPanel .info-tab');
        firstTab?.classList.add('active');
        const gm = document.getElementById('groupInfoMediaGrid');
        const gf = document.getElementById('groupInfoFilesList');
        const gp = document.getElementById('groupInfoPinnedList');
        if (gm) gm.style.display = 'grid';
        if (gf) gf.style.display = 'none';
        if (gp) gp.style.display = 'none';
        panel.classList.add('open');
        chatArea.classList.add('with-panel');
        // Если открыта панель профиля — закрываем
        document.getElementById('userInfoPanel').classList.remove('open');

    } catch(e) {
        console.error('Ошибка загрузки группы:', e);
        showToast('Ошибка загрузки группы', 'error');
    }
};

window.closeGroupPanel = () => {
    document.getElementById('groupInfoPanel').classList.remove('open');
    document.getElementById('chatArea').classList.remove('with-panel');
};

window.copyGroupInviteLink = async () => {
    const inp = document.getElementById('groupInviteLink');
    const link = String(inp?.value || '').trim();
    if (!link) {
        showToast('Ссылка недоступна', 'error');
        return;
    }
    try {
        await navigator.clipboard.writeText(link);
        showToast('Ссылка скопирована');
    } catch {
        inp?.select();
        document.execCommand('copy');
        showToast('Ссылка скопирована');
    }
};

window.toggleGroupEdit = () => {
    const view = document.getElementById('groupViewMode');
    const edit = document.getElementById('groupEditMode');
    const data = window._currentGroupData;
    if (!data) return;
    const canChangeInfo = _effectiveGroupPerm(window._currentGroupPermissions || {}, data.owner, username, 'can_change_info');
    const nameEl = document.getElementById('groupEditName');
    const descEl = document.getElementById('groupEditDesc');
    const avatarPicker = document.getElementById('groupEditAvatarPicker');
    const saveBtn = document.getElementById('groupEditSaveBtn');

    if (edit.style.display === 'none') {
        // Заполняем поля
        nameEl.value = data.name || '';
        descEl.value = data.desc || '';
        const preview = document.getElementById('groupEditAvatarPreview');
        const icon = document.getElementById('groupEditAvatarIcon');
        if (data.avatar) {
            preview.src = data.avatar;
            preview.style.display = 'block';
            icon.style.display = 'none';
        } else {
            preview.style.display = 'none';
            icon.style.display = 'block';
        }
        nameEl.disabled = !canChangeInfo;
        descEl.disabled = !canChangeInfo;
        if (avatarPicker) {
            avatarPicker.style.pointerEvents = canChangeInfo ? 'auto' : 'none';
            avatarPicker.style.opacity = canChangeInfo ? '1' : '0.6';
        }
        if (saveBtn) saveBtn.style.display = canChangeInfo ? 'inline-flex' : 'none';
        view.style.display = 'none';
        edit.style.display = 'block';
    } else {
        view.style.display = 'block';
        edit.style.display = 'none';
    }
};

window.cancelGroupEdit = () => {
    document.getElementById('groupViewMode').style.display = 'block';
    document.getElementById('groupEditMode').style.display = 'none';
};

window.saveGroupEdit = async () => {
    const groupId = window._currentGroupId;
    const data    = window._currentGroupData || {};
    const canChangeInfo = _effectiveGroupPerm(window._currentGroupPermissions || {}, data.owner, username, 'can_change_info');
    if (!canChangeInfo) { showToast('Недостаточно прав для изменения информации', 'error'); return; }
    const name    = document.getElementById('groupEditName').value.trim();
    const desc    = document.getElementById('groupEditDesc').value.trim();
    const preview = document.getElementById('groupEditAvatarPreview');
    const avatar  = preview.style.display !== 'none' ? preview.src : '';

    if (!name) { showToast('Введите название', 'error'); return; }

    await fetch('/api/update_group', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ group_id: groupId, username, name, desc, avatar })
    });
    showToast('✅ Группа обновлена!');
    await openGroupPanel(groupId);
    await syncMyContacts();
    // Обновляем заголовок если этот чат открыт
    if (window.currentChat === groupId) {
        document.getElementById('chatHeaderName').textContent = name;
    }
};

window.previewEditGroupAvatar = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
        const preview = document.getElementById('groupEditAvatarPreview');
        const icon = document.getElementById('groupEditAvatarIcon');
        preview.src = ev.target.result;
        preview.style.display = 'block';
        icon.style.display = 'none';
    };
    reader.readAsDataURL(file);
};

// ───────────────────────────────────────────────
// ВНЕШНИЙ ВИД
// ───────────────────────────────────────────────
const THEMES = {
    dark:    { '--bg-main':'#020617', '--sidebar-bg':'#0f172a', '--panel':'#1e293b', '--msg-in-bg':'#1e293b' },
    darker:  { '--bg-main':'#000000', '--sidebar-bg':'#0a0a0a', '--panel':'#111111', '--msg-in-bg':'#111111' },
    navy:    { '--bg-main':'#050d1e', '--sidebar-bg':'#0a1628', '--panel':'#122040', '--msg-in-bg':'#122040' },
    forest:  { '--bg-main':'#030d04', '--sidebar-bg':'#0a1a0e', '--panel':'#0f2813', '--msg-in-bg':'#0f2813' },
    purple:  { '--bg-main':'#080414', '--sidebar-bg':'#130a24', '--panel':'#1c0f35', '--msg-in-bg':'#1c0f35' },
};

window.applyTheme = (name, el) => {
    const vars = THEMES[name];
    if (vars) {
        Object.entries(vars).forEach(([k, v]) => document.documentElement.style.setProperty(k, v));
    }
    document.querySelectorAll('.theme-card').forEach(c => c.classList.remove('active'));
    if (el) el.classList.add('active');
    localStorage.setItem('levart_theme', name);
};

window.setAccent = (color, el) => {
    document.documentElement.style.setProperty('--accent', color);
    // Пересчитываем hover (чуть темнее)
    const darker = color + 'cc';
    document.documentElement.style.setProperty('--accent-hover', darker);
    document.documentElement.style.setProperty('--accent-dim', color + '26');
    document.querySelectorAll('.accent-dot').forEach(d => {
        d.style.border = d.dataset.color === color ? '3px solid white' : '2px solid transparent';
        d.style.transform = d.dataset.color === color ? 'scale(1.15)' : 'scale(1)';
    });
    document.getElementById('accentColorPicker').value = color;
    localStorage.setItem('levart_accent', color);
};

window.applyFontSize = (val) => {
    document.getElementById('fontSizeValue').textContent = val + 'px';
    document.querySelectorAll('.msg-text').forEach(el => el.style.fontSize = val + 'px');
    document.documentElement.style.setProperty('--msg-font-size', val + 'px');
    localStorage.setItem('levart_fontsize', val);
};

window.applyFont = (font, el) => {
    document.documentElement.style.setProperty('--font', `'${font}', system-ui, sans-serif`);
    document.querySelectorAll('.font-option').forEach(b => b.classList.remove('active'));
    if (el) el.classList.add('active');
    localStorage.setItem('levart_font', font);
};

window.applyMsgGap = (val) => {
    document.getElementById('msgGapValue').textContent = val + 'px';
    document.getElementById('messages').style.gap = val + 'px';
    localStorage.setItem('levart_msggap', val);
};

window.applyCustomCss = () => {
    const css = document.getElementById('customCssInput').value;
    let style = document.getElementById('levart-custom-css');
    if (!style) { style = document.createElement('style'); style.id = 'levart-custom-css'; document.head.appendChild(style); }
    style.textContent = css;
    localStorage.setItem('levart_custom_css', css);
    showToast('✅ CSS применён!');
};

window.resetCustomCss = () => {
    const style = document.getElementById('levart-custom-css');
    if (style) style.textContent = '';
    document.getElementById('customCssInput').value = '';
    localStorage.removeItem('levart_custom_css');
    showToast('Стили сброшены');
};

// Загрузить сохранённые настройки при старте
function loadAppearanceSettings() {
    const theme  = localStorage.getItem('levart_theme');
    const accent = localStorage.getItem('levart_accent');
    const size   = localStorage.getItem('levart_fontsize');
    const font   = localStorage.getItem('levart_font');
    const gap    = localStorage.getItem('levart_msggap');
    const css    = localStorage.getItem('levart_custom_css');

    if (theme)  applyTheme(theme, document.querySelector(`[data-theme="${theme}"]`));
    if (accent) setAccent(accent, null);
    if (size)   { document.getElementById('fontSizeSlider').value = size; applyFontSize(size); }
    if (font)   { applyFont(font, document.querySelector(`[data-font="${font}"]`)); }
    if (gap)    { document.getElementById('msgGapSlider').value = gap; applyMsgGap(gap); }
    if (css)    { document.getElementById('customCssInput').value = css; applyCustomCss(); }
}

// ───────────────────────────────────────────────
// АВАТАРКА ГРУППЫ при создании
// ───────────────────────────────────────────────
window.handleGroupAvatarChange = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    if (!file.type.startsWith('image/')) { showToast('Только изображения', 'error'); return; }
    if (file.size > 5 * 1024 * 1024)    { showToast('Макс. 5MB', 'error'); return; }
    const reader = new FileReader();
    reader.onload = (ev) => {
        const preview = document.getElementById('groupAvatarPreview');
        const icon    = document.getElementById('groupAvatarIcon');
        preview.src   = ev.target.result;
        preview.style.display = 'block';
        icon.style.display    = 'none';
        window.currentGroupAvatar = ev.target.result;
    };
    reader.readAsDataURL(file);
};



// ═══════════════════════════════════════════════════════════════════
// ПАПКИ ЧАТОВ — полная версия
// ═══════════════════════════════════════════════════════════════════

const FOLDER_EMOJIS = ['📁','💼','🏠','❤️','⭐','🎮','📚','🎵','💡','🔒','🌍','🚀','👪','💸','🎨','🐾','📢','🏋️','🎯','🔥','🎉','🌸','⚡','🧠','🎭'];

// Встроенные папки (нельзя удалить)
const BUILT_IN_FOLDERS = [
    { id: 'all',     name: 'Все',      icon: '💬', locked: true },
    { id: 'inbox',   name: 'Входящие', icon: '📥', locked: true },
    { id: 'friends', name: 'Друзья',   icon: '👥', locked: true },
];

function loadFolders() {
    try {
        if (!username) return [];
        return JSON.parse(localStorage.getItem(`levart_folders_${username}`) || '[]');
    } catch { return []; }
}

function saveFolders(folders) {
    if (!username) return;
    localStorage.setItem(`levart_folders_${username}`, JSON.stringify(folders));
}

function getAllFolders() {
    return [...BUILT_IN_FOLDERS, ...loadFolders()];
}

// Кэш всех контактов (обновляется при каждом syncMyContacts)
window._allContacts  = [];
window._activeFolder = 'all';
let _folderUserMenuEl = null;

function getPreferredDisplayName(item, fallbackUser = '') {
    const first = String(item?.first_name || '').trim();
    const last = String(item?.last_name || '').trim();
    const full = `${first} ${last}`.trim();
    if (full) return full;
    const dn = String(item?.display_name || '').trim();
    if (dn) return dn;
    return String(fallbackUser || item?.username || '').trim();
}

function trimDisplayName(name, max = 22) {
    const s = String(name || '').trim();
    if (!s) return '';
    if (s.length <= max) return s;
    const first = s.split(/\s+/)[0] || s;
    return first.length <= max ? first : `${first.slice(0, Math.max(1, max - 1))}…`;
}

window.activateSearchMode = () => {
    const sb = document.querySelector('.sidebar');
    if (!sb) return;
    sb.classList.add('search-mode');
};

window.clearSearchMode = () => {
    const sb = document.querySelector('.sidebar');
    if (!sb) return;
    sb.classList.remove('search-mode');
};

window.toggleStoriesFocusMode = (on) => {
    const sb = document.querySelector('.sidebar');
    if (!sb) return;
    // Stories now have one stable layout; focus mode disabled.
    sb.classList.remove('stories-focus-mode');
};

function closeFolderUserMenu() {
    _folderUserMenuEl?.remove();
    _folderUserMenuEl = null;
}

window.toggleFolderUserMenu = async (anchorEl) => {
    if (_folderUserMenuEl) {
        closeFolderUserMenu();
        return;
    }
    const me = String(username || '').toLowerCase();
    if (!me) return;
    let profile = {};
    try {
        const r = await fetch(`/api/user_profile/${encodeURIComponent(me)}?me=${encodeURIComponent(me)}`);
        profile = await r.json();
    } catch {}
    const fullNameRaw = getPreferredDisplayName(profile, me);
    const fullName = trimDisplayName(fullNameRaw, 34) || me;
    const avatar = String(profile?.avatar || '').trim();
    const letter = (fullNameRaw || me).charAt(0).toUpperCase();
    const menu = document.createElement('div');
    menu.className = 'folder-user-menu';
    menu.innerHTML = `
      <div class="folder-user-menu-head">
        <div class="folder-user-menu-avatar">${avatar ? `<img src="${avatar}" alt="">` : letter}</div>
        <div>
          <div class="folder-user-menu-name">${String(fullName).replace(/</g, '&lt;')}</div>
          <div class="folder-user-menu-user">@${me}</div>
        </div>
      </div>
      <div class="folder-user-menu-actions">
        <button type="button" data-action="settings">⚙️ Настройки</button>
        <button type="button" data-action="devices">🖥️ Устройства (скоро)</button>
        <button type="button" data-action="subs">💎 Подписки (скоро)</button>
        <button type="button" class="danger" data-action="logout">🚪 Выйти</button>
      </div>
    `;
    document.body.appendChild(menu);
    const r = (anchorEl || document.getElementById('folderUserBtn'))?.getBoundingClientRect?.();
    const mw = menu.getBoundingClientRect().width || 320;
    const left = r ? Math.max(8, Math.min(window.innerWidth - mw - 8, r.left + (r.width / 2) - (mw / 2))) : 8;
    const top = r ? Math.min(window.innerHeight - menu.getBoundingClientRect().height - 8, r.bottom + 8) : 56;
    menu.style.left = `${Math.round(left)}px`;
    menu.style.top = `${Math.round(top)}px`;
    _folderUserMenuEl = menu;

    menu.addEventListener('click', (e) => {
        const btn = e.target.closest('button[data-action]');
        if (!btn) return;
        const action = btn.dataset.action;
        closeFolderUserMenu();
        if (action === 'settings') openSettings();
        else if (action === 'devices') showToast('Раздел устройств скоро появится', 'info');
        else if (action === 'subs') showToast('Раздел подписок скоро появится', 'info');
        else if (action === 'logout') logout();
    });
    setTimeout(() => {
        document.addEventListener('click', onFolderMenuDocClick, { once: true });
    }, 0);
};

function onFolderMenuDocClick(e) {
    if (!_folderUserMenuEl) return;
    if (e.target.closest('.folder-user-menu') || e.target.closest('#folderUserBtn')) {
        document.addEventListener('click', onFolderMenuDocClick, { once: true });
        return;
    }
    closeFolderUserMenu();
}

// ─── Фильтрация по папке ───────────────────────────────────────────
function filterContactsByFolder(contacts, folder) {
    if (!folder) return contacts;
    switch(folder.id) {
        case 'all':     return contacts;
        case 'inbox':   return contacts.filter(c => (c.unread_count || 0) > 0);
        case 'friends': return contacts.filter(c => c.is_friend === true);
        default: {
            const ids = folder.chats || [];
            if (ids.length === 0) return [];
            return contacts.filter(c => ids.includes(c.username));
        }
    }
}

// ─── Рендер folder bar ─────────────────────────────────────────────
function renderFolderBar() {
    const bar = document.getElementById('folderBar');
    if (!bar) return;
    bar.innerHTML = '';

    const me = String(username || '').toLowerCase();
    const meContact = (window._allContacts || []).find((c) => String(c.username || '').toLowerCase() === me) || {};
    const meName = trimDisplayName(getPreferredDisplayName(meContact, me), 12) || me;
    const meLetter = meName.charAt(0).toUpperCase();
    const meAvatar = String(meContact.avatar || '').trim();
    const userBtn = document.createElement('button');
    userBtn.id = 'folderUserBtn';
    userBtn.className = 'folder-user-btn';
    userBtn.type = 'button';
    userBtn.innerHTML = `
      <span class="folder-user-avatar">${meAvatar ? `<img src="${meAvatar}" alt="">` : meLetter}</span>
      <span class="folder-user-label">${String(meName).replace(/</g, '&lt;')}</span>
    `;
    userBtn.onclick = (e) => {
        e.stopPropagation();
        window.toggleFolderUserMenu(userBtn);
    };
    bar.appendChild(userBtn);

    const folders = getAllFolders();

    folders.forEach(f => {
        const item = document.createElement('div');
        item.className = 'folder-item' + (window._activeFolder === f.id ? ' active' : '');
        item.dataset.id   = f.id;
        item.dataset.name = f.name;
        item.innerHTML = `
            <span class="folder-emoji">${f.icon}</span>
            <span class="folder-label">${f.name.length > 9 ? f.name.slice(0,8)+'…' : f.name}</span>
        `;
        item.onclick = () => switchFolder(f.id);
        bar.appendChild(item);
    });

    const sep = document.createElement('div');
    sep.className = 'folder-sep';
    bar.appendChild(sep);

    const addBtn = document.createElement('div');
    addBtn.className = 'folder-add-btn';
    addBtn.innerHTML = '+';
    addBtn.title = 'Создать папку';
    addBtn.onclick = () => openFolderSettingsTab();
    bar.appendChild(addBtn);
}

// ─── Переключение папки ────────────────────────────────────────────
function switchFolder(folderId) {
    window._activeFolder = folderId;
    document.querySelectorAll('.folder-item').forEach(el =>
        el.classList.toggle('active', el.dataset.id === folderId)
    );
    const folder = getAllFolders().find(f => f.id === folderId);
    if (!folder) return;
    const filtered = filterContactsByFolder(window._allContacts, folder);
    renderContactsList(filtered);
}

// ─── Рендер списка контактов (общая функция) ───────────────────────
function renderContactsList(contacts) {
    const list = document.getElementById('contacts');
    if (!list) return;
    list.innerHTML = '';

    if (!contacts || contacts.length === 0) {
        list.innerHTML = `<div style="text-align:center;opacity:0.4;padding:28px 10px;font-size:13px;">
            <div style="font-size:32px;margin-bottom:8px;">📭</div>
            <div>Нет чатов в этой папке</div>
        </div>`;
        return;
    }

    contacts.forEach(c => renderContactItem(c, list));
}

// ─── Рендер одного контакта ────────────────────────────────────────
function renderContactItem(c, list) {
    const div = document.createElement('div');
    div.className = 'contact-item' + (window.currentChat === c.username ? ' active' : '');
    div.dataset.peer = c.username;

    let displayName = getPreferredDisplayName(c, c.username);
    displayName = trimDisplayName(displayName, 28);

    // Аватар — обёртка с position:relative для точки онлайн
    const avatarWrap = document.createElement('div');
    avatarWrap.style.cssText = 'position:relative;flex-shrink:0;';
    const avatarDiv = document.createElement('div');
    avatarDiv.className = 'contact-avatar';
    if (c.avatar) {
        const img = document.createElement('img');
        img.src = c.avatar;
        avatarDiv.appendChild(img);
    } else {
        avatarDiv.textContent = c.is_group ? '👥' : displayName.charAt(0).toUpperCase();
    }
    avatarWrap.appendChild(avatarDiv);
    // Точка онлайн (только для личных чатов)
    if (!c.is_group) {
        const dot = document.createElement('div');
        dot.className = 'status-dot';
        dot.style.cssText = 'position:absolute;bottom:1px;right:1px;width:10px;height:10px;border-radius:50%;border:2px solid var(--bg-panel);background:#6b7280;';
        const peerLower = c.username.toLowerCase();
        if (_onlineUsers.has(peerLower)) {
            dot.style.background = '#4ade80';
            dot.style.boxShadow = '0 0 4px #4ade80';
        }
        avatarWrap.appendChild(dot);
    }

    // Превью из кэша
    const previews  = loadMsgPreviews();
    const chatId    = c.username.startsWith('group_') ? c.username : [username, c.username].sort().join('_');
    const localPreview = previews[chatId] || c.last_message_preview || 'Нет сообщений';

    // Инфо
    const infoDiv = document.createElement('div');
    infoDiv.className = 'contact-info';
    infoDiv.innerHTML = `
        <div class="contact-name">
            ${displayName}
            ${c.is_group ? '<span style="font-size:10px;color:var(--accent);font-weight:600;margin-left:4px;">группа</span>' : ''}
        </div>
        <div class="contact-last-msg">${localPreview}</div>
    `;

    // Мета
    const metaDiv = document.createElement('div');
    metaDiv.className = 'contact-meta';
    if (c.last_time > 0) {
        const d   = new Date(c.last_time < 1e12 ? c.last_time * 1000 : c.last_time);
        const now = new Date();
        const timeEl = document.createElement('div');
        timeEl.className = 'contact-time';
        timeEl.textContent = d.toDateString() === now.toDateString()
            ? d.toLocaleTimeString('ru', { hour:'2-digit', minute:'2-digit' })
            : d.toLocaleDateString('ru', { day:'numeric', month:'short' });
        metaDiv.appendChild(timeEl);
    }
    if (c.unread_count > 0) {
        const badge = document.createElement('div');
        badge.className = 'unread-badge';
        badge.textContent = c.unread_count > 99 ? '99+' : c.unread_count;
        metaDiv.appendChild(badge);
    }

    // Кнопка ⋮
    const dotsBtn = document.createElement('button');
    dotsBtn.className = 'contact-dots-btn';
    dotsBtn.textContent = '⋮';
    dotsBtn.onclick = e => { e.stopPropagation(); showContactDropdown(e, c, div); };

    div.appendChild(avatarWrap);
    div.appendChild(infoDiv);
    div.appendChild(metaDiv);
    div.appendChild(dotsBtn);
    div.onclick = e => {
        if (e.target.classList.contains('contact-dots-btn')) return;
        document.querySelectorAll('.contact-item').forEach(i => i.classList.remove('active'));
        div.classList.add('active');
        window.openChat(c.username);
    };

    list.appendChild(div);
}

function restoreContactsFromOfflineCache(forUser) {
    const curUser = String(forUser || username || localStorage.getItem('username') || '').trim();
    if (!curUser) return false;
    try {
        const raw = localStorage.getItem(OFFLINE_META_KEY(curUser));
        const parsed = raw ? JSON.parse(raw) : null;
        const contacts = Array.isArray(parsed?.contacts) ? parsed.contacts : [];
        if (!contacts.length) return false;
        window._allContacts = contacts;
        const folder = getAllFolders().find(f => f.id === window._activeFolder) || BUILT_IN_FOLDERS[0];
        renderContactsList(filterContactsByFolder(window._allContacts, folder));
        renderFolderBar();
        return true;
    } catch {
        return false;
    }
}

// ─── Патч syncMyContacts ───────────────────────────────────────────
// Сохраняем исходную (если она есть) и перезаписываем
window.syncMyContacts = async function() {
    // username — это const в том же скрипте, берём его напрямую
    // Fallback на всякий случай через DOM
    let curUser = username;
    if (!curUser || curUser === 'undefined') {
        curUser = localStorage.getItem('username') || localStorage.getItem('chat_username');
    }
    if (!curUser || curUser === 'undefined') {
        const meEl = document.getElementById('me');
        curUser = meEl?.innerText?.trim();
    }
    if (!curUser || curUser === 'undefined') return;

    try {
        const res = await fetch(`/api/my_contacts/${curUser}?t=${Date.now()}`);
        window._allContacts = await res.json() || [];
        try {
            localStorage.setItem(OFFLINE_META_KEY(curUser), JSON.stringify({
                contacts: window._allContacts,
                updated_at: Date.now()
            }));
        } catch {}

        // Применяем активную папку
        const folder = getAllFolders().find(f => f.id === window._activeFolder) || BUILT_IN_FOLDERS[0];
        renderContactsList(filterContactsByFolder(window._allContacts, folder));
        renderFolderBar();
        // Фоновая загрузка превью (не блокирует UI)
        setTimeout(() => initAllPreviews(), 300);
        scheduleOfflinePreload();
    } catch(e) {
        console.error('syncMyContacts error:', e);
        restoreContactsFromOfflineCache(curUser);
    }
};

// ─── Открыть вкладку Папки в настройках ───────────────────────────
function openFolderSettingsTab() {
    openSettings();
    setTimeout(() => {
        document.querySelectorAll('.settings-nav-item').forEach(el =>
            el.classList.toggle('active', el.dataset.tab === 'folders')
        );
        document.querySelectorAll('.settings-tab').forEach(t => t.style.display = 'none');
        const tab = document.getElementById('tab-folders');
        if (tab) { tab.style.display = 'block'; renderFolderSettings(); }
    }, 60);
}

// ─── Настройки папок ───────────────────────────────────────────────
window.renderFolderSettings = function() {
    const listEl = document.getElementById('folderSettingsList');
    if (!listEl) return;
    listEl.innerHTML = '';

    getAllFolders().forEach(f => {
        const isBuiltIn = f.locked;
        const folderChats = isBuiltIn
            ? filterContactsByFolder(window._allContacts, f)
            : (f.chats || []).map(id => window._allContacts.find(c => c.username === id)).filter(Boolean);

        const item = document.createElement('div');
        item.className = 'folder-settings-item';
        item.dataset.fid = f.id;

        // Шапка папки
        const header = document.createElement('div');
        header.className = 'folder-settings-header';
        header.innerHTML = `
            <span class="folder-settings-emoji">${f.icon}</span>
            <div style="flex:1;">
                <div class="folder-settings-name">${f.name}</div>
                <div class="folder-settings-count">${folderChats.length} чат${folderChats.length===1?'':folderChats.length<5?'а':'ов'}</div>
            </div>
            ${isBuiltIn
                ? '<span class="folder-settings-locked" title="Встроенная">🔒</span>'
                : `
                    <button class="folder-chat-manage-btn" onclick="toggleFolderExpand('${f.id}',this)" title="Управление чатами">✎ Изменить</button>
                    <button class="folder-settings-del" onclick="deleteFolder('${f.id}')" title="Удалить">✕</button>
                `
            }
        `;
        item.appendChild(header);

        // Список чатов в папке (для кастомных — разворачиваемый)
        if (!isBuiltIn) {
            const chatsArea = document.createElement('div');
            chatsArea.id = `folder-expand-${f.id}`;
            chatsArea.style.display = 'none';
            chatsArea.innerHTML = renderFolderEditArea(f);
            item.appendChild(chatsArea);
        }

        listEl.appendChild(item);
    });

    // Рендер эмодзи-пикера
    const picker = document.getElementById('folderEmojiPicker');
    if (picker) {
        picker.innerHTML = '';
        if (!window._selectedFolderEmoji) window._selectedFolderEmoji = '📁';
        FOLDER_EMOJIS.forEach(e => {
            const btn = document.createElement('button');
            btn.className = 'emoji-opt' + (window._selectedFolderEmoji === e ? ' active' : '');
            btn.textContent = e;
            btn.onclick = () => {
                window._selectedFolderEmoji = e;
                picker.querySelectorAll('.emoji-opt').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                const prev = document.getElementById('folderEmojiPreview');
                const inp  = document.getElementById('folderEmojiCustom');
                if (prev) prev.textContent = e;
                if (inp)  inp.value = '';
            };
            picker.appendChild(btn);
        });
    }

    const customInput = document.getElementById('folderEmojiCustom');
    if (customInput) {
        customInput.oninput = () => {
            const v = [...customInput.value].slice(0,2).join('');
            if (v) {
                window._selectedFolderEmoji = v;
                const prev = document.getElementById('folderEmojiPreview');
                if (prev) prev.textContent = v;
                document.querySelectorAll('.emoji-opt').forEach(b => b.classList.remove('active'));
            }
        };
    }
};

function renderFolderEditArea(folder) {
    const chatsInFolder = (folder.chats || [])
        .map(id => window._allContacts.find(c => c.username === id))
        .filter(Boolean);

    const availableChats = window._allContacts.filter(c => !(folder.chats||[]).includes(c.username));

    const inFolderRows = chatsInFolder.map(c => {
        const dn = c.display_name || c.username;
        const icon = c.is_group ? '👥' : dn.charAt(0).toUpperCase();
        return `
            <div class="folder-chat-in-folder-row">
                <div style="width:24px;height:24px;border-radius:50%;background:linear-gradient(135deg,var(--accent),#8b5cf6);display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:white;flex-shrink:0;">${icon}</div>
                <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${dn}</span>
                <button class="folder-chat-manage-btn remove" onclick="removeChatFromFolder('${folder.id}','${c.username}')">✕ Убрать</button>
            </div>
        `;
    }).join('') || '<div style="opacity:0.4;font-size:11px;padding:4px 6px;">Нет чатов</div>';

    const addRows = availableChats.map(c => {
        const dn = c.display_name || c.username;
        const icon = c.is_group ? '👥' : dn.charAt(0).toUpperCase();
        return `
            <div class="folder-add-chat-row" onclick="addChatToFolder('${folder.id}','${c.username}')">
                <div style="width:24px;height:24px;border-radius:50%;background:linear-gradient(135deg,var(--accent),#8b5cf6);display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:white;flex-shrink:0;">${icon}</div>
                <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${dn}</span>
                <span style="color:var(--accent);font-size:11px;">+ Добавить</span>
            </div>
        `;
    }).join('') || '<div style="opacity:0.4;font-size:11px;padding:4px;">Все чаты уже в папке</div>';

    return `
        <div class="folder-chats-in-folder">
            <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-dim);margin-bottom:4px;">В папке:</div>
            ${inFolderRows}
        </div>
        <div class="folder-add-chat-area">
            <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-dim);">Добавить чат:</div>
            <div class="folder-add-chat-list">${addRows}</div>
        </div>
    `;
}

window.toggleFolderExpand = (folderId, btn) => {
    const area = document.getElementById(`folder-expand-${folderId}`);
    if (!area) return;
    const isOpen = area.style.display !== 'none';
    area.style.display = isOpen ? 'none' : 'block';
    if (!isOpen) {
        const folder = loadFolders().find(f => f.id === folderId);
        if (folder) area.innerHTML = renderFolderEditArea(folder);
        btn.closest('.folder-settings-item')?.classList.add('folder-settings-item-open');
    } else {
        btn.closest('.folder-settings-item')?.classList.remove('folder-settings-item-open');
    }
};

window.addChatToFolder = (folderId, chatUsername) => {
    const folders = loadFolders();
    const folder  = folders.find(f => f.id === folderId);
    if (!folder) return;
    if (!folder.chats) folder.chats = [];
    if (!folder.chats.includes(chatUsername)) {
        folder.chats.push(chatUsername);
        saveFolders(folders);
        showToast('Чат добавлен в папку ✅');
        renderFolderSettings();
        // Снова открываем панель редактирования
        const area = document.getElementById(`folder-expand-${folderId}`);
        if (area) { area.style.display = 'block'; area.innerHTML = renderFolderEditArea(folder); }
        renderFolderBar();
    }
};

window.removeChatFromFolder = (folderId, chatUsername) => {
    const folders = loadFolders();
    const folder  = folders.find(f => f.id === folderId);
    if (!folder) return;
    folder.chats = (folder.chats || []).filter(id => id !== chatUsername);
    saveFolders(folders);
    showToast('Чат убран из папки');
    renderFolderSettings();
    const area = document.getElementById(`folder-expand-${folderId}`);
    if (area) { area.style.display = 'block'; area.innerHTML = renderFolderEditArea(folder); }
    // Если смотрим эту папку — обновить список
    if (window._activeFolder === folderId) switchFolder(folderId);
    renderFolderBar();
};

window.createFolder = () => {
    const name = (document.getElementById('folderNameInput')?.value || '').trim();
    if (!name) { showToast('Введите название папки', 'error'); return; }

    const icon  = window._selectedFolderEmoji || '📁';
    const chats = [...document.querySelectorAll('#folderChatList input[type="checkbox"]:checked')].map(cb => cb.value);

    const folder = { id: `folder_${Date.now()}`, name, icon, locked: false, chats };
    const folders = loadFolders();
    folders.push(folder);
    saveFolders(folders);

    // Сброс формы
    const nameInput = document.getElementById('folderNameInput');
    if (nameInput) nameInput.value = '';
    window._selectedFolderEmoji = '📁';

    showToast(`✅ Папка "${name}" создана!`);
    renderFolderSettings();
    renderFolderBar();
};

window.deleteFolder = (folderId) => {
    const folders = loadFolders().filter(f => f.id !== folderId);
    saveFolders(folders);
    if (window._activeFolder === folderId) {
        window._activeFolder = 'all';
    }
    renderFolderSettings();
    renderFolderBar();
    switchFolder(window._activeFolder);
    showToast('Папка удалена');
};

// Инициализация при старте
setTimeout(() => {
    renderFolderBar();
    switchFolder('all');
}, 800);

fetch('/api/online_status').then(r => r.json()).then(list => {
    list.forEach(u => {
        _onlineUsers.add(u.toLowerCase());
        updateOnlineIndicator(u.toLowerCase(), true);
    });
}).catch(() => {});

let _storiesFeed = [];

let _storyEditorState = null;
let _storyEditorCleanup = null;
let _storyEditorRenderRaf = 0;

function scheduleStoryEditorRender() {
    if (_storyEditorRenderRaf) return;
    _storyEditorRenderRaf = requestAnimationFrame(() => {
        _storyEditorRenderRaf = 0;
        renderStoryEditorLayers();
    });
}

function storyEditorSetTool(tool) {
    if (!_storyEditorState) return;
    _storyEditorState.tool = String(tool || 'move');
    const modal = document.getElementById('storyEditorModal');
    if (!modal) return;
    modal.querySelectorAll('[data-story-tool]').forEach((btn) => {
        btn.classList.toggle('active', btn.getAttribute('data-story-tool') === _storyEditorState.tool);
    });
}

function storyEditorSyncControls() {
    if (!_storyEditorState) return;
    const i = Number(_storyEditorState.selectedLayer ?? -1);
    const l = i >= 0 ? _storyEditorState.layers[i] : null;
    const textInput = document.getElementById('storyLayerInput');
    const colorInput = document.getElementById('storyLayerColor');
    const sizeInput = document.getElementById('storyLayerSize');
    const controls = document.getElementById('storyEditorControls');
    const delBtn = document.getElementById('storyLayerDeleteBtn');
    const cloneBtn = document.getElementById('storyLayerCloneBtn');
    const sizeDownBtn = document.getElementById('storySizeDownBtn');
    const sizeUpBtn = document.getElementById('storySizeUpBtn');
    const disabledByVideo = !!_storyEditorState.isVideo;
    if (textInput) textInput.value = l && l.kind !== 'image' ? (l.text || '') : '';
    if (colorInput) colorInput.value = l?.color || '#ffffff';
    if (sizeInput) sizeInput.value = String(Number(l?.size || (l?.kind === 'image' ? 108 : 34)));
    if (controls) controls.classList.toggle('disabled', !l || disabledByVideo);
    if (textInput) textInput.disabled = !l || disabledByVideo || l?.kind === 'image';
    if (colorInput) colorInput.disabled = !l || disabledByVideo || l?.kind === 'image';
    if (sizeInput) sizeInput.disabled = !l || disabledByVideo;
    if (delBtn) delBtn.disabled = !l || disabledByVideo;
    if (cloneBtn) cloneBtn.disabled = !l || disabledByVideo;
    if (sizeDownBtn) sizeDownBtn.disabled = !l || disabledByVideo;
    if (sizeUpBtn) sizeUpBtn.disabled = !l || disabledByVideo;
}

function suppressNativeTitleTooltips(root = document) {
    if (!root?.querySelectorAll) return;
    root.querySelectorAll('[title]').forEach((el) => {
        const titleText = el.getAttribute('title');
        if (!titleText) return;
        el.setAttribute('data-native-title', titleText);
        el.removeAttribute('title');
    });
}

function initNativeTooltipSuppression() {
    suppressNativeTitleTooltips(document);
    const obs = new MutationObserver((mutations) => {
        for (const m of mutations) {
            if (m.type === 'attributes' && m.attributeName === 'title' && m.target instanceof Element) {
                const t = m.target.getAttribute('title');
                if (t) {
                    m.target.setAttribute('data-native-title', t);
                    m.target.removeAttribute('title');
                }
                continue;
            }
            if (m.type === 'childList' && m.addedNodes?.length) {
                m.addedNodes.forEach((node) => {
                    if (!(node instanceof Element)) return;
                    if (node.hasAttribute('title')) {
                        const t = node.getAttribute('title');
                        if (t) node.setAttribute('data-native-title', t);
                        node.removeAttribute('title');
                    }
                    suppressNativeTitleTooltips(node);
                });
            }
        }
    });
    obs.observe(document.documentElement || document.body, {
        subtree: true,
        childList: true,
        attributes: true,
        attributeFilter: ['title']
    });
}

function storyEditorEscape(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function closeStoryEditor() {
    try { _storyEditorCleanup?.(); } catch {}
    _storyEditorCleanup = null;
    document.getElementById('storyEditorModal')?.remove();
    try {
        document.documentElement.style.overflow = '';
        document.body.style.overflow = '';
    } catch {}
    if (_storyEditorRenderRaf) {
        cancelAnimationFrame(_storyEditorRenderRaf);
        _storyEditorRenderRaf = 0;
    }
    _storyEditorState = null;
}

function getStoryEditorBgMetrics(targetW, targetH, offsetScaleX = 1, offsetScaleY = 1) {
    if (!_storyEditorState?.imageElement) return null;
    const img = _storyEditorState.imageElement;
    const fitScale = Math.min(targetW / Math.max(1, img.width), targetH / Math.max(1, img.height));
    const scale = Math.max(0.72, Math.min(4, Number(_storyEditorState.bgScale || 1)));
    const dw = img.width * fitScale * scale;
    const dh = img.height * fitScale * scale;
    const maxX = Math.max(0, Math.abs(targetW - dw) / 2);
    const maxY = Math.max(0, Math.abs(targetH - dh) / 2);
    const bgX = Math.max(-maxX, Math.min(maxX, Number(_storyEditorState.bgX || 0) * offsetScaleX));
    const bgY = Math.max(-maxY, Math.min(maxY, Number(_storyEditorState.bgY || 0) * offsetScaleY));
    const dx = (targetW - dw) / 2 + bgX;
    const dy = (targetH - dh) / 2 + bgY;
    return { dx, dy, dw, dh, bgX, bgY };
}

function drawStoryEditorBaseImage(canvas, ctx) {
    if (!_storyEditorState?.imageElement || !canvas || !ctx) return;
    const displayW = Math.max(1, Number(_storyEditorState.editorWidth || canvas.clientWidth || canvas.width || 1));
    const displayH = Math.max(1, Number(_storyEditorState.editorHeight || canvas.clientHeight || canvas.height || 1));
    const m = getStoryEditorBgMetrics(displayW, displayH, 1, 1);
    if (!m) return;
    _storyEditorState.bgX = m.bgX;
    _storyEditorState.bgY = m.bgY;
    ctx.clearRect(0, 0, displayW, displayH);
    ctx.drawImage(_storyEditorState.imageElement, m.dx, m.dy, m.dw, m.dh);
}

function initStoryEditorBackgroundDrag(stage, canvas) {
    if (!stage || !canvas || !_storyEditorState) return;
    let active = false;
    let startX = 0;
    let startY = 0;
    let startBgX = 0;
    let startBgY = 0;
    let pointerId = null;
    let pinchDist = 0;
    let pinchScale = 1;
    const ctx = canvas.getContext('2d');
    const redraw = () => {
        drawStoryEditorBaseImage(canvas, ctx);
        renderStoryEditorLayers();
    };
    const getTouchDist = (t0, t1) => {
        const dx = t1.clientX - t0.clientX;
        const dy = t1.clientY - t0.clientY;
        return Math.hypot(dx, dy);
    };
    const onPointerMove = (e) => {
        if (!active || e.pointerId !== pointerId) return;
        _storyEditorState.bgX = startBgX + (e.clientX - startX);
        _storyEditorState.bgY = startBgY + (e.clientY - startY);
        _storyEditorState.bgUserMoved = true;
        redraw();
        if (e.cancelable) e.preventDefault();
    };
    const onPointerUp = (e) => {
        if (!active || e.pointerId !== pointerId) return;
        active = false;
        pointerId = null;
        window.removeEventListener('pointermove', onPointerMove);
        window.removeEventListener('pointerup', onPointerUp);
        window.removeEventListener('pointercancel', onPointerUp);
    };
    const onPointerDown = (e) => {
        if (_storyEditorState.selectedLayer >= 0) return;
        if (e.target?.closest?.('.story-editor-layer')) return;
        const allowed = e.target === canvas || e.target === stage || e.target?.id === 'storyEditorLayers';
        if (!allowed) return;
        active = true;
        pointerId = e.pointerId;
        startX = e.clientX;
        startY = e.clientY;
        startBgX = Number(_storyEditorState.bgX || 0);
        startBgY = Number(_storyEditorState.bgY || 0);
        try { canvas.setPointerCapture?.(e.pointerId); } catch {}
        window.addEventListener('pointermove', onPointerMove, { passive: false });
        window.addEventListener('pointerup', onPointerUp);
        window.addEventListener('pointercancel', onPointerUp);
        if (e.cancelable) e.preventDefault();
    };
    const onWheel = (e) => {
        const t = e.target;
        const onLayer = !!t?.closest?.('.story-editor-layer');
        const onAllowedSurface = t === canvas || t === stage || t?.id === 'storyEditorLayers';
        if (!onAllowedSurface || onLayer) return;
        if (_storyEditorState?.selectedLayer >= 0) return;
        const next = Number(_storyEditorState.bgScale || 1) + (e.deltaY < 0 ? 0.08 : -0.08);
        _storyEditorState.bgScale = Math.max(0.72, Math.min(4, next));
        _storyEditorState.bgUserMoved = true;
        redraw();
        if (e.cancelable) e.preventDefault();
    };
    const onTouchStart = (e) => {
        if (_storyEditorState.selectedLayer >= 0) return;
        if (e.touches?.length !== 2) return;
        pinchDist = getTouchDist(e.touches[0], e.touches[1]);
        pinchScale = Number(_storyEditorState.bgScale || 1);
    };
    const onTouchMove = (e) => {
        if (_storyEditorState.selectedLayer >= 0) return;
        if (e.touches?.length !== 2 || pinchDist <= 0) return;
        const d = getTouchDist(e.touches[0], e.touches[1]);
        const ratio = d / Math.max(1, pinchDist);
        _storyEditorState.bgScale = Math.max(0.72, Math.min(4, pinchScale * ratio));
        _storyEditorState.bgUserMoved = true;
        redraw();
        if (e.cancelable) e.preventDefault();
    };
    const onTouchEnd = (e) => {
        if ((e.touches?.length || 0) < 2) pinchDist = 0;
    };
    canvas.addEventListener('pointerdown', onPointerDown, { passive: false });
    stage.addEventListener('pointerdown', onPointerDown, { passive: false });
    canvas.addEventListener('wheel', onWheel, { passive: false });
    stage.addEventListener('wheel', onWheel, { passive: false });
    stage.addEventListener('touchstart', onTouchStart, { passive: false });
    stage.addEventListener('touchmove', onTouchMove, { passive: false });
    stage.addEventListener('touchend', onTouchEnd, { passive: true });
    if (_storyEditorState) {
        const cleanup = _storyEditorState.cleanupFns || (_storyEditorState.cleanupFns = []);
        cleanup.push(() => {
            canvas.removeEventListener('pointerdown', onPointerDown);
            stage.removeEventListener('pointerdown', onPointerDown);
            canvas.removeEventListener('wheel', onWheel);
            stage.removeEventListener('wheel', onWheel);
            stage.removeEventListener('touchstart', onTouchStart);
            stage.removeEventListener('touchmove', onTouchMove);
            stage.removeEventListener('touchend', onTouchEnd);
            window.removeEventListener('pointermove', onPointerMove);
            window.removeEventListener('pointerup', onPointerUp);
            window.removeEventListener('pointercancel', onPointerUp);
        });
    }
}

function getStoryLayerHalfSize(layerObj, stageEl, layerEl = null) {
    const stageRect = stageEl?.getBoundingClientRect?.();
    const minHalf = 10;
    if (!stageRect) return { halfW: minHalf, halfH: minHalf };
    if (layerEl) {
        const lr = layerEl.getBoundingClientRect();
        if (lr.width > 0 && lr.height > 0) {
            return { halfW: Math.max(minHalf, lr.width / 2), halfH: Math.max(minHalf, lr.height / 2) };
        }
    }
    if (layerObj?.kind === 'image') {
        const size = Number(layerObj.size || 108);
        return { halfW: Math.max(minHalf, size / 2), halfH: Math.max(minHalf, size / 2) };
    }
    const txt = String(layerObj?.text || 'Текст');
    const size = Number(layerObj?.size || 34);
    const boxW = Math.max(40, Number(layerObj?.boxW || 180));
    const boxH = Math.max(24, Number(layerObj?.boxH || Math.max(42, Math.round(size * 1.25))));
    const approxW = Math.max(size * 0.7, Math.min(boxW, txt.length * size * 0.54));
    const approxH = Math.max(size * 1.15, Math.min(boxH, size * 6));
    return { halfW: Math.max(minHalf, approxW / 2), halfH: Math.max(minHalf, approxH / 2) };
}

function clampStoryLayerToStage(layerObj, stageEl, layerEl = null) {
    if (!layerObj || !stageEl) return;
    const rect = stageEl.getBoundingClientRect();
    if (!rect.width || !rect.height) return;
    const { halfW, halfH } = getStoryLayerHalfSize(layerObj, stageEl, layerEl);
    const minX = Math.max(halfW / rect.width, 0);
    const maxX = Math.min(1 - halfW / rect.width, 1);
    const minY = Math.max(halfH / rect.height, 0);
    const maxY = Math.min(1 - halfH / rect.height, 1);
    layerObj.x = Math.max(minX, Math.min(maxX, Number(layerObj.x ?? 0.5)));
    layerObj.y = Math.max(minY, Math.min(maxY, Number(layerObj.y ?? 0.5)));
    if (layerEl) {
        layerEl.style.left = `${layerObj.x * 100}%`;
        layerEl.style.top = `${layerObj.y * 100}%`;
    }
}

function initStoryLayerDrag(layerEl, layerObj) {
    if (!layerEl || !layerObj) return;
    let active = false;
    let pointerActive = false;
    let pointerId = null;
    let pinchActive = false;
    let moved = false;
    let sx = 0;
    let sy = 0;
    let ox = 0;
    let oy = 0;
    let pinchStartDist = 0;
    let pinchStartSize = 0;
    let pinchStartW = 0;
    let pinchStartH = 0;
    const stage = document.getElementById('storyEditorStage');
    const touchDist = (t0, t1) => {
        const dx = (t1.clientX - t0.clientX);
        const dy = (t1.clientY - t0.clientY);
        return Math.hypot(dx, dy);
    };
    const syncLayerInputs = () => {
        const txt = document.getElementById('storyLayerInput');
        const col = document.getElementById('storyLayerColor');
        const siz = document.getElementById('storyLayerSize');
        if (txt) txt.value = layerObj.kind === 'image' ? '' : (layerObj.text || '');
        if (col && layerObj.color) col.value = layerObj.color;
        if (siz) siz.value = String(Number(layerObj.size || (layerObj.kind === 'image' ? 108 : 34)));
    };
    const onMove = (ev) => {
        if (!stage) return;
        if (active && !pinchActive && ev.touches && ev.touches.length >= 2) {
            pinchActive = true;
            active = false;
            pinchStartDist = touchDist(ev.touches[0], ev.touches[1]);
            pinchStartSize = Number(layerObj.size || (layerObj.kind === 'image' ? 108 : 34));
            pinchStartW = Number(layerObj.boxW || 180);
            pinchStartH = Number(layerObj.boxH || Math.max(42, Math.round(Number(layerObj.size || 34) * 1.25)));
        }
        if (pinchActive && ev.touches && ev.touches.length >= 2) {
            const d = touchDist(ev.touches[0], ev.touches[1]);
            const ratio = Math.max(0.4, Math.min(2.8, d / Math.max(1, pinchStartDist)));
            if (layerObj.kind === 'image') {
                layerObj.size = Math.max(36, Math.min(360, Math.round(pinchStartSize * ratio)));
            } else {
                layerObj.size = Math.max(16, Math.min(220, Math.round(pinchStartSize * ratio)));
            }
            syncLayerInputs();
            storyEditorSyncControls();
            scheduleStoryEditorRender();
            if (ev.cancelable) ev.preventDefault();
            return;
        }
        if (!active) return;
        const pt = ev.touches?.[0] || ev;
        const rect = stage.getBoundingClientRect();
        const nx = Math.max(0, Math.min(1, (ox + (pt.clientX - sx)) / rect.width));
        const ny = Math.max(0, Math.min(1, (oy + (pt.clientY - sy)) / rect.height));
        const pxDx = Math.abs(pt.clientX - sx);
        const pxDy = Math.abs(pt.clientY - sy);
        if (pxDx > 2 || pxDy > 2) moved = true;
        layerObj.x = nx;
        layerObj.y = ny;
        layerEl.style.left = `${(nx * 100)}%`;
        layerEl.style.top = `${(ny * 100)}%`;
        layerEl.style.transform = 'translate3d(-50%, -50%, 0)';
        clampStoryLayerToStage(layerObj, stage, layerEl);
        if (ev.cancelable) ev.preventDefault();
    };
    const onUp = () => {
        const wasActive = active || pinchActive;
        active = false;
        pointerActive = false;
        pointerId = null;
        pinchActive = false;
        if (moved && _storyEditorState) _storyEditorState.justDraggedUntil = Date.now() + 220;
        if (wasActive && _storyEditorState) _storyEditorState.justDraggedUntil = Date.now() + 220;
        if (!moved) scheduleStoryEditorRender();
        layerEl.classList.remove('dragging');
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
        window.removeEventListener('touchmove', onMove);
        window.removeEventListener('touchend', onUp);
        layerEl.removeEventListener('touchmove', onMove);
        layerEl.removeEventListener('touchend', onUp);
        window.removeEventListener('pointermove', onPointerMove);
        window.removeEventListener('pointerup', onPointerUp);
        window.removeEventListener('pointercancel', onPointerUp);
        layerEl.removeEventListener('pointermove', onPointerMove);
        layerEl.removeEventListener('pointerup', onPointerUp);
        layerEl.removeEventListener('pointercancel', onPointerUp);
    };
    const onDown = (ev) => {
        if (_storyEditorState?.tool && _storyEditorState.tool !== 'move') return;
        const idxRaw = Number(layerEl.dataset.layerIndex || -1);
        if (Number.isFinite(idxRaw) && idxRaw >= 0 && _storyEditorState) {
            const changed = _storyEditorState.selectedLayer !== idxRaw;
            _storyEditorState.selectedLayer = idxRaw;
            syncLayerInputs();
            storyEditorSyncControls();
            if (changed) {
                const host = document.getElementById('storyEditorLayers');
                host?.querySelectorAll('.story-editor-layer').forEach((n, i) => {
                    n.classList.toggle('active', i === idxRaw);
                });
            }
        }
        if (ev.cancelable) ev.preventDefault();
        ev.stopPropagation();
        layerEl.classList.add('dragging');
        moved = false;
        if (ev.touches && ev.touches.length >= 2) {
            pinchActive = true;
            active = false;
            pinchStartDist = touchDist(ev.touches[0], ev.touches[1]);
            pinchStartSize = Number(layerObj.size || (layerObj.kind === 'image' ? 108 : 34));
            pinchStartW = Number(layerObj.boxW || 180);
            pinchStartH = Number(layerObj.boxH || Math.max(42, Math.round(Number(layerObj.size || 34) * 1.25)));
            window.addEventListener('touchmove', onMove, { passive: false });
            window.addEventListener('touchend', onUp);
            return;
        }
        const pt = ev.touches?.[0] || ev;
        const rect = stage?.getBoundingClientRect();
        if (!rect) return;
        active = true;
        pinchActive = false;
        sx = pt.clientX;
        sy = pt.clientY;
        ox = layerObj.x * rect.width;
        oy = layerObj.y * rect.height;
        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
        window.addEventListener('touchmove', onMove, { passive: false });
        window.addEventListener('touchend', onUp);
        layerEl.addEventListener('touchmove', onMove, { passive: false });
        layerEl.addEventListener('touchend', onUp);
    };
    const onPointerMove = (ev) => {
        if (!pointerActive || ev.pointerId !== pointerId) return;
        onMove(ev);
    };
    const onPointerUp = (ev) => {
        if (!pointerActive || ev.pointerId !== pointerId) return;
        onUp();
    };
    const onPointerDown = (ev) => {
        if (_storyEditorState?.tool && _storyEditorState.tool !== 'move') return;
        const idxRaw = Number(layerEl.dataset.layerIndex || -1);
        if (Number.isFinite(idxRaw) && idxRaw >= 0 && _storyEditorState) {
            const changed = _storyEditorState.selectedLayer !== idxRaw;
            _storyEditorState.selectedLayer = idxRaw;
            syncLayerInputs();
            storyEditorSyncControls();
            if (changed) {
                const host = document.getElementById('storyEditorLayers');
                host?.querySelectorAll('.story-editor-layer').forEach((n, i) => {
                    n.classList.toggle('active', i === idxRaw);
                });
            }
        }
        if (ev.cancelable) ev.preventDefault();
        ev.stopPropagation();
        const rect = stage?.getBoundingClientRect();
        if (!rect) return;
        moved = false;
        layerEl.classList.add('dragging');
        pointerActive = true;
        pointerId = ev.pointerId;
        active = true;
        pinchActive = false;
        sx = ev.clientX;
        sy = ev.clientY;
        ox = layerObj.x * rect.width;
        oy = layerObj.y * rect.height;
        try { layerEl.setPointerCapture?.(ev.pointerId); } catch {}
        window.addEventListener('pointermove', onPointerMove, { passive: false });
        window.addEventListener('pointerup', onPointerUp);
        window.addEventListener('pointercancel', onPointerUp);
        layerEl.addEventListener('pointermove', onPointerMove, { passive: false });
        layerEl.addEventListener('pointerup', onPointerUp);
        layerEl.addEventListener('pointercancel', onPointerUp);
    };
    layerEl.addEventListener('mousedown', onDown);
    layerEl.addEventListener('touchstart', onDown, { passive: false });
    layerEl.addEventListener('pointerdown', onPointerDown, { passive: false });
}

function initStoryTextResizeHandle(handleEl, layerEl, layerObj) {
    if (!handleEl || !layerEl || !layerObj || layerObj.kind !== 'text') return;
    let active = false;
    let sx = 0;
    let sy = 0;
    let startW = 0;
    let startH = 0;
    let pointerActive = false;
    let pointerId = null;
    const onMove = (ev) => {
        if (!active) return;
        const pt = ev.touches?.[0] || ev;
        const dx = pt.clientX - sx;
        const dy = pt.clientY - sy;
        layerObj.boxW = Math.max(36, Math.min(640, Math.round(startW + dx)));
        layerObj.boxH = Math.max(22, Math.min(520, Math.round(startH + dy)));
        scheduleStoryEditorRender();
        storyEditorSyncControls();
        if (ev.cancelable) ev.preventDefault();
    };
    const onUp = () => {
        active = false;
        pointerActive = false;
        pointerId = null;
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
        window.removeEventListener('touchmove', onMove);
        window.removeEventListener('touchend', onUp);
        window.removeEventListener('pointermove', onPointerMove);
        window.removeEventListener('pointerup', onPointerUp);
        window.removeEventListener('pointercancel', onPointerUp);
    };
    const onDown = (ev) => {
        active = true;
        const pt = ev.touches?.[0] || ev;
        sx = pt.clientX;
        sy = pt.clientY;
        startW = Number(layerObj.boxW || 180);
        startH = Number(layerObj.boxH || Math.max(42, Math.round(Number(layerObj.size || 34) * 1.25)));
        if (ev.cancelable) ev.preventDefault();
        ev.stopPropagation();
        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
        window.addEventListener('touchmove', onMove, { passive: false });
        window.addEventListener('touchend', onUp);
    };
    const onPointerMove = (ev) => {
        if (!pointerActive || ev.pointerId !== pointerId) return;
        onMove(ev);
    };
    const onPointerUp = (ev) => {
        if (!pointerActive || ev.pointerId !== pointerId) return;
        onUp();
    };
    const onPointerDown = (ev) => {
        active = true;
        pointerActive = true;
        pointerId = ev.pointerId;
        sx = ev.clientX;
        sy = ev.clientY;
        startW = Number(layerObj.boxW || 180);
        startH = Number(layerObj.boxH || Math.max(42, Math.round(Number(layerObj.size || 34) * 1.25)));
        if (ev.cancelable) ev.preventDefault();
        ev.stopPropagation();
        try { handleEl.setPointerCapture?.(ev.pointerId); } catch {}
        window.addEventListener('pointermove', onPointerMove, { passive: false });
        window.addEventListener('pointerup', onPointerUp);
        window.addEventListener('pointercancel', onPointerUp);
    };
    handleEl.addEventListener('mousedown', onDown);
    handleEl.addEventListener('touchstart', onDown, { passive: false });
    handleEl.addEventListener('pointerdown', onPointerDown, { passive: false });
}

function initStoryLayerResizeHandle(handleEl, layerEl, layerObj) {
    if (!handleEl || !layerEl || !layerObj) return;
    if (layerObj.kind === 'text') {
        initStoryTextResizeHandle(handleEl, layerEl, layerObj);
        return;
    }
    let active = false;
    let pointerActive = false;
    let pointerId = null;
    let sx = 0;
    let sy = 0;
    let startSize = 0;
    const onMove = (ev) => {
        if (!active) return;
        const pt = ev.touches?.[0] || ev;
        const dx = pt.clientX - sx;
        const dy = pt.clientY - sy;
        const delta = (dx + dy) / 2;
        layerObj.size = Math.max(36, Math.min(420, Math.round(startSize + delta)));
        storyEditorSyncControls();
        scheduleStoryEditorRender();
        if (ev.cancelable) ev.preventDefault();
    };
    const onUp = () => {
        active = false;
        pointerActive = false;
        pointerId = null;
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
        window.removeEventListener('touchmove', onMove);
        window.removeEventListener('touchend', onUp);
        window.removeEventListener('pointermove', onPointerMove);
        window.removeEventListener('pointerup', onPointerUp);
        window.removeEventListener('pointercancel', onPointerUp);
    };
    const onDown = (ev) => {
        active = true;
        const pt = ev.touches?.[0] || ev;
        sx = pt.clientX;
        sy = pt.clientY;
        startSize = Number(layerObj.size || 108);
        if (ev.cancelable) ev.preventDefault();
        ev.stopPropagation();
        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
        window.addEventListener('touchmove', onMove, { passive: false });
        window.addEventListener('touchend', onUp);
    };
    const onPointerMove = (ev) => {
        if (!pointerActive || ev.pointerId !== pointerId) return;
        onMove(ev);
    };
    const onPointerUp = (ev) => {
        if (!pointerActive || ev.pointerId !== pointerId) return;
        onUp();
    };
    const onPointerDown = (ev) => {
        active = true;
        pointerActive = true;
        pointerId = ev.pointerId;
        sx = ev.clientX;
        sy = ev.clientY;
        startSize = Number(layerObj.size || 108);
        if (ev.cancelable) ev.preventDefault();
        ev.stopPropagation();
        try { handleEl.setPointerCapture?.(ev.pointerId); } catch {}
        window.addEventListener('pointermove', onPointerMove, { passive: false });
        window.addEventListener('pointerup', onPointerUp);
        window.addEventListener('pointercancel', onPointerUp);
    };
    handleEl.addEventListener('mousedown', onDown);
    handleEl.addEventListener('touchstart', onDown, { passive: false });
    handleEl.addEventListener('pointerdown', onPointerDown, { passive: false });
}

function renderStoryEditorLayers() {
    const host = document.getElementById('storyEditorLayers');
    const stage = document.getElementById('storyEditorStage');
    if (!host || !_storyEditorState) return;
    host.innerHTML = '';
    _storyEditorState.layers.forEach((l, idx) => {
        const el = document.createElement('div');
        el.className = `story-editor-layer${_storyEditorState.selectedLayer === idx ? ' active' : ''}`;
        el.dataset.layerIndex = String(idx);
        el.style.left = `${l.x * 100}%`;
        el.style.top = `${l.y * 100}%`;
        if (l.kind === 'image' && l.src) {
            el.classList.add('sticker');
            const size = Number(l.size || 96);
            el.style.width = `${size}px`;
            el.style.height = `${size}px`;
            el.style.backgroundImage = `url('${l.src}')`;
            el.style.backgroundSize = 'contain';
            el.style.backgroundRepeat = 'no-repeat';
            el.style.backgroundPosition = 'center';
            el.textContent = '';
            if (_storyEditorState.selectedLayer === idx) {
                const handle = document.createElement('span');
                handle.className = 'story-editor-text-resize-handle';
                handle.title = 'Изменить размер';
                el.appendChild(handle);
                initStoryLayerResizeHandle(handle, el, l);
            }
        } else {
            el.style.color = l.color;
            el.style.fontSize = `${l.size}px`;
            const stageW = Math.max(120, Number(stage?.clientWidth || 0) - 16);
            const boxW = Math.max(40, Math.min(stageW, Number(l.boxW || 180)));
            const boxH = Math.max(24, Number(l.boxH || Math.max(42, Math.round(Number(l.size || 34) * 1.25))));
            el.style.width = `${boxW}px`;
            el.style.minHeight = `${boxH}px`;
            el.style.maxWidth = `${stageW}px`;
            el.style.whiteSpace = 'pre-wrap';
            el.style.wordBreak = 'break-word';
            el.style.lineHeight = '1.12';
            el.style.textAlign = 'center';
            el.style.display = 'flex';
            el.style.alignItems = 'center';
            el.style.justifyContent = 'center';
            el.style.position = 'absolute';
            el.textContent = l.text;
            if (_storyEditorState.selectedLayer === idx) {
                const handle = document.createElement('span');
                handle.className = 'story-editor-text-resize-handle';
                handle.title = 'Растянуть текстовый блок';
                el.appendChild(handle);
                initStoryLayerResizeHandle(handle, el, l);
            }
        }
        el.oncontextmenu = (e) => e.preventDefault();
        el.onclick = (e) => {
            e.stopPropagation();
            _storyEditorState.selectedLayer = idx;
            const txt = document.getElementById('storyLayerInput');
            const col = document.getElementById('storyLayerColor');
            const siz = document.getElementById('storyLayerSize');
            if (txt) txt.value = l.kind === 'image' ? '' : (l.text || '');
            if (col && l.color) col.value = l.color;
            if (siz) siz.value = String(Number(l.size || (l.kind === 'image' ? 108 : 34)));
            storyEditorSyncControls();
            renderStoryEditorLayers();
        };
        if (l.kind !== 'image') {
            el.ondblclick = (e) => {
                e.stopPropagation();
                _storyEditorState.selectedLayer = idx;
                const txt = document.getElementById('storyLayerInput');
                if (txt) {
                    txt.focus();
                    txt.select();
                }
            };
        }
        host.appendChild(el);
        if (stage) clampStoryLayerToStage(l, stage, el);
        initStoryLayerDrag(el, l);
    });
}

async function getVideoDurationSeconds(file) {
    return await new Promise((resolve) => {
        try {
            const url = URL.createObjectURL(file);
            const v = document.createElement('video');
            v.preload = 'metadata';
            v.onloadedmetadata = () => {
                const d = Number(v.duration || 0);
                URL.revokeObjectURL(url);
                resolve(Number.isFinite(d) ? d : 0);
            };
            v.onerror = () => {
                URL.revokeObjectURL(url);
                resolve(0);
            };
            v.src = url;
        } catch {
            resolve(0);
        }
    });
}

function openStoryEditor(file) {
    closeStoryEditor();
    const isVideo = String(file.type || '').startsWith('video/');
    const modal = document.createElement('div');
    modal.id = 'storyEditorModal';
    modal.className = 'story-editor-modal';
    if (isVideo) modal.classList.add('is-video');
    modal.innerHTML = `
      <div class="story-editor-shell">
        <div class="story-editor-topbar">
          <button class="story-editor-btn" id="storyEditorCloseBtn">✕</button>
          <div class="story-editor-title">Редактор истории</div>
          <div class="story-editor-top-actions">
            <button class="story-editor-btn" id="storyAspectBtn" ${isVideo ? 'disabled' : ''}>9:16</button>
            <button class="story-editor-btn primary" id="storyEditorPublishBtn">Опубликовать</button>
          </div>
        </div>
        <div class="story-editor-stage" id="storyEditorStage">
          ${isVideo ? '<video id="storyEditorVideo" class="story-editor-media" autoplay muted loop playsinline></video>' : '<canvas id="storyEditorCanvas" class="story-editor-media"></canvas>'}
          <div class="story-editor-layers" id="storyEditorLayers"></div>
        </div>
        <div class="story-editor-toolbar">
          <div class="story-editor-tools-row">
            <button class="story-editor-chip active icon" data-story-tool="move" id="storyToolMoveBtn" data-label="Move">↕</button>
            <button class="story-editor-chip icon" id="storyAddTextBtn" data-label="Text">T</button>
            <button class="story-editor-chip icon" id="storyAddStickerBtn" data-label="Sticker">😊</button>
            <button class="story-editor-chip icon" id="storyLayerCloneBtn" data-label="Duplicate">⧉</button>
            <button class="story-editor-chip danger icon" id="storyLayerDeleteBtn" data-label="Delete">🗑</button>
          </div>
          <div class="story-editor-controls-row" id="storyEditorControls">
            <input id="storyLayerInput" class="story-editor-input" type="text" placeholder="Текст слоя...">
            <input type="color" id="storyLayerColor" class="story-editor-color" value="#ffffff" title="Цвет текста">
            <button class="story-editor-chip icon mini" id="storySizeDownBtn" type="button" title="Меньше">A−</button>
            <button class="story-editor-chip icon mini" id="storySizeUpBtn" type="button" title="Больше">A+</button>
            <input type="range" id="storyLayerSize" min="16" max="220" value="34" class="story-editor-size story-editor-size-hidden" title="Размер слоя">
          </div>
        </div>
        <div class="story-editor-sticker-picker hidden" id="storyStickerPicker"></div>
        <div class="story-editor-bottom">
          <input id="storyEditorCaption" class="story-editor-caption" type="text" placeholder="Добавьте подпись...">
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    try {
        document.documentElement.style.overflow = 'hidden';
        document.body.style.overflow = 'hidden';
    } catch {}

    _storyEditorState = {
        file,
        isVideo,
        aspectIndex: 0,
        aspectPresets: [
            { label: '9:16', ratio: 9 / 16 },
            { label: '3:4', ratio: 3 / 4 },
            { label: '1:1', ratio: 1 }
        ],
        aspectRatio: 9 / 16,
        layers: [],
        selectedLayer: -1,
        tool: 'move',
        imageElement: null,
        bgX: 0,
        bgY: 0,
        bgScale: 1,
        bgUserMoved: false,
        cleanupFns: []
    };

    const shell = modal.querySelector('.story-editor-shell');
    const stage = modal.querySelector('#storyEditorStage');
    const colorInput = modal.querySelector('#storyLayerColor');
    const sizeInput = modal.querySelector('#storyLayerSize');
    const textInput = modal.querySelector('#storyLayerInput');
    const stickerPicker = modal.querySelector('#storyStickerPicker');
    const publishBtn = modal.querySelector('#storyEditorPublishBtn');
    const closeBtn = modal.querySelector('#storyEditorCloseBtn');
    const captionInput = modal.querySelector('#storyEditorCaption');
    const aspectBtn = modal.querySelector('#storyAspectBtn');
    const sizeDownBtn = modal.querySelector('#storySizeDownBtn');
    const sizeUpBtn = modal.querySelector('#storySizeUpBtn');
    const setAspect = (idx) => {
        if (!_storyEditorState || _storyEditorState.isVideo) return;
        const list = _storyEditorState.aspectPresets;
        const i = ((Number(idx) % list.length) + list.length) % list.length;
        _storyEditorState.aspectIndex = i;
        _storyEditorState.aspectRatio = Number(list[i].ratio || (9 / 16));
        if (aspectBtn) aspectBtn.textContent = list[i].label;
        const cssRatio = String(list[i].label || '9:16').replace(':', ' / ');
        stage.style.setProperty('--story-editor-aspect', cssRatio);
        onViewportChange();
    };
    const applyStoryEditorLayout = () => {
        const vv = window.visualViewport;
        const vh = Math.max(320, Math.round(vv?.height || window.innerHeight || document.documentElement.clientHeight || 0));
        const vw = Math.max(280, Math.round(vv?.width || window.innerWidth || document.documentElement.clientWidth || 0));
        modal.style.setProperty('--story-editor-vh', `${vh}px`);
        modal.style.height = `${vh}px`;
        modal.style.maxHeight = `${vh}px`;
        modal.style.width = `${vw}px`;
        modal.style.maxWidth = `${vw}px`;
        const shellRect = shell.getBoundingClientRect();
        const shellStyle = getComputedStyle(shell);
        const shellPadTop = parseFloat(shellStyle.paddingTop || '0') || 0;
        const shellPadBottom = parseFloat(shellStyle.paddingBottom || '0') || 0;
        const shellPadLeft = parseFloat(shellStyle.paddingLeft || '0') || 0;
        const shellPadRight = parseFloat(shellStyle.paddingRight || '0') || 0;
        const shellGap = parseFloat(shellStyle.rowGap || shellStyle.gap || '0') || 0;
        const topbarH = modal.querySelector('.story-editor-topbar')?.getBoundingClientRect().height || 0;
        const toolbarH = modal.querySelector('.story-editor-toolbar')?.getBoundingClientRect().height || 0;
        const stickerHost = modal.querySelector('#storyStickerPicker');
        const stickerH = (!stickerHost || stickerHost.classList.contains('hidden'))
            ? 0
            : (stickerHost.getBoundingClientRect().height || 0);
        const bottomH = modal.querySelector('.story-editor-bottom')?.getBoundingClientRect().height || 0;
        const gapsCount = stickerH > 0 ? 4 : 3;
        const reservedH = shellPadTop + shellPadBottom + topbarH + toolbarH + stickerH + bottomH + (shellGap * gapsCount);
        const availableH = Math.max(190, Math.floor(shellRect.height - reservedH));
        const availableWByShell = Math.max(180, Math.floor(shellRect.width - shellPadLeft - shellPadRight));
        const availableWByViewport = Math.max(180, Math.floor(vw - shellPadLeft - shellPadRight - 6));
        const availableW = Math.min(availableWByShell, availableWByViewport);
        const aspect = Math.max(0.35, Math.min(1.8, Number(_storyEditorState?.aspectRatio || (9 / 16))));
        const mobile = window.innerWidth <= 900;
        const widthFactor = mobile ? 0.84 : 0.80;
        const maxWByVw = Math.floor(vw * (mobile ? 0.84 : 0.64));
        let stageW = Math.min(Math.floor(availableW * widthFactor), Math.floor(availableH * aspect), maxWByVw);
        let stageH = Math.floor(stageW / aspect);
        if (stageH > availableH) {
            stageH = availableH;
            stageW = Math.floor(stageH * aspect);
        }
        stage.style.setProperty('width', `${Math.max(170, stageW)}px`, 'important');
        stage.style.setProperty('max-width', `${availableW}px`, 'important');
        stage.style.setProperty('height', `${Math.max(190, stageH)}px`, 'important');
        stage.style.setProperty('max-height', `${availableH}px`, 'important');

        const rect = stage.getBoundingClientRect();
        if (!rect.width || !rect.height) return;
        _storyEditorState.editorWidth = Math.max(200, Math.round(rect.width || 0));
        _storyEditorState.editorHeight = Math.max(280, Math.round(rect.height || 0));
        if (!_storyEditorState.isVideo && !_storyEditorState.bgUserMoved) {
            _storyEditorState.bgX = mobile ? -Math.round(_storyEditorState.editorWidth * 0.06) : 0;
            _storyEditorState.bgY = 0;
            _storyEditorState.bgScale = mobile ? 0.92 : 1;
        }
        if (_storyEditorState?.isVideo) {
            _storyEditorState.layers.forEach((l) => clampStoryLayerToStage(l, stage));
            renderStoryEditorLayers();
            return;
        }
        const canvas = document.getElementById('storyEditorCanvas');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        const renderScale = Math.max(1, Math.min(2, Number(window.devicePixelRatio || 1)));
        _storyEditorState.renderScale = renderScale;
        canvas.width = Math.max(1, Math.round(_storyEditorState.editorWidth * renderScale));
        canvas.height = Math.max(1, Math.round(_storyEditorState.editorHeight * renderScale));
        canvas.style.width = `${_storyEditorState.editorWidth}px`;
        canvas.style.height = `${_storyEditorState.editorHeight}px`;
        ctx.setTransform(renderScale, 0, 0, renderScale, 0, 0);
        drawStoryEditorBaseImage(canvas, ctx);
        _storyEditorState.layers.forEach((l) => clampStoryLayerToStage(l, stage));
        renderStoryEditorLayers();
    };
    const onViewportChange = () => {
        applyStoryEditorLayout();
        requestAnimationFrame(() => applyStoryEditorLayout());
    };

    closeBtn.onclick = () => closeStoryEditor();
    modal.addEventListener('click', (e) => { if (e.target === modal) closeStoryEditor(); });
    const vv = window.visualViewport;
    window.addEventListener('resize', onViewportChange, { passive: true });
    vv?.addEventListener('resize', onViewportChange, { passive: true });
    vv?.addEventListener('scroll', onViewportChange, { passive: true });

    const setSelectedLayer = (idx) => {
        _storyEditorState.selectedLayer = idx;
        const l = _storyEditorState.layers[idx];
        if (l) {
            textInput.value = l.kind === 'image' ? '' : (l.text || '');
            colorInput.value = l.color || '#ffffff';
            sizeInput.value = String(Number(l.size || 34));
        } else {
            textInput.value = '';
        }
        storyEditorSyncControls();
        renderStoryEditorLayers();
    };

    const addTextLayer = (txt = 'Текст') => {
        if (_storyEditorState.isVideo) return;
        const text = String(txt || '').trim() || 'Текст';
        const layer = {
            kind: 'text',
            text: text.slice(0, 120),
            x: 0.5,
            y: 0.5,
            size: Number(sizeInput.value || 34),
            boxW: 180,
            boxH: 54,
            color: colorInput.value || '#ffffff'
        };
        _storyEditorState.layers.push(layer);
        setSelectedLayer(_storyEditorState.layers.length - 1);
    };
    const addStickerLayer = (src, emoji = '') => {
        if (_storyEditorState.isVideo) return;
        const isImageSticker = !!src;
        const layer = isImageSticker
            ? { kind: 'image', src, x: 0.5, y: 0.5, size: 108, color: '#ffffff', text: '' }
            : { kind: 'text', text: String(emoji || '🙂'), x: 0.5, y: 0.5, size: 52, color: '#ffffff' };
        _storyEditorState.layers.push(layer);
        setSelectedLayer(_storyEditorState.layers.length - 1);
    };
    const renderStickerPicker = () => {
        if (!stickerPicker) return;
        const custom = [];
        try {
            getStickerPacks().forEach((p) => (p.stickers || []).forEach((s) => {
                if (s?.src) custom.push(s.src);
            }));
        } catch {}
        const builtIn = (typeof BUILT_IN_STICKERS !== 'undefined' ? BUILT_IN_STICKERS : []).slice(0, 24);
        stickerPicker.innerHTML = `
          <div class="story-sticker-grid">
            ${builtIn.map((s) => `<button class="story-sticker-item" data-emoji="${storyEditorEscape(s.emoji)}">${storyEditorEscape(s.emoji)}</button>`).join('')}
            ${custom.map((src) => `<button class="story-sticker-item img" data-src="${storyEditorEscape(src)}"><img src="${storyEditorEscape(src)}" alt=""></button>`).join('')}
          </div>
        `;
        stickerPicker.querySelectorAll('.story-sticker-item').forEach((btn) => {
            btn.onclick = () => {
                const src = btn.getAttribute('data-src') || '';
                const em = btn.getAttribute('data-emoji') || '';
                addStickerLayer(src, em);
                stickerPicker.classList.add('hidden');
            };
        });
    };
    renderStickerPicker();

    modal.querySelector('#storyToolMoveBtn').onclick = () => storyEditorSetTool('move');
    modal.querySelector('#storyAddTextBtn').onclick = () => {
        if (_storyEditorState.isVideo) return;
        storyEditorSetTool('move');
        addTextLayer('Текст');
        textInput.focus();
        textInput.select();
    };
    modal.querySelector('#storyAddStickerBtn').onclick = () => {
        if (_storyEditorState.isVideo) return;
        storyEditorSetTool('move');
        stickerPicker.classList.toggle('hidden');
    };
    modal.querySelector('#storyLayerCloneBtn').onclick = () => {
        if (_storyEditorState.isVideo) return;
        const i = _storyEditorState.selectedLayer;
        if (i < 0) return;
        const src = _storyEditorState.layers[i];
        const copy = JSON.parse(JSON.stringify(src));
        copy.x = Math.min(0.92, Math.max(0.08, Number(copy.x || 0.5) + 0.04));
        copy.y = Math.min(0.92, Math.max(0.08, Number(copy.y || 0.5) + 0.04));
        _storyEditorState.layers.push(copy);
        setSelectedLayer(_storyEditorState.layers.length - 1);
    };
    const applySelectedLayerScale = (ratio) => {
        if (_storyEditorState.isVideo) return;
        const i = _storyEditorState.selectedLayer;
        if (i < 0) return;
        const l = _storyEditorState.layers[i];
        const base = Number(l.size || (l.kind === 'image' ? 108 : 34));
        const next = Math.round(base * ratio);
        if (l.kind === 'image') l.size = Math.max(36, Math.min(420, next));
        else l.size = Math.max(12, Math.min(260, next));
        storyEditorSyncControls();
        renderStoryEditorLayers();
    };
    sizeDownBtn.onclick = () => applySelectedLayerScale(0.9);
    sizeUpBtn.onclick = () => applySelectedLayerScale(1.1);
    modal.querySelector('#storyLayerDeleteBtn').onclick = () => {
        if (_storyEditorState.isVideo) return;
        const i = _storyEditorState.selectedLayer;
        if (i < 0) return;
        _storyEditorState.layers.splice(i, 1);
        setSelectedLayer(Math.min(_storyEditorState.layers.length - 1, i));
    };
    textInput.oninput = () => {
        if (_storyEditorState.isVideo) return;
        const i = _storyEditorState.selectedLayer;
        if (i < 0) return;
        const l = _storyEditorState.layers[i];
        if (l.kind === 'image') return;
        l.text = String(textInput.value || '').slice(0, 120);
        storyEditorSyncControls();
        renderStoryEditorLayers();
    };
    colorInput.oninput = () => {
        if (_storyEditorState.isVideo) return;
        const i = _storyEditorState.selectedLayer;
        if (i < 0) return;
        if (_storyEditorState.layers[i].kind === 'image') return;
        _storyEditorState.layers[i].color = colorInput.value;
        storyEditorSyncControls();
        renderStoryEditorLayers();
    };
    sizeInput.oninput = () => {
        if (_storyEditorState.isVideo) return;
        const i = _storyEditorState.selectedLayer;
        if (i < 0) return;
        _storyEditorState.layers[i].size = Number(sizeInput.value || (_storyEditorState.layers[i].kind === 'image' ? 108 : 34));
        storyEditorSyncControls();
        renderStoryEditorLayers();
    };
    aspectBtn?.addEventListener('click', () => setAspect((_storyEditorState.aspectIndex || 0) + 1));

    stage.addEventListener('click', () => {
        if (_storyEditorState.isVideo) return;
        if (_storyEditorState?.justDraggedUntil && Date.now() < Number(_storyEditorState.justDraggedUntil || 0)) return;
        setSelectedLayer(-1);
        stickerPicker.classList.add('hidden');
    });
    stage.addEventListener('pointerdown', (e) => {
        const t = e.target;
        if (t && t.closest && t.closest('.story-editor-layer')) return;
        if (t && t.id === 'storyEditorStage') setSelectedLayer(-1);
    }, { passive: true });

    const url = URL.createObjectURL(file);
    if (isVideo) {
        const vid = modal.querySelector('#storyEditorVideo');
        vid.style.objectFit = 'cover';
        onViewportChange();
        vid.src = url;
    } else {
        const canvas = modal.querySelector('#storyEditorCanvas');
        const ctx = canvas.getContext('2d');
        const img = new Image();
        img.onload = () => {
            _storyEditorState.imageElement = img;
            onViewportChange();
            drawStoryEditorBaseImage(canvas, ctx);
            initStoryEditorBackgroundDrag(stage, canvas);
        };
        img.src = url;
    }
    setAspect(0);

    publishBtn.onclick = async () => {
        publishBtn.disabled = true;
        try {
            let uploadFile = file;
            if (!_storyEditorState.isVideo) {
                const canvas = modal.querySelector('#storyEditorCanvas');
                const ctx = canvas.getContext('2d');
                drawStoryEditorBaseImage(canvas, ctx);
                const loadImage = (src) => new Promise((resolve) => {
                    const im = new Image();
                    im.onload = () => resolve(im);
                    im.onerror = () => resolve(null);
                    im.src = src;
                });
                const drawWrapped = (ctx2, textVal, x, y, maxW, fontPx, color, maxH) => {
                    const txtVal = String(textVal || '');
                    const words = txtVal.split(/\s+/).filter(Boolean);
                    const lines = [];
                    let line = '';
                    ctx2.font = `700 ${fontPx}px Inter, sans-serif`;
                    for (const w of words) {
                        const probe = line ? `${line} ${w}` : w;
                        if (ctx2.measureText(probe).width <= maxW || !line) line = probe;
                        else { lines.push(line); line = w; }
                    }
                    if (line) lines.push(line);
                    const lineH = Math.max(14, Math.round(fontPx * 1.16));
                    const capH = Math.max(lineH, Number(maxH || lineH * 3));
                    const maxLines = Math.max(1, Math.floor(capH / lineH));
                    if (lines.length > maxLines) {
                        lines.length = maxLines;
                    }
                    const totalH = lines.length * lineH;
                    let cy = y - totalH / 2 + lineH / 2;
                    ctx2.textAlign = 'center';
                    ctx2.textBaseline = 'middle';
                    ctx2.lineWidth = Math.max(2, Math.round(fontPx * 0.12));
                    ctx2.strokeStyle = 'rgba(0,0,0,0.38)';
                    ctx2.fillStyle = color || '#fff';
                    lines.forEach((ln) => {
                        ctx2.strokeText(ln, x, cy);
                        ctx2.fillText(ln, x, cy);
                        cy += lineH;
                    });
                };
                const srcW = Math.max(1, Number(_storyEditorState.editorWidth || canvas.clientWidth || 360));
                const srcH = Math.max(1, Number(_storyEditorState.editorHeight || canvas.clientHeight || 640));
                const ratio = Math.max(0.35, Math.min(1.8, Number(_storyEditorState.aspectRatio || (srcW / srcH) || (9 / 16))));
                const imgNatW = Math.max(1, Number(_storyEditorState.imageElement?.naturalWidth || _storyEditorState.imageElement?.width || srcW));
                const imgNatH = Math.max(1, Number(_storyEditorState.imageElement?.naturalHeight || _storyEditorState.imageElement?.height || srcH));
                const baseLong = Math.max(1080, imgNatW, imgNatH);
                const cappedLong = Math.min(2160, baseLong);
                let outW = ratio >= 1 ? cappedLong : Math.round(cappedLong * ratio);
                let outH = ratio >= 1 ? Math.round(cappedLong / ratio) : cappedLong;
                outW = Math.max(720, outW);
                outH = Math.max(720, outH);
                const kx = outW / srcW;
                const ky = outH / srcH;
                const k = Math.min(kx, ky);

                const exportCanvas = document.createElement('canvas');
                exportCanvas.width = outW;
                exportCanvas.height = outH;
                const exportCtx = exportCanvas.getContext('2d');
                if (!exportCtx) throw new Error('story_export_ctx_failed');
                const img = _storyEditorState.imageElement;
                if (!img) throw new Error('story_export_img_failed');
                const m = getStoryEditorBgMetrics(exportCanvas.width, exportCanvas.height, kx, ky);
                if (!m) throw new Error('story_export_bg_failed');
                exportCtx.clearRect(0, 0, exportCanvas.width, exportCanvas.height);
                exportCtx.drawImage(img, m.dx, m.dy, m.dw, m.dh);

                for (const l of _storyEditorState.layers) {
                    const x = l.x * exportCanvas.width;
                    const y = l.y * exportCanvas.height;
                    if (l.kind === 'image' && l.src) {
                        const stImg = await loadImage(l.src);
                        if (!stImg) continue;
                        const size = Number(l.size || 108) * k;
                        exportCtx.drawImage(stImg, x - size / 2, y - size / 2, size, size);
                    } else {
                        const boxW = Math.max(40, Number(l.boxW || 180) * kx);
                        const boxH = Math.max(24, Number(l.boxH || Math.max(42, Math.round(Number(l.size || 34) * 1.25))) * ky);
                        const fontPx = Math.max(12, Number(l.size || 34) * k);
                        drawWrapped(exportCtx, l.text || '', x, y, boxW, fontPx, l.color || '#fff', boxH);
                    }
                }
                const blob = await new Promise((resolve) => exportCanvas.toBlob(resolve, 'image/png'));
                if (!blob) throw new Error('story_export_blob_failed');
                uploadFile = new File([blob], `story_${Date.now()}.png`, { type: 'image/png' });
            }
            const caption = (captionInput.value || '').trim();
            const meta = await uploadEncryptedBlobAndMeta(
                uploadFile,
                uploadFile.name || `story_${Date.now()}`,
                uploadFile.type || (_storyEditorState.isVideo ? 'video/mp4' : 'image/png'),
                {
                    type: _storyEditorState.isVideo ? 'video' : 'image',
                    caption,
                    editor_w: Number(_storyEditorState.editorWidth || 0),
                    editor_h: Number(_storyEditorState.editorHeight || 0)
                }
            );
            const res = await fetch('/api/story_create', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, meta })
            });
            const d = await res.json();
            if (!res.ok || d.status !== 'ok') throw new Error('story_create_failed');
            window.removeEventListener('resize', onViewportChange);
            vv?.removeEventListener('resize', onViewportChange);
            vv?.removeEventListener('scroll', onViewportChange);
            closeStoryEditor();
            showToast('История опубликована');
            if (d?.story && typeof d.story === 'object') {
                _storiesFeed = Array.isArray(_storiesFeed) ? _storiesFeed : [];
                _storiesFeed = _storiesFeed.filter((s) => String(s?.id || '') !== String(d.story.id || ''));
                _storiesFeed.push(d.story);
                renderStoriesBar();
            }
            await loadStoriesFeed();
        } catch {
            showToast('Ошибка публикации истории', 'error');
            publishBtn.disabled = false;
        }
    };

    _storyEditorCleanup = () => {
        window.removeEventListener('resize', onViewportChange);
        vv?.removeEventListener('resize', onViewportChange);
        vv?.removeEventListener('scroll', onViewportChange);
        if (_storyEditorState?.cleanupFns?.length) {
            _storyEditorState.cleanupFns.forEach((fn) => {
                try { fn(); } catch {}
            });
        }
        try { URL.revokeObjectURL(url); } catch {}
    };
    if (isVideo) {
        ['storyAddTextBtn', 'storyAddStickerBtn', 'storyLayerCloneBtn', 'storyLayerDeleteBtn', 'storySizeDownBtn', 'storySizeUpBtn']
            .forEach((id) => {
                const btn = modal.querySelector(`#${id}`);
                if (btn) btn.disabled = true;
            });
        const controls = modal.querySelector('#storyEditorControls');
        if (controls) controls.classList.add('disabled');
    }
    storyEditorSetTool('move');
    storyEditorSyncControls();
}

async function createStoryFromDevice() {
    const inp = document.createElement('input');
    inp.type = 'file';
    inp.accept = 'image/*,video/*';
    inp.onchange = async () => {
        const file = inp.files?.[0];
        if (!file) return;
        if (String(file.type || '').startsWith('video/')) {
            const duration = await getVideoDurationSeconds(file);
            if (duration > 60) {
                showToast('Видео для истории должно быть не длиннее 1 минуты', 'error');
                return;
            }
        }
        openStoryEditor(file);
    };
    inp.click();
}
window.createStoryFromDevice = createStoryFromDevice;

async function loadStoriesFeed() {
    try {
        const me = String(username || localStorage.getItem('username') || '').toLowerCase();
        if (!me) return;
        const res = await fetch(`/api/stories_feed/${encodeURIComponent(me)}`);
        const data = await res.json();
        _storiesFeed = Array.isArray(data) ? data : [];
        renderStoriesBar();
    } catch {}
}

function getStoriesByOwner() {
    const me = String(username || localStorage.getItem('username') || '').toLowerCase();
    const byOwner = {};
    (_storiesFeed || []).forEach((s) => {
        const o = String(s.owner || '').toLowerCase();
        if (!o) return;
        if (!byOwner[o]) byOwner[o] = [];
        byOwner[o].push(s);
    });
    const items = Object.entries(byOwner).map(([owner, list]) => {
        const sorted = list.sort((a, b) => Number(a.created_at || 0) - Number(b.created_at || 0));
        const seen = sorted.every((s) => Array.isArray(s.viewers) && s.viewers.map(v => String(v).toLowerCase()).includes(me));
        const newestAt = sorted.length ? Number(sorted[sorted.length - 1].created_at || 0) : 0;
        return { owner, items: sorted, seen, newestAt };
    });
    items.sort((a, b) => {
        if (a.owner === me) return -1;
        if (b.owner === me) return 1;
        if (a.seen !== b.seen) return a.seen ? 1 : -1;
        return b.newestAt - a.newestAt;
    });
    return items;
}

const _storyAvatarCache = {};
const _storyAvatarPending = new Set();
const _storyAvatarResolved = new Set();

function preloadStoryAvatar(uname) {
    const u = String(uname || '').toLowerCase();
    const me = String(username || localStorage.getItem('username') || '').toLowerCase();
    if (!u || _storyAvatarResolved.has(u) || _storyAvatarPending.has(u)) return;
    _storyAvatarPending.add(u);
    fetch(`/api/user_profile/${encodeURIComponent(u)}?me=${encodeURIComponent(me)}`)
        .then((r) => r.json())
        .then((p) => {
            const av = String(p?.avatar || '').trim();
            _storyAvatarCache[u] = av || '';
            _storyAvatarResolved.add(u);
            if (av) renderStoriesBar();
        })
        .catch(() => {})
        .finally(() => {
            _storyAvatarPending.delete(u);
        });
}

function findAvatarForUser(uname) {
    const u = String(uname || '').toLowerCase();
    const me = String(username || localStorage.getItem('username') || '').toLowerCase();
    if (!u) return '';
    const isValidAvatar = (v) => {
        const s = String(v || '').trim();
        if (!s) return false;
        if (s === window.location.href || s === window.location.origin + window.location.pathname) return false;
        if (/\/undefined$/i.test(s)) return false;
        return true;
    };
    const avatarEl = document.getElementById('avatarImage');
    const meAvatar = String(avatarEl?.getAttribute('src') || '').trim();
    if (u === me && isValidAvatar(meAvatar)) return meAvatar;
    if (isValidAvatar(_storyAvatarCache[u])) return _storyAvatarCache[u];
    const fromContacts = (window._allContacts || []).find((c) => String(c.username || '').toLowerCase() === u);
    const fromContactsAvatar = String(fromContacts?.avatar || '').trim();
    if (isValidAvatar(fromContactsAvatar)) return fromContactsAvatar;
    preloadStoryAvatar(u);
    return '';
}

function getStoryInitials(uname) {
    const u = String(uname || '').toLowerCase();
    const contact = (window._allContacts || []).find((c) => String(c.username || '').toLowerCase() === u);
    const dn = String(contact?.display_name || contact?.first_name || uname || '?').trim();
    const parts = dn.split(/\s+/).filter(Boolean);
    if (parts.length >= 2) return `${parts[0][0] || ''}${parts[1][0] || ''}`.toUpperCase();
    return (parts[0]?.[0] || '?').toUpperCase();
}

function renderStoriesBar() {
    const me = String(username || localStorage.getItem('username') || '').toLowerCase();
    const bar = document.getElementById('storiesBar');
    if (!bar) return;
    bar.innerHTML = '';
    const addChip = document.createElement('div');
    addChip.className = 'story-chip add';
    addChip.innerHTML = `<div class="story-avatar">+</div><div class="story-name">Новая</div>`;
    addChip.onclick = (e) => {
        e.stopPropagation();
        createStoryFromDevice();
    };
    bar.appendChild(addChip);

    getStoriesByOwner().forEach(({ owner, items, seen }) => {
        preloadStoryAvatar(owner);
        const av = findAvatarForUser(owner);
        const chip = document.createElement('div');
        chip.className = `story-chip ${seen ? 'seen' : ''}`;
        const avatarWrap = document.createElement('div');
        avatarWrap.className = 'story-avatar';
        if (av) {
            const img = document.createElement('img');
            img.src = av;
            img.alt = owner;
            img.onerror = () => {
                avatarWrap.innerHTML = '';
                avatarWrap.textContent = getStoryInitials(owner);
            };
            avatarWrap.appendChild(img);
        } else {
            avatarWrap.textContent = getStoryInitials(owner);
        }
        const nameEl = document.createElement('div');
        nameEl.className = 'story-name';
        nameEl.textContent = owner === me ? 'Вы' : owner;
        chip.appendChild(avatarWrap);
        chip.appendChild(nameEl);
        chip.onclick = (e) => {
            e.stopPropagation();
            const firstUnseenIndex = items.findIndex((s) => {
                const viewers = Array.isArray(s.viewers) ? s.viewers.map(v => String(v).toLowerCase()) : [];
                return !viewers.includes(me);
            });
            openStoryViewer(owner, Math.max(0, firstUnseenIndex));
        };
        bar.appendChild(chip);
    });
}

function formatStoryAge(ts) {
    const sec = Math.max(0, Math.floor(Date.now() / 1000 - Number(ts || 0)));
    if (sec < 60) return 'сейчас';
    if (sec < 3600) return `${Math.floor(sec / 60)} мин`;
    if (sec < 86400) return `${Math.floor(sec / 3600)} ч`;
    return `${Math.floor(sec / 86400)} д`;
}

async function decryptStoryToObjectUrl(meta) {
    const m = meta || {};
    const response = await fetch(m.url);
    const encrypted = await response.arrayBuffer();
    const iv = new Uint8Array(encrypted.slice(0, 12));
    const data = encrypted.slice(12);
    const rawKey = new Uint8Array(atob(m.file_key).split("").map(c => c.charCodeAt(0)));
    const cryptoKey = await window.crypto.subtle.importKey("raw", rawKey, { name: "AES-GCM" }, false, ["decrypt"]);
    const dec = await window.crypto.subtle.decrypt({ name: "AES-GCM", iv }, cryptoKey, data);
    return URL.createObjectURL(new Blob([dec], { type: m.mime || 'image/jpeg' }));
}

async function openStoryViewer(owner, startIndex = 0) {
    const owners = getStoriesByOwner();
    if (!owners.length) return;
    let ownerIdx = owners.findIndex((o) => o.owner === String(owner || '').toLowerCase());
    if (ownerIdx < 0) ownerIdx = 0;
    let idx = Math.max(0, Math.min(startIndex, (owners[ownerIdx]?.items?.length || 1) - 1));
    let rafId = 0;
    let playStartedAt = 0;
    let playDuration = 5000;
    let paused = false;
    let pauseFrom = 0;
    let playedBeforePause = 0;
    let liveBlobUrl = '';
    let loadToken = 0;

    const stopProgress = () => {
        if (rafId) cancelAnimationFrame(rafId);
        rafId = 0;
    };
    const revokeLiveUrl = () => {
        if (liveBlobUrl) {
            URL.revokeObjectURL(liveBlobUrl);
            liveBlobUrl = '';
        }
    };
    const closeViewer = () => {
        stopProgress();
        revokeLiveUrl();
        document.removeEventListener('keydown', onKey);
        ov.remove();
    };

    const stepOwner = (dir) => {
        const n = owners.length;
        ownerIdx = Math.max(0, Math.min(n - 1, ownerIdx + dir));
        idx = dir > 0 ? 0 : Math.max(0, owners[ownerIdx].items.length - 1);
    };
    const nextStory = () => {
        const curItems = owners[ownerIdx]?.items || [];
        if (idx < curItems.length - 1) {
            idx += 1;
            show();
            return;
        }
        if (ownerIdx < owners.length - 1) {
            stepOwner(1);
            show();
            return;
        }
        closeViewer();
    };
    const prevStory = () => {
        if (idx > 0) {
            idx -= 1;
            show();
            return;
        }
        if (ownerIdx > 0) {
            stepOwner(-1);
            show();
        }
    };

    document.getElementById('storyViewer')?.remove();
    const ov = document.createElement('div');
    ov.id = 'storyViewer';
    ov.className = 'story-viewer';
    ov.innerHTML = `
      <button id="storyPrev" class="story-nav-btn left">◀</button>
      <div class="story-shell">
        <div id="storyProgress" class="story-progress"></div>
        <div class="story-head">
          <div class="story-owner-wrap">
            <div class="story-owner-avatar" id="storyOwnerAvatar"></div>
            <div class="story-owner-meta">
              <div class="story-owner-name" id="storyOwnerName"></div>
              <div class="story-owner-time" id="storyOwnerTime"></div>
            </div>
          </div>
          <button id="storyClose" class="story-close-btn">✕</button>
        </div>
        <div id="storyStage" class="story-stage"></div>
      </div>
      <button id="storyNext" class="story-nav-btn right">▶</button>
    `;
    document.body.appendChild(ov);
    const stage = ov.querySelector('#storyStage');
    const progress = ov.querySelector('#storyProgress');
    const ownerName = ov.querySelector('#storyOwnerName');
    const ownerTime = ov.querySelector('#storyOwnerTime');
    const ownerAvatar = ov.querySelector('#storyOwnerAvatar');

    const renderProgress = (count, curIdx, pct) => {
        progress.innerHTML = '';
        for (let i = 0; i < count; i++) {
            const item = document.createElement('div');
            item.className = 'story-progress-item';
            const fill = document.createElement('div');
            fill.className = 'story-progress-fill';
            fill.style.transform = `scaleX(${i < curIdx ? 1 : i > curIdx ? 0 : Math.max(0, Math.min(1, pct))})`;
            item.appendChild(fill);
            progress.appendChild(item);
        }
    };

    const startProgress = (durationMs) => {
        stopProgress();
        playDuration = Math.max(1500, durationMs);
        playStartedAt = performance.now();
        playedBeforePause = 0;
        paused = false;
        const tick = (now) => {
            if (paused) return;
            const pct = Math.max(0, Math.min(1, (playedBeforePause + (now - playStartedAt)) / playDuration));
            const curItems = owners[ownerIdx]?.items || [];
            renderProgress(curItems.length, idx, pct);
            if (pct >= 1) {
                nextStory();
                return;
            }
            rafId = requestAnimationFrame(tick);
        };
        rafId = requestAnimationFrame(tick);
    };

    const setPaused = (state) => {
        const media = stage.querySelector('#storyMedia');
        if (state && !paused) {
            paused = true;
            pauseFrom = performance.now();
            if (media?.pause) media.pause();
            stopProgress();
            return;
        }
        if (!state && paused) {
            paused = false;
            playedBeforePause += performance.now() - pauseFrom;
            playStartedAt = performance.now();
            if (media?.play) media.play().catch(() => {});
            const tick = (now) => {
                if (paused) return;
                const pct = Math.max(0, Math.min(1, (playedBeforePause + (now - playStartedAt)) / playDuration));
                const curItems = owners[ownerIdx]?.items || [];
                renderProgress(curItems.length, idx, pct);
                if (pct >= 1) {
                    nextStory();
                    return;
                }
                rafId = requestAnimationFrame(tick);
            };
            rafId = requestAnimationFrame(tick);
        }
    };

    const onKey = (e) => {
        if (e.key === 'Escape') closeViewer();
        if (e.key === 'ArrowRight') nextStory();
        if (e.key === 'ArrowLeft') prevStory();
    };
    document.addEventListener('keydown', onKey);

    const show = async () => {
        const curOwner = owners[ownerIdx];
        const cur = curOwner?.items?.[idx];
        if (!cur) return;
        loadToken += 1;
        const token = loadToken;
        stopProgress();
        revokeLiveUrl();
        stage.innerHTML = '<div class="story-loading">Загрузка...</div>';
        const av = findAvatarForUser(curOwner.owner);
        ownerName.textContent = curOwner.owner === username ? 'Вы' : `@${curOwner.owner}`;
        ownerTime.textContent = formatStoryAge(cur.created_at);
        ownerAvatar.innerHTML = av ? `<img src="${av}" alt="${curOwner.owner}">` : `<span>${curOwner.owner[0]?.toUpperCase() || '?'}</span>`;
        renderProgress(curOwner.items.length, idx, 0);

        try {
            await fetch('/api/story_view', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, story_id: cur.id })
            });
        } catch {}
        const m = cur.meta || {};
        const cap = String(m.caption || '').replace(/</g, '&lt;');
        stage.innerHTML = String(m.type || '').startsWith('video')
            ? `<video id="storyMedia" autoplay playsinline class="story-media"></video>${cap ? `<div class="story-caption">${cap}</div>` : ''}`
            : `<img id="storyMedia" alt="" class="story-media">${cap ? `<div class="story-caption">${cap}</div>` : ''}`;
        const media = stage.querySelector('#storyMedia');
        if (!media) return;
        try {
            liveBlobUrl = await decryptStoryToObjectUrl(m);
            if (token !== loadToken) return;
            media.src = liveBlobUrl;
            if (String(m.type || '').startsWith('video')) {
                media.muted = false;
                hardenVideoElementUi(media);
                media.onloadedmetadata = () => {
                    const ms = Math.max(2500, Math.min(60000, Number(media.duration || 0) * 1000 || 7000));
                    startProgress(ms);
                    media.play().catch(() => {});
                };
                media.onended = () => nextStory();
            } else {
                startProgress(5000);
            }
        } catch {
            stage.innerHTML = '<div class="story-error">Не удалось загрузить историю</div>';
        }
    };
    ov.querySelector('#storyClose')?.addEventListener('click', closeViewer);
    ov.querySelector('#storyPrev')?.addEventListener('click', prevStory);
    ov.querySelector('#storyNext')?.addEventListener('click', nextStory);
    stage.addEventListener('click', (e) => {
        const rect = stage.getBoundingClientRect();
        const leftSide = (e.clientX - rect.left) < rect.width * 0.35;
        if (leftSide) prevStory();
        else nextStory();
    });
    stage.addEventListener('mousedown', () => setPaused(true));
    stage.addEventListener('mouseup', () => setPaused(false));
    stage.addEventListener('mouseleave', () => setPaused(false));
    stage.addEventListener('touchstart', () => setPaused(true), { passive: true });
    stage.addEventListener('touchend', () => setPaused(false), { passive: true });
    stage.addEventListener('touchcancel', () => setPaused(false), { passive: true });

    await show();
    loadStoriesFeed().catch(() => {});
}


// ═══════════════════════════════════════════════════════════════════
// ПАНЕЛЬ ЭМОДЗИ / СТИКЕРОВ / GIF
// ═══════════════════════════════════════════════════════════════════

const EMOJI_CATEGORIES = {
    '😀 Смайлики': ['😀','😃','😄','😁','😆','😅','🤣','😂','🙂','🙃','😉','😊','😇','🥰','😍','🤩','😘','😗','😚','😙','🥲','😋','😛','😜','🤪','😝','🤑','🤗','🤭','🤫','🤔','🤐','🤨','😐','😑','😶','😏','😒','🙄','😬','🤥','😌','😔','😪','🤤','😴','😷','🤒','🤕','🤢','🤮','🤧','🥵','🥶','🥴','😵','💫','🤯'],
    '👋 Жесты':    ['👋','🤚','🖐️','✋','🖖','👌','🤌','✌️','🤞','🤟','🤘','🤙','👈','👉','👆','🖕','👇','☝️','👍','👎','✊','👊','🤛','🤜','👏','🙌','👐','🤲','🤝','🙏'],
    '❤️ Сердца':   ['❤️','🧡','💛','💚','💙','💜','🖤','🤍','🤎','💔','❤️‍🔥','❤️‍🩹','💕','💞','💓','💗','💖','💘','💝','💟','☮️','✝️','☯️'],
    '🎉 Праздник': ['🎉','🎊','🎈','🎁','🎀','🎗️','🎟️','🎫','🎖️','🏆','🥇','🥈','🥉','🏅','🎯','🎲','🎮','🕹️','🎰'],
    '🐶 Животные': ['🐶','🐱','🐭','🐹','🐰','🦊','🐻','🐼','🐨','🐯','🦁','🐮','🐷','🐸','🐵','🙈','🙉','🙊','🐔','🐧','🐦','🦆','🦅','🦉','🦇','🐺','🐗','🐴','🦄'],
    '🍕 Еда':      ['🍎','🍊','🍋','🍇','🍓','🍒','🍑','🥭','🍍','🥥','🍞','🧀','🥚','🍳','🥞','🧇','🥓','🌭','🍔','🍟','🍕','🌮','🌯','🍜','🍝','🍣','🍱','🧁','🎂','🍰','🍩','🍪','🍫','🍬','🍭','🍺','🍻','🥂','🍷'],
};

const BUILT_IN_STICKERS = [
    { id:'e1', emoji:'😺', label:'кот привет', type:'emoji' },
    { id:'e2', emoji:'🐶', label:'пёс', type:'emoji' },
    { id:'e3', emoji:'🐾', label:'лапки', type:'emoji' },
    { id:'e4', emoji:'🫶', label:'обнимашки', type:'emoji' },
    { id:'e5', emoji:'🤩', label:'вау', type:'emoji' },
    { id:'e6', emoji:'🥳', label:'ура', type:'emoji' },
    { id:'e7', emoji:'😂', label:'смех', type:'emoji' },
    { id:'e8', emoji:'😎', label:'круто', type:'emoji' },
    { id:'e9', emoji:'😍', label:'любовь', type:'emoji' },
    { id:'e10', emoji:'🔥', label:'огонь', type:'emoji' },
    { id:'e11', emoji:'💯', label:'супер', type:'emoji' },
    { id:'e12', emoji:'👍', label:'ок', type:'emoji' },
    { id:'e13', emoji:'🤝', label:'согласен', type:'emoji' },
    { id:'e14', emoji:'🎉', label:'праздник', type:'emoji' },
    { id:'e15', emoji:'💖', label:'милота', type:'emoji' },
    { id:'e16', emoji:'✨', label:'блеск', type:'emoji' },
];

const STICKER_PACKS_KEY = () => `levart_sticker_packs_${username}`;
let _activeStickerPack = 'all';
let _mediaSearchQuery = '';

function getStickerPacks() {
    let packs = [];
    try { packs = JSON.parse(localStorage.getItem(STICKER_PACKS_KEY()) || '[]'); } catch {}
    if (!Array.isArray(packs)) packs = [];
    const hasMine = packs.some((p) => p.id === 'mine');
    if (!hasMine) {
        let legacy = [];
        try { legacy = JSON.parse(localStorage.getItem(`levart_stickers_${username}`) || '[]'); } catch {}
        packs.unshift({ id: 'mine', title: 'Мои', stickers: Array.isArray(legacy) ? legacy : [] });
    }
    const normalizeSticker = (s, idx = 0) => {
        if (typeof s === 'string') {
            return { id: `legacy_${Date.now()}_${idx}`, src: s, type: 'custom' };
        }
        if (!s || typeof s !== 'object') {
            return { id: `sticker_${Date.now()}_${idx}`, src: '', type: 'custom' };
        }
        const out = {
            id: String(s.id || `sticker_${Date.now()}_${idx}`),
            src: String(s.src || ''),
            type: String(s.type || (Array.isArray(s.frames) && s.frames.length > 1 ? 'gif' : 'custom'))
        };
        if (Array.isArray(s.frames) && s.frames.length > 0) {
            out.frames = s.frames.map((f) => String(f || '')).filter(Boolean);
            out.delay = Number(s.delay || 150);
            if (!out.src) out.src = out.frames[0] || '';
        }
        if (s.emoji) out.emoji = String(s.emoji);
        if (s.label) out.label = String(s.label);
        return out;
    };

    packs = packs.map((p) => ({
        id: String(p.id || `pack_${Date.now()}`),
        title: String(p.title || 'Пак'),
        stickers: (Array.isArray(p.stickers) ? p.stickers : []).map((s, i) => normalizeSticker(s, i)).filter((s) => !!(s.src || s.emoji))
    }));
    localStorage.setItem(STICKER_PACKS_KEY(), JSON.stringify(packs));
    return packs;
}

function saveStickerPacks(packs) {
    const normalizeSticker = (s, idx = 0) => {
        if (typeof s === 'string') return { id: `legacy_${Date.now()}_${idx}`, src: s, type: 'custom' };
        if (!s || typeof s !== 'object') return null;
        const out = {
            id: String(s.id || `sticker_${Date.now()}_${idx}`),
            src: String(s.src || ''),
            type: String(s.type || (Array.isArray(s.frames) && s.frames.length > 1 ? 'gif' : 'custom'))
        };
        if (Array.isArray(s.frames) && s.frames.length) {
            out.frames = s.frames.map((f) => String(f || '')).filter(Boolean);
            out.delay = Number(s.delay || 150);
            if (!out.src) out.src = out.frames[0] || '';
        }
        if (s.emoji) out.emoji = String(s.emoji);
        if (s.label) out.label = String(s.label);
        if (!out.src && !out.emoji) return null;
        return out;
    };
    const safe = (Array.isArray(packs) ? packs : []).map((p) => ({
        id: String(p.id || `pack_${Date.now()}`),
        title: String(p.title || 'Пак'),
        stickers: (Array.isArray(p.stickers) ? p.stickers : []).map((s, i) => normalizeSticker(s, i)).filter(Boolean)
    }));
    localStorage.setItem(STICKER_PACKS_KEY(), JSON.stringify(safe));
    const mine = safe.find((p) => p.id === 'mine');
    localStorage.setItem(`levart_stickers_${username}`, JSON.stringify(mine?.stickers || []));
}

// Пользовательские стикеры из localStorage
function getUserStickers() {
    const packs = getStickerPacks();
    const mine = packs.find((p) => p.id === 'mine');
    return Array.isArray(mine?.stickers) ? mine.stickers : [];
}

function resetScrollToStart(root) {
    if (!root) return;
    root.scrollTop = 0;
    root.scrollLeft = 0;
}

function enableHorizontalDragScroll(el) {
    if (!el || el.dataset.dragScrollInit === '1') return;
    el.dataset.dragScrollInit = '1';
    let startX = 0;
    let startLeft = 0;
    let dragging = false;
    const onDown = (ev) => {
        if ((ev.pointerType === 'mouse' || ev.pointerType === 'pen') && ev.button !== 0) return;
        dragging = true;
        startX = ev.clientX;
        startLeft = el.scrollLeft;
        el.classList.add('dragging');
        try { el.setPointerCapture(ev.pointerId); } catch {}
    };
    const onMove = (ev) => {
        if (!dragging) return;
        const dx = ev.clientX - startX;
        el.scrollLeft = startLeft - dx;
    };
    const onUp = (ev) => {
        dragging = false;
        el.classList.remove('dragging');
        try { el.releasePointerCapture(ev.pointerId); } catch {}
    };
    el.addEventListener('pointerdown', onDown, { passive: true });
    el.addEventListener('pointermove', onMove, { passive: true });
    el.addEventListener('pointerup', onUp, { passive: true });
    el.addEventListener('pointercancel', onUp, { passive: true });
}

function getStickersForActivePack() {
    const packs = getStickerPacks();
    if (_activeStickerPack === 'all') return packs.flatMap((p) => p.stickers || []);
    const p = packs.find((x) => x.id === _activeStickerPack);
    return Array.isArray(p?.stickers) ? p.stickers : [];
}

function stickerMatchesQuery(sticker, q) {
    if (!q) return true;
    const label = String(sticker.label || sticker.title || '').toLowerCase();
    const emoji = String(sticker.emoji || '').toLowerCase();
    return label.includes(q) || emoji.includes(q) || String(sticker.id || '').toLowerCase().includes(q);
}

window.toggleEmojiPanel = () => {
    const panel   = document.getElementById('emojiPanel');
    const trigger = document.getElementById('emojiTriggerBtn');
    if (!panel) return;

    const isVisible = panel.dataset.open === '1';

    if (!isVisible) {
        // Позиционирование панели
        if (isMobile()) {
            panel.style.position = 'fixed';
            panel.style.left = '8px';
            panel.style.right = '8px';
            panel.style.width = 'auto';
            panel.style.bottom = 'calc(72px + env(safe-area-inset-bottom))';
            panel.style.transform = 'none';
        } else if (trigger) {
            const rect = trigger.getBoundingClientRect();
            panel.style.position  = 'fixed';
            panel.style.bottom    = (window.innerHeight - rect.top + 8) + 'px';
            panel.style.right     = (window.innerWidth - rect.right - 4) + 'px';
            panel.style.left      = 'auto';
            panel.style.transform = 'none';
        }
        panel.style.display = 'block';
        panel.dataset.open  = '1';
        const content = document.getElementById('emojiPanelContent');
        if (content) resetScrollToStart(content);
        renderEmojiTab();
        // Закрываем attach menu
        document.getElementById('attachMenu')?.classList.remove('active');
    } else {
        panel.style.display = 'none';
        panel.dataset.open  = '0';
    }
};

function closeEmojiPanel() {
    const p = document.getElementById('emojiPanel');
    if (p) { p.style.display = 'none'; p.dataset.open = '0'; }
}
window.closeEmojiPanel = closeEmojiPanel;


window.switchEmojiTab = (tab, btn) => {
    document.querySelectorAll('.emoji-panel-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    _mediaSearchQuery = (document.getElementById('emojiSearchInput')?.value || '').trim().toLowerCase();
    if (tab === 'emoji')    renderEmojiTab();
    if (tab === 'stickers') renderStickersTab();
    if (tab === 'gif')      renderGifTab();
    resetScrollToStart(document.getElementById('emojiPanelContent'));
};

function renderEmojiTab() {
    const content = document.getElementById('emojiPanelContent');
    if (!content) return;
    content.innerHTML = '';
    const grid = document.createElement('div');
    grid.className = 'emoji-grid';

    const q = _mediaSearchQuery;
    Object.entries(EMOJI_CATEGORIES).forEach(([cat, emojis]) => {
        const filtered = q ? emojis.filter((em) => em.includes(q) || cat.toLowerCase().includes(q)) : emojis;
        if (!filtered.length) return;
        const header = document.createElement('div');
        header.className = 'emoji-category-header';
        header.style.gridColumn = '1 / -1';
        header.textContent = cat;
        grid.appendChild(header);

        filtered.forEach(em => {
            const btn = document.createElement('button');
            btn.className = 'emoji-btn';
            btn.textContent = em;
            btn.onclick = () => {
                insertEmojiToMessage(em);
                closeEmojiPanel();
            };
            grid.appendChild(btn);
        });
    });
    content.appendChild(grid);
}

function renderStickersTab() {
    const content = document.getElementById('emojiPanelContent');
    if (!content) return;
    content.innerHTML = '';

    const q = _mediaSearchQuery;
    const packs = getStickerPacks();
    const chips = document.createElement('div');
    chips.className = 'sticker-pack-list';
    const allChip = document.createElement('button');
    allChip.className = `sticker-pack-chip ${_activeStickerPack === 'all' ? 'active' : ''}`;
    allChip.textContent = 'Все';
    allChip.onclick = () => { _activeStickerPack = 'all'; renderStickersTab(); };
    chips.appendChild(allChip);
    packs.forEach((p) => {
        const chip = document.createElement('button');
        chip.className = `sticker-pack-chip ${_activeStickerPack === p.id ? 'active' : ''}`;
        chip.textContent = `${p.title} (${(p.stickers || []).length})`;
        chip.onclick = () => { _activeStickerPack = p.id; renderStickersTab(); };
        chips.appendChild(chip);
    });
    const chipsTail = document.createElement('span');
    chipsTail.style.cssText = 'display:inline-block;width:14px;flex:0 0 14px;height:1px;';
    chips.appendChild(chipsTail);
    content.appendChild(chips);
    resetScrollToStart(chips);
    enableHorizontalDragScroll(chips);

    const actions = document.createElement('div');
    actions.style.cssText = 'display:flex;gap:6px;overflow-x:auto;padding:0 0 6px 0;margin:0 0 4px 0;';
    const addGifBtn = document.createElement('button');
    addGifBtn.className = 'sticker-pack-chip';
    addGifBtn.textContent = '🎞️ Загрузить GIF';
    addGifBtn.onclick = () => document.getElementById('emojiGifUploadInput')?.click();
    actions.appendChild(addGifBtn);
    const actionsTail = document.createElement('span');
    actionsTail.style.cssText = 'display:inline-block;width:14px;flex:0 0 14px;height:1px;';
    actions.appendChild(actionsTail);
    enableHorizontalDragScroll(actions);
    content.appendChild(actions);

    const userStickers = getStickersForActivePack().filter((s) => stickerMatchesQuery(s, q));
    if (userStickers.length > 0) {
        const header = document.createElement('div');
        header.className = 'emoji-category-header';
        header.textContent = '⭐ Мои стикеры';
        content.appendChild(header);

        const ugrid = document.createElement('div');
        ugrid.className = 'sticker-grid';
        userStickers.forEach(s => {
            const btn = document.createElement('button');
            btn.className = 'sticker-btn';
            btn.style.fontSize = '0';
            // Пользовательские стикеры — изображения
            const img = document.createElement('img');
            img.src = s.src;
            img.style.cssText = 'width:100%;height:100%;object-fit:contain;border-radius:8px;';
            if (Array.isArray(s.frames) && s.frames.length > 1) {
                img.dataset.stickerFrames = JSON.stringify(s.frames);
                img.dataset.stickerDelay = String(Number(s.delay || 150));
            }
            btn.appendChild(img);
            mountAnimatedSticker(btn);
            btn.onclick = () => sendStickerMessage(s);
            ugrid.appendChild(btn);
        });
        content.appendChild(ugrid);
    }

    // Встроенные
    const filteredBuiltins = BUILT_IN_STICKERS.filter((s) => stickerMatchesQuery(s, q));
    const renderBuiltInPack = (title, list) => {
        if (!list.length) return;
        const header = document.createElement('div');
        header.className = 'emoji-category-header';
        header.textContent = title;
        content.appendChild(header);
        const grid = document.createElement('div');
        grid.className = 'sticker-grid';
        list.forEach((s) => {
            const btn = document.createElement('button');
            btn.className = 'sticker-btn';
            btn.style.fontSize = '0';
            if (s.src) {
                const img = document.createElement('img');
                img.src = s.src;
                img.alt = s.label || 'sticker';
                img.style.cssText = 'width:100%;height:100%;object-fit:contain;border-radius:8px;';
                btn.appendChild(img);
            } else {
                btn.textContent = s.emoji || '🎭';
                btn.style.fontSize = '28px';
            }
            btn.onclick = () => sendStickerMessage({ ...s, type: String(s.type || 'gif') });
            grid.appendChild(btn);
        });
        content.appendChild(grid);
    };
    renderBuiltInPack('🎭 Стикеры Levart', filteredBuiltins);
}

// Пустые GIF (без внешнего API)
const SAMPLE_GIFS = [
    { id:'g1',  url:'https://media.giphy.com/media/LmNwrBhejkK9EFP504/giphy.gif', title:'привет' },
    { id:'g2',  url:'https://media.giphy.com/media/3oKIPsx2VAYAgEHC12/giphy.gif', title:'ок' },
    { id:'g3',  url:'https://media.giphy.com/media/XreQmk7ETCak0/giphy.gif', title:'лол' },
    { id:'g4',  url:'https://media.giphy.com/media/26ufdipQqU2lhNA4g/giphy.gif', title:'ура' },
    { id:'g5',  url:'https://media.giphy.com/media/pFZTlrO0MV6LoWSDXd/giphy.gif', title:'танец' },
    { id:'g6',  url:'https://media.giphy.com/media/077i6AULCXc0FKTj9s/giphy.gif', title:'нет' },
    { id:'g7',  url:'https://media.giphy.com/media/zcVOyJBHYZvX2/giphy.gif', title:'фейспалм' },
    { id:'g8',  url:'https://media.giphy.com/media/Lny6Rw04nsOOc/giphy.gif', title:'хмм' },
    { id:'g9',  url:'https://media.giphy.com/media/3oEjI6SIIHBdRxXI40/giphy.gif', title:'пока' },
    { id:'g10', url:'https://media.giphy.com/media/ASd0Ukj0y3qMM/giphy.gif', title:'спасибо' },
    { id:'g11', url:'https://media.giphy.com/media/9Y5BbDSkSTiY8/giphy.gif', title:'кот' },
    { id:'g12', url:'https://media.giphy.com/media/13CoXDiaCcCoyk/giphy.gif', title:'собака' },
    { id:'g13', url:'https://media.giphy.com/media/l0MYt5jPR6QX5pnqM/giphy.gif', title:'обниму' },
    { id:'g14', url:'https://media.giphy.com/media/l0HlQ7LRalQqdWfao/giphy.gif', title:'шок' },
    { id:'g15', url:'https://media.giphy.com/media/l4FGuhL4U2WyjdkaY/giphy.gif', title:'милота' },
    { id:'g16', url:'https://media.giphy.com/media/5xtDarIN81U0KvlnzKo/giphy.gif', title:'успех' },
    { id:'g17', url:'https://media.giphy.com/media/xT0xeJpnrWC4XWblEk/giphy.gif', title:'смех' },
    { id:'g18', url:'https://media.giphy.com/media/5VKbvrjxpVJCM/giphy.gif', title:'любовь' },
    { id:'g19', url:'https://media.giphy.com/media/26gR0YFZxWbnUPtMA/giphy.gif', title:'хайп' },
    { id:'g20', url:'https://media.giphy.com/media/l0ExncehJzexFpRHq/giphy.gif', title:'норм' },
];

window._gifResults = [...SAMPLE_GIFS];

function renderGifTab(gifs) {
    const content = document.getElementById('emojiPanelContent');
    if (!content) return;
    const q = _mediaSearchQuery;
    const baseList = (gifs || window._gifResults || []);
    const myGifStickers = getStickerPacks()
        .flatMap((p) => Array.isArray(p.stickers) ? p.stickers : [])
        .filter((s) => {
            const src = String(s.src || '').toLowerCase();
            const t = String(s.type || '').toLowerCase();
            const byType = t === 'gif' || (Array.isArray(s.frames) && s.frames.length > 1);
            const bySrc = src.startsWith('data:image/gif') || src.endsWith('.gif');
            return byType || bySrc;
        })
        .map((s, i) => ({ id: `mine_${i}_${s.id || ''}`, url: s.src, title: s.label || s.name || 'мой GIF', _mine: true, _sticker: s }));
    const list = [...myGifStickers, ...baseList]
        .filter((g) => !q || String(g.title || '').toLowerCase().includes(q));
    content.innerHTML = '';

    if (!list || list.length === 0) {
        content.innerHTML = '<div style="text-align:center;opacity:0.4;padding:30px;font-size:13px;">Ничего не найдено</div>';
        return;
    }

    const grid = document.createElement('div');
    grid.className = 'gif-grid';
    list.forEach(g => {
        const item = document.createElement('div');
        item.className = 'gif-item';
        const img = document.createElement('img');
        img.src = g.url;
        img.alt = g.title || 'GIF';
        img.loading = 'lazy';
        item.appendChild(img);
        item.onclick = () => {
            if (g._mine && g._sticker) {
                sendStickerMessage(g._sticker);
            } else {
                sendGifMessage(g);
            }
            closeEmojiPanel();
        };
        grid.appendChild(item);
    });
    content.appendChild(grid);
    resetScrollToStart(content);
}

window.searchGifs = (query) => {
    if (!query) { renderGifTab(SAMPLE_GIFS); return; }
    const q = query.toLowerCase();
    renderGifTab(SAMPLE_GIFS.filter(g => g.title.includes(q)));
};

window.searchEmojiStickersGif = (query) => {
    _mediaSearchQuery = String(query || '').trim().toLowerCase();
    const active = document.querySelector('.emoji-panel-tab.active')?.dataset?.tab || 'emoji';
    if (active === 'emoji') renderEmojiTab();
    if (active === 'stickers') renderStickersTab();
    if (active === 'gif') renderGifTab();
};

function insertEmojiToMessage(emoji) {
    const input = document.getElementById('messageInput');
    if (!input) return;
    const pos = input.selectionStart || input.value.length;
    input.value = input.value.slice(0, pos) + emoji + input.value.slice(pos);
    input.focus();
    input.selectionStart = input.selectionEnd = pos + emoji.length;
}

async function sendStickerMessage(sticker) {
    if (!window.currentChat || !currentAES) { showToast('Откройте чат', 'error'); return; }
    if (String(window.currentChat).startsWith('group_')) {
        const canStickers = await canSendByGroupPermission(window.currentChat, 'can_send_stickers');
        if (!canStickers) { showToast('В этой группе вам запрещены стикеры', 'error'); return; }
    }
    closeEmojiPanel();

    const payload = { __STICKER__: true };
    if (sticker.frames && sticker.frames.length > 1) {
        payload.frames = sticker.frames;
        payload.src = sticker.src || sticker.frames[0];
        payload.delay = Number(sticker.delay || 150);
        payload.kind = 'gif';
    } else if (sticker.src) {
        payload.src = sticker.src;
        payload.kind = String(sticker.type || 'custom');
    } else {
        payload.emoji = sticker.emoji;
        payload.label = sticker.label;
        payload.kind = 'emoji';
    }

    try {
        const encrypted = await cryptoMod.encrypt(currentAES, JSON.stringify(payload));
        socket.emit('send_message', { from: username, to: window.currentChat, cipher: encrypted, type: 'sticker' });
    } catch(e) { showToast('Ошибка отправки', 'error'); }
}

async function sendGifMessage(gif) {
    if (!window.currentChat || !currentAES) { showToast('Откройте чат', 'error'); return; }
    if (String(window.currentChat).startsWith('group_')) {
        const canGifs = await canSendByGroupPermission(window.currentChat, 'can_send_gifs');
        if (!canGifs) { showToast('В этой группе вам запрещены GIF', 'error'); return; }
    }
    const gifData = JSON.stringify({ __GIF__: true, url: gif.url, title: gif.title || 'GIF' });
    try {
        const encrypted = await cryptoMod.encrypt(currentAES, gifData);
        socket.emit('send_message', { from: username, to: window.currentChat, cipher: encrypted, type: 'gif' });
    } catch(e) { showToast('Ошибка отправки', 'error'); }
}

// Закрытие панели при клике вне
// СТАЛО:
document.addEventListener('click', (e) => {
    const panel = document.getElementById('emojiPanel');
    if (panel && panel.style.display !== 'none') {
        if (
            !panel.contains(e.target) &&
            !e.target.closest('#emojiTriggerBtn') &&  // <-- это исправление
            !e.target.closest('.attach-item')
        ) {
            closeEmojiPanel();
        }
    }
});

// ─── Техническая поддержка ─────────────────────────────────────────
window.sendSupportTicket = async () => {
    const subject = document.getElementById('supportSubject')?.value || 'other';
    const message = document.getElementById('supportMessage')?.value?.trim() || '';

    if (!message) { showToast('Опишите проблему', 'error'); return; }

    const subjectMap = { bug:'Ошибка / баг', account:'Аккаунт', security:'Безопасность', feature:'Предложение', other:'Другое' };

    try {
        const res = await fetch('/api/support_ticket', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, subject: subjectMap[subject] || subject, message })
        });
        const data = await res.json();
        if (data.status === 'ok') {
            const success = document.getElementById('supportSuccess');
            const ticketEl = document.getElementById('supportTicketId');
            if (success) success.style.display = 'block';
            if (ticketEl) ticketEl.textContent = `ID тикета: ${data.ticket_id}`;
            document.getElementById('supportMessage').value = '';
            showToast('✅ Обращение отправлено!');
        }
    } catch(e) { showToast('Ошибка отправки', 'error'); }
};

// Счётчик символов поддержки
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('supportMessage')?.addEventListener('input', function() {
        const counter = document.getElementById('supportCounter');
        if (counter) counter.textContent = `${this.value.length} / 2000`;
    });
});


// ═══════════════════════════════════════════════════════════════════
// РЕДАКТОР СТИКЕРОВ
// ═══════════════════════════════════════════════════════════════════

let _editorTool    = 'draw';
let _editorMode    = 'sticker';
let _brushSize     = 6;
let _canvasHistory = [];
let _gifFrames     = [];  // Массив base64 кадров
let _activeFrame   = 0;
let _stickerEditorReady = false;
let _moveState = null;
let _editingStickerRef = null;
let _cutSelection = null;
let _stickerSaveLock = false;
let _stickerTextLayers = [];
let _stickerTextSeq = 0;
let _activeStickerTextId = '';

function _updateStickerSelectionBox(canvas, sel) {
    const box = document.getElementById('stickerSelectionBox');
    if (!canvas || !box || !sel) return;
    const rect = canvas.getBoundingClientRect();
    const sx = rect.width / canvas.width;
    const sy = rect.height / canvas.height;
    box.style.display = 'block';
    box.style.left = `${sel.x * sx}px`;
    box.style.top = `${sel.y * sy}px`;
    box.style.width = `${Math.max(1, sel.w * sx)}px`;
    box.style.height = `${Math.max(1, sel.h * sy)}px`;
}

function _hideStickerSelectionBox() {
    const box = document.getElementById('stickerSelectionBox');
    if (box) box.style.display = 'none';
}

function _clearStickerTextLayers() {
    document.querySelectorAll('#stickerCanvasWrap .sticker-text-layer').forEach((n) => n.remove());
    _stickerTextLayers = [];
    _activeStickerTextId = '';
}

function _getStickerTextLayerById(id) {
    return _stickerTextLayers.find((l) => l.id === id) || null;
}

function _syncStickerTextLayerFromDom(el) {
    if (!el) return;
    const wrap = document.getElementById('stickerCanvasWrap');
    const canvas = document.getElementById('stickerCanvas');
    if (!wrap || !canvas) return;
    const id = String(el.dataset.id || '');
    const layer = _getStickerTextLayerById(id);
    if (!layer) return;
    const wr = wrap.getBoundingClientRect();
    const er = el.getBoundingClientRect();
    const sx = canvas.width / Math.max(1, wr.width);
    const sy = canvas.height / Math.max(1, wr.height);
    layer.x = Math.max(0, (er.left - wr.left) * sx);
    layer.y = Math.max(0, (er.top - wr.top) * sy);
    layer.w = Math.max(24, er.width * sx);
    layer.h = Math.max(24, er.height * sy);
    layer.text = String(el.innerText || '').replace(/\r/g, '');
    const fs = parseFloat(getComputedStyle(el).fontSize || '24');
    if (Number.isFinite(fs)) layer.size = Math.max(10, Math.round(fs * sx));
}

function _setActiveStickerTextLayer(id) {
    _activeStickerTextId = String(id || '');
    document.querySelectorAll('#stickerCanvasWrap .sticker-text-layer').forEach((el) => {
        const active = String(el.dataset.id || '') === _activeStickerTextId;
        el.classList.toggle('active', active);
        if (!active) _syncStickerTextLayerFromDom(el);
    });
}

function _renderStickerTextLayers() {
    const wrap = document.getElementById('stickerCanvasWrap');
    const canvas = document.getElementById('stickerCanvas');
    if (!wrap || !canvas) return;
    wrap.querySelectorAll('.sticker-text-layer').forEach((n) => n.remove());
    const wr = wrap.getBoundingClientRect();
    const sx = Math.max(0.01, wr.width / canvas.width);
    const sy = Math.max(0.01, wr.height / canvas.height);
    _stickerTextLayers.forEach((layer) => {
        const el = document.createElement('div');
        el.className = `sticker-text-layer${layer.id === _activeStickerTextId ? ' active' : ''}`;
        el.dataset.id = layer.id;
        el.contentEditable = 'true';
        el.spellcheck = false;
        el.style.left = `${layer.x * sx}px`;
        el.style.top = `${layer.y * sy}px`;
        el.style.width = `${Math.max(24, layer.w * sx)}px`;
        el.style.height = `${Math.max(24, layer.h * sy)}px`;
        el.style.fontSize = `${Math.max(12, layer.size * sx)}px`;
        el.style.color = layer.color || '#ffffff';
        el.textContent = layer.text || 'Текст';

        let drag = null;
        const onMove = (ev) => {
            if (!drag) return;
            const p = ev.touches?.[0] || ev;
            const nx = drag.x + (p.clientX - drag.sx);
            const ny = drag.y + (p.clientY - drag.sy);
            el.style.left = `${Math.max(0, nx)}px`;
            el.style.top = `${Math.max(0, ny)}px`;
            ev.preventDefault?.();
        };
        const onUp = () => {
            if (!drag) return;
            drag = null;
            window.removeEventListener('mousemove', onMove);
            window.removeEventListener('mouseup', onUp);
            window.removeEventListener('touchmove', onMove);
            window.removeEventListener('touchend', onUp);
            _syncStickerTextLayerFromDom(el);
        };

        el.addEventListener('mousedown', (ev) => {
            if (ev.button !== 0) return;
            _setActiveStickerTextLayer(layer.id);
            const rr = el.getBoundingClientRect();
            if (ev.clientX > rr.right - 14 && ev.clientY > rr.bottom - 14) return; // resize handle
            drag = { sx: ev.clientX, sy: ev.clientY, x: parseFloat(el.style.left || '0'), y: parseFloat(el.style.top || '0') };
            window.addEventListener('mousemove', onMove);
            window.addEventListener('mouseup', onUp);
            ev.preventDefault();
        });
        el.addEventListener('touchstart', (ev) => {
            _setActiveStickerTextLayer(layer.id);
            const t = ev.touches?.[0];
            if (!t) return;
            drag = { sx: t.clientX, sy: t.clientY, x: parseFloat(el.style.left || '0'), y: parseFloat(el.style.top || '0') };
            window.addEventListener('touchmove', onMove, { passive: false });
            window.addEventListener('touchend', onUp);
        }, { passive: true });
        el.addEventListener('input', () => _syncStickerTextLayerFromDom(el));
        el.addEventListener('blur', () => _syncStickerTextLayerFromDom(el));
        el.addEventListener('mouseup', () => _syncStickerTextLayerFromDom(el));
        el.addEventListener('touchend', () => _syncStickerTextLayerFromDom(el));
        wrap.appendChild(el);
    });
}

function _addStickerTextLayerAt(x, y) {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return;
    const layer = {
        id: `st_text_${Date.now()}_${++_stickerTextSeq}`,
        x: Math.max(0, Math.min(canvas.width - 120, Math.round(x || canvas.width * 0.3))),
        y: Math.max(0, Math.min(canvas.height - 42, Math.round(y || canvas.height * 0.3))),
        w: 140,
        h: 48,
        size: Math.max(16, Math.round((_brushSize || 6) * 2.6 + 10)),
        color: document.getElementById('editorColor')?.value || '#ffffff',
        text: 'Текст'
    };
    _stickerTextLayers.push(layer);
    _setActiveStickerTextLayer(layer.id);
    _renderStickerTextLayers();
    setTimeout(() => {
        const el = document.querySelector(`#stickerCanvasWrap .sticker-text-layer[data-id="${layer.id}"]`);
        if (el) {
            el.focus();
            try { document.execCommand('selectAll', false, null); } catch {}
        }
    }, 20);
}

function _drawStickerTextToContext(targetCtx, baseW, baseH) {
    const sx = targetCtx.canvas.width / Math.max(1, baseW);
    const sy = targetCtx.canvas.height / Math.max(1, baseH);
    _stickerTextLayers.forEach((layer) => {
        const txt = String(layer.text || '').trim();
        if (!txt) return;
        const x = (layer.x || 0) * sx;
        const y = (layer.y || 0) * sy;
        const w = Math.max(18, (layer.w || 120) * sx);
        const h = Math.max(18, (layer.h || 40) * sy);
        const size = Math.max(10, (layer.size || 22) * sx);
        targetCtx.font = `700 ${size}px Inter, sans-serif`;
        targetCtx.fillStyle = layer.color || '#ffffff';
        targetCtx.shadowColor = 'rgba(0,0,0,0.75)';
        targetCtx.shadowBlur = Math.max(1, Math.round(size * 0.15));
        targetCtx.textBaseline = 'top';
        const words = txt.split(/\s+/);
        const lines = [];
        let line = '';
        words.forEach((word) => {
            const test = line ? `${line} ${word}` : word;
            if (targetCtx.measureText(test).width > w && line) {
                lines.push(line);
                line = word;
            } else {
                line = test;
            }
        });
        if (line) lines.push(line);
        const lineH = size * 1.2;
        const maxLines = Math.max(1, Math.floor(h / lineH));
        lines.slice(0, maxLines).forEach((ln, i) => {
            targetCtx.fillText(ln, x, y + i * lineH);
        });
        targetCtx.shadowBlur = 0;
    });
}

function _composeStickerCanvasDataUrl(targetSize = 200) {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return '';
    const out = document.createElement('canvas');
    out.width = targetSize;
    out.height = targetSize;
    const octx = out.getContext('2d');
    octx.clearRect(0, 0, out.width, out.height);
    octx.drawImage(canvas, 0, 0, out.width, out.height);
    _drawStickerTextToContext(octx, canvas.width, canvas.height);
    return out.toDataURL('image/png');
}

function initStickerEditor() {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas || _stickerEditorReady) return;
    _stickerEditorReady = true;
    const ctx = canvas.getContext('2d');
    if (!window._stickerTextResizeBound) {
        window._stickerTextResizeBound = true;
        window.addEventListener('resize', () => _renderStickerTextLayers(), { passive: true });
    }

    ctx.clearRect(0, 0, canvas.width, canvas.height);
    _canvasHistory = [ctx.getImageData(0, 0, canvas.width, canvas.height)];

    let drawing = false;
    let lastX = 0, lastY = 0;
    let cutSnapshot = null;
    let cutDraft = null;

    function getPos(e) {
        const rect = canvas.getBoundingClientRect();
        const scaleX = canvas.width / rect.width;
        const scaleY = canvas.height / rect.height;
        const clientX = e.touches ? e.touches[0].clientX : e.clientX;
        const clientY = e.touches ? e.touches[0].clientY : e.clientY;
        return [(clientX - rect.left) * scaleX, (clientY - rect.top) * scaleY];
    }

    function startDraw(e) {
        e.preventDefault();
        drawing = true;
        [lastX, lastY] = getPos(e);
        if (_editorTool === 'fill') { floodFill(canvas, ctx, Math.round(lastX), Math.round(lastY), document.getElementById('editorColor').value); return; }
        if (_editorTool === 'text') { _addStickerTextLayerAt(lastX, lastY); return; }
        if (_editorTool === 'move') {
            if (_cutSelection && lastX >= _cutSelection.x && lastX <= (_cutSelection.x + _cutSelection.w) && lastY >= _cutSelection.y && lastY <= (_cutSelection.y + _cutSelection.h)) {
                _moveState = {
                    kind: 'selection',
                    snapshot: ctx.getImageData(0, 0, canvas.width, canvas.height),
                    startX: lastX,
                    startY: lastY,
                    startSelX: _cutSelection.x,
                    startSelY: _cutSelection.y
                };
                return;
            }
            _moveState = {
                kind: 'all',
                snapshot: ctx.getImageData(0, 0, canvas.width, canvas.height),
                startX: lastX,
                startY: lastY
            };
            return;
        }
        if (_editorTool === 'cut') {
            cutSnapshot = ctx.getImageData(0, 0, canvas.width, canvas.height);
            cutDraft = { x0: lastX, y0: lastY, x1: lastX, y1: lastY };
            _hideStickerSelectionBox();
            return;
        }
        ctx.beginPath();
        ctx.arc(lastX, lastY, _brushSize/2, 0, Math.PI*2);
        if (_editorTool === 'erase') {
            ctx.save();
            ctx.globalCompositeOperation = 'destination-out';
            ctx.fillStyle = 'rgba(0,0,0,1)';
            ctx.fill();
            ctx.restore();
            return;
        }
        ctx.fillStyle = document.getElementById('editorColor').value;
        ctx.fill();
    }

    function doDraw(e) {
        e.preventDefault();
        if (!drawing || _editorTool === 'fill' || _editorTool === 'text') return;
        const [x, y] = getPos(e);
        if (_editorTool === 'move') {
            if (!_moveState?.snapshot) return;
            const tmp = document.createElement('canvas');
            tmp.width = canvas.width;
            tmp.height = canvas.height;
            tmp.getContext('2d').putImageData(_moveState.snapshot, 0, 0);
            if (_moveState.kind === 'selection' && _cutSelection) {
                const dx = x - _moveState.startX;
                const dy = y - _moveState.startY;
                const nx = Math.round(_moveState.startSelX + dx);
                const ny = Math.round(_moveState.startSelY + dy);
                ctx.clearRect(0, 0, canvas.width, canvas.height);
                ctx.drawImage(tmp, 0, 0);
                ctx.clearRect(_moveState.startSelX, _moveState.startSelY, _cutSelection.w, _cutSelection.h);
                ctx.putImageData(_cutSelection.bitmap, nx, ny);
                _updateStickerSelectionBox(canvas, { x: nx, y: ny, w: _cutSelection.w, h: _cutSelection.h });
            } else {
                const dx = x - _moveState.startX;
                const dy = y - _moveState.startY;
                ctx.clearRect(0, 0, canvas.width, canvas.height);
                ctx.drawImage(tmp, dx, dy);
                if (_cutSelection) _hideStickerSelectionBox();
            }
            [lastX, lastY] = [x, y];
            return;
        }
        if (_editorTool === 'cut') {
            if (!cutDraft) return;
            cutDraft.x1 = x;
            cutDraft.y1 = y;
            const sx = Math.min(cutDraft.x0, cutDraft.x1);
            const sy = Math.min(cutDraft.y0, cutDraft.y1);
            const sw = Math.abs(cutDraft.x1 - cutDraft.x0);
            const sh = Math.abs(cutDraft.y1 - cutDraft.y0);
            _updateStickerSelectionBox(canvas, { x: sx, y: sy, w: sw, h: sh });
            return;
        }
        ctx.beginPath();
        ctx.moveTo(lastX, lastY);
        ctx.lineTo(x, y);
        if (_editorTool === 'erase') {
            ctx.save();
            ctx.globalCompositeOperation = 'destination-out';
            ctx.strokeStyle = 'rgba(0,0,0,1)';
        } else {
            ctx.strokeStyle = document.getElementById('editorColor').value;
        }
        ctx.lineWidth = _brushSize;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        ctx.stroke();
        if (_editorTool === 'erase') ctx.restore();
        [lastX, lastY] = [x, y];
    }

    function endDraw(e) {
        if (!drawing) return;
        drawing = false;
        if (_editorTool === 'cut' && cutDraft && cutSnapshot) {
            const sx = Math.max(0, Math.round(Math.min(cutDraft.x0, cutDraft.x1)));
            const sy = Math.max(0, Math.round(Math.min(cutDraft.y0, cutDraft.y1)));
            const sw = Math.min(canvas.width - sx, Math.round(Math.abs(cutDraft.x1 - cutDraft.x0)));
            const sh = Math.min(canvas.height - sy, Math.round(Math.abs(cutDraft.y1 - cutDraft.y0)));
            if (sw > 3 && sh > 3) {
                const bmp = cutSnapshot;
                const off = document.createElement('canvas');
                off.width = canvas.width;
                off.height = canvas.height;
                off.getContext('2d').putImageData(bmp, 0, 0);
                const bctx = off.getContext('2d');
                const selData = bctx.getImageData(sx, sy, sw, sh);
                _cutSelection = { x: sx, y: sy, w: sw, h: sh, bitmap: selData };
                _updateStickerSelectionBox(canvas, _cutSelection);
                showToast('Область выделена');
            } else {
                _cutSelection = null;
                _hideStickerSelectionBox();
            }
            cutSnapshot = null;
            cutDraft = null;
            return;
        }
        if (_moveState?.kind === 'selection' && _cutSelection) {
            const dx = lastX - _moveState.startX;
            const dy = lastY - _moveState.startY;
            _cutSelection.x = Math.max(0, Math.min(canvas.width - _cutSelection.w, Math.round(_moveState.startSelX + dx)));
            _cutSelection.y = Math.max(0, Math.min(canvas.height - _cutSelection.h, Math.round(_moveState.startSelY + dy)));
            _updateStickerSelectionBox(canvas, _cutSelection);
        } else if (_moveState?.kind === 'all') {
            _cutSelection = null;
            _hideStickerSelectionBox();
        }
        _moveState = null;
        _canvasHistory.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
        if (_canvasHistory.length > 50) _canvasHistory.shift();
    }

    canvas.addEventListener('mousedown', startDraw);
    canvas.addEventListener('mousemove', doDraw);
    canvas.addEventListener('mouseup', endDraw);
    canvas.addEventListener('mouseleave', endDraw);
    canvas.addEventListener('touchstart', startDraw, { passive: false });
    canvas.addEventListener('touchmove', doDraw, { passive: false });
    canvas.addEventListener('touchend', endDraw);

    const colorEl = document.getElementById('editorColor');
    if (colorEl) {
        colorEl.addEventListener('input', () => {
            const active = _getStickerTextLayerById(_activeStickerTextId);
            if (!active) return;
            active.color = colorEl.value || '#ffffff';
            _renderStickerTextLayers();
        });
    }
}

// Flood fill
function floodFill(canvas, ctx, startX, startY, fillColor) {
    const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
    const data = imageData.data;
    const targetIdx = (startY * canvas.width + startX) * 4;
    const targetR = data[targetIdx], targetG = data[targetIdx+1], targetB = data[targetIdx+2];
    const fillRGB = hexToRgb(fillColor);
    if (!fillRGB) return;

    function match(idx) {
        return Math.abs(data[idx]-targetR)<30 && Math.abs(data[idx+1]-targetG)<30 && Math.abs(data[idx+2]-targetB)<30;
    }
    function setPixel(idx) {
        data[idx] = fillRGB.r; data[idx+1] = fillRGB.g; data[idx+2] = fillRGB.b; data[idx+3] = 255;
    }

    const queue = [[startX, startY]];
    const visited = new Set();
    while (queue.length) {
        const [x, y] = queue.pop();
        const key = `${x},${y}`;
        if (visited.has(key) || x<0 || y<0 || x>=canvas.width || y>=canvas.height) continue;
        visited.add(key);
        const idx = (y * canvas.width + x) * 4;
        if (!match(idx)) continue;
        setPixel(idx);
        queue.push([x+1,y],[x-1,y],[x,y+1],[x,y-1]);
    }
    ctx.putImageData(imageData, 0, 0);
    _canvasHistory.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
}

function hexToRgb(hex) {
    const r = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return r ? { r: parseInt(r[1],16), g: parseInt(r[2],16), b: parseInt(r[3],16) } : null;
}

window.setEditorTool = (tool, btn) => {
    _editorTool = tool;
    document.querySelectorAll('.sticker-tool').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const canvas = document.getElementById('stickerCanvas');
    if (canvas) {
        canvas.style.cursor =
            tool === 'erase' ? 'cell'
            : tool === 'cut' ? 'crosshair'
            : tool === 'fill' ? 'copy'
            : tool === 'move' ? 'grab'
            : tool === 'text' ? 'text'
            : 'crosshair';
    }
    if (tool !== 'move' && tool !== 'cut' && _cutSelection) {
        _hideStickerSelectionBox();
    }
};

window.updateBrushSize = (v) => {
    _brushSize = parseInt(v);
    const lbl = document.getElementById('brushSizeLabel');
    if (lbl) lbl.textContent = v + 'px';
    const active = _getStickerTextLayerById(_activeStickerTextId);
    if (active) {
        active.size = Math.max(16, Math.round((_brushSize || 6) * 2.6 + 10));
        _renderStickerTextLayers();
    }
};

window.clearCanvas = () => {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    _clearStickerTextLayers();
    _cutSelection = null;
    _hideStickerSelectionBox();
    _canvasHistory.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
};

window.undoCanvas = () => {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas || _canvasHistory.length < 2) return;
    _canvasHistory.pop();
    const ctx = canvas.getContext('2d');
    ctx.putImageData(_canvasHistory[_canvasHistory.length - 1], 0, 0);
    _clearStickerTextLayers();
};

window.importImageToCanvas = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
        const img = new Image();
        img.onload = () => {
            const canvas = document.getElementById('stickerCanvas');
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            // Fit image
            const scale = Math.min(canvas.width/img.width, canvas.height/img.height);
            const w = img.width * scale, h = img.height * scale;
            const ox = (canvas.width - w) / 2, oy = (canvas.height - h) / 2;
            ctx.drawImage(img, ox, oy, w, h);
            _clearStickerTextLayers();
            _cutSelection = null;
            _hideStickerSelectionBox();
            _canvasHistory.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
        };
        img.src = ev.target.result;
    };
    reader.readAsDataURL(file);
};

window.importGifToStickerLibrary = (e) => {
    const file = e?.target?.files?.[0];
    if (!file) return;
    const mime = String(file.type || '').toLowerCase();
    if (!mime.includes('gif')) {
        showToast('Нужен GIF файл', 'error');
        e.target.value = '';
        return;
    }
    if (file.size > 20 * 1024 * 1024) {
        showToast('GIF слишком большой (макс. 20MB)', 'error');
        e.target.value = '';
        return;
    }
    const reader = new FileReader();
    reader.onload = (ev) => {
        try {
            const src = String(ev?.target?.result || '').trim();
            if (!src) throw new Error('empty');
            const packs = getStickerPacks();
            let pack = packs.find((p) => p.id === 'mine');
            if (!pack) {
                pack = { id: 'mine', title: 'Мои', stickers: [] };
                packs.unshift(pack);
            }
            pack.stickers.unshift({
                id: `gif_${Date.now()}`,
                src,
                type: 'gif',
                label: file.name.replace(/\.[^.]+$/, '') || 'Мой GIF'
            });
            if (pack.stickers.length > 120) pack.stickers.pop();
            saveStickerPacks(packs);
            renderStickerPackList();
            renderStickersTab();
            showToast('GIF добавлен в стикеры и вкладку GIF');
        } catch {
            showToast('Не удалось добавить GIF', 'error');
        } finally {
            e.target.value = '';
        }
    };
    reader.onerror = () => {
        showToast('Ошибка чтения GIF', 'error');
        e.target.value = '';
    };
    reader.readAsDataURL(file);
};

window.trimCanvasToContent = () => {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const data = ctx.getImageData(0, 0, canvas.width, canvas.height);
    const px = data.data;
    let minX = canvas.width, minY = canvas.height, maxX = -1, maxY = -1;
    for (let y = 0; y < canvas.height; y++) {
        for (let x = 0; x < canvas.width; x++) {
            const a = px[(y * canvas.width + x) * 4 + 3];
            if (a > 6) {
                if (x < minX) minX = x;
                if (y < minY) minY = y;
                if (x > maxX) maxX = x;
                if (y > maxY) maxY = y;
            }
        }
    }
    if (maxX < minX || maxY < minY) {
        showToast('Нечего обрезать');
        return;
    }
    const w = maxX - minX + 1;
    const h = maxY - minY + 1;
    const side = Math.max(w, h);
    const tmp = document.createElement('canvas');
    tmp.width = side;
    tmp.height = side;
    const tctx = tmp.getContext('2d');
    tctx.clearRect(0, 0, side, side);
    tctx.putImageData(ctx.getImageData(minX, minY, w, h), Math.floor((side - w) / 2), Math.floor((side - h) / 2));
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(tmp, 0, 0, canvas.width, canvas.height);
    _clearStickerTextLayers();
    _cutSelection = null;
    _hideStickerSelectionBox();
    _canvasHistory.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
};

window.centerCanvasContent = () => {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const src = ctx.getImageData(0, 0, canvas.width, canvas.height);
    const px = src.data;
    let minX = canvas.width, minY = canvas.height, maxX = -1, maxY = -1;
    for (let y = 0; y < canvas.height; y++) {
        for (let x = 0; x < canvas.width; x++) {
            const a = px[(y * canvas.width + x) * 4 + 3];
            if (a > 6) {
                if (x < minX) minX = x;
                if (y < minY) minY = y;
                if (x > maxX) maxX = x;
                if (y > maxY) maxY = y;
            }
        }
    }
    if (maxX < minX || maxY < minY) return;
    const w = maxX - minX + 1;
    const h = maxY - minY + 1;
    const ox = Math.floor((canvas.width - w) / 2);
    const oy = Math.floor((canvas.height - h) / 2);
    const content = ctx.getImageData(minX, minY, w, h);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.putImageData(content, ox, oy);
    _clearStickerTextLayers();
    _cutSelection = null;
    _hideStickerSelectionBox();
    _canvasHistory.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
};

window.newStickerFromScratch = () => {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    _clearStickerTextLayers();
    _gifFrames = [];
    _activeFrame = 0;
    _editingStickerRef = null;
    _cutSelection = null;
    _hideStickerSelectionBox();
    renderGifFrames();
    _canvasHistory = [ctx.getImageData(0, 0, canvas.width, canvas.height)];
    showToast('Создан новый стикер');
};

window.setStickerEditorMode = (mode, btn) => {
    _editorMode = mode;
    document.querySelectorAll('#modeSticker,#modeGif').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const gfSection = document.getElementById('gifFramesSection');
    if (gfSection) gfSection.style.display = mode === 'gif' ? 'block' : 'none';
    const exportBtn = document.getElementById('exportGifBtn');
    if (exportBtn) exportBtn.style.display = mode === 'gif' ? 'inline-block' : 'none';
    if (mode === 'gif' && _gifFrames.length === 0) addGifFrame();
};

// GIF кадры
window.addGifFrame = () => {
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return;
    const frameData = _composeStickerCanvasDataUrl(200);
    _gifFrames.push(frameData);
    renderGifFrames();
    switchGifFrame(_gifFrames.length - 1);
};

function renderGifFrames() {
    const list = document.getElementById('gifFramesList');
    if (!list) return;
    list.innerHTML = '';
    _gifFrames.forEach((fd, i) => {
        const thumb = document.createElement('div');
        thumb.className = 'gif-frame-thumb' + (i === _activeFrame ? ' active' : '');
        const img = document.createElement('img');
        img.src = fd; img.style.cssText = 'width:100%;height:100%;object-fit:cover;';
        const label = document.createElement('div');
        label.style.cssText = 'position:absolute;bottom:0;left:0;right:0;background:rgba(0,0,0,0.6);font-size:9px;text-align:center;color:white;padding:2px;';
        label.textContent = `#${i+1}`;
        const delBtn = document.createElement('button');
        delBtn.className = 'gif-frame-del';
        delBtn.textContent = '✕';
        delBtn.onclick = (e) => { e.stopPropagation(); removeGifFrame(i); };
        thumb.appendChild(img); thumb.appendChild(label); thumb.appendChild(delBtn);
        thumb.onclick = () => switchGifFrame(i);
        list.appendChild(thumb);
    });
}

function switchGifFrame(idx) {
    _activeFrame = idx;
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const img = new Image();
    img.onload = () => {
        ctx.clearRect(0,0,canvas.width,canvas.height);
        ctx.drawImage(img,0,0);
        _clearStickerTextLayers();
    };
    img.src = _gifFrames[idx] || '';
    renderGifFrames();
}

function removeGifFrame(idx) {
    _gifFrames.splice(idx, 1);
    if (_activeFrame >= _gifFrames.length) _activeFrame = Math.max(0, _gifFrames.length - 1);
    renderGifFrames();
}

function getActiveTargetPack(packs) {
    let pack = packs.find((p) => p.id === (_activeStickerPack === 'all' ? 'mine' : _activeStickerPack));
    if (!pack) {
        pack = packs.find((p) => p.id === 'mine') || { id: 'mine', title: 'Мои', stickers: [] };
        if (!packs.some((p) => p.id === 'mine')) packs.unshift(pack);
    }
    return pack;
}

function drawDataUrlToEditor(dataUrl) {
    return new Promise((resolve) => {
        const canvas = document.getElementById('stickerCanvas');
        if (!canvas) { resolve(false); return; }
        const ctx = canvas.getContext('2d');
        const img = new Image();
        img.onload = () => {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            const scale = Math.min(canvas.width / img.width, canvas.height / img.height);
            const w = img.width * scale;
            const h = img.height * scale;
            const ox = (canvas.width - w) / 2;
            const oy = (canvas.height - h) / 2;
            ctx.drawImage(img, ox, oy, w, h);
            _clearStickerTextLayers();
            _canvasHistory.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
            resolve(true);
        };
        img.onerror = () => resolve(false);
        img.src = dataUrl || '';
    });
}

window.editStickerFromLibrary = async (packId, stickerId) => {
    const packs = getStickerPacks();
    const pack = packs.find((p) => p.id === packId);
    const sticker = (pack?.stickers || []).find((s) => s.id === stickerId);
    if (!sticker) return;

    openSettings();
    switchSettingsTab('sticker-editor');
    onStickerEditorTabOpen();
    _activeStickerPack = packId;
    renderStickerPackList();
    _editingStickerRef = { packId, stickerId };

    if (Array.isArray(sticker.frames) && sticker.frames.length > 1) {
        setStickerEditorMode('gif', document.getElementById('modeGif'));
        _gifFrames = [...sticker.frames];
        _activeFrame = 0;
        renderGifFrames();
        await drawDataUrlToEditor(_gifFrames[0]);
        showToast('GIF загружен в редактор');
        return;
    }

    setStickerEditorMode('sticker', document.getElementById('modeSticker'));
    await drawDataUrlToEditor(sticker.src || '');
    showToast('Стикер загружен в редактор');
};

window.copyStickerToPack = async (packId, stickerId) => {
    const packs = getStickerPacks();
    const sourcePack = packs.find((p) => p.id === packId);
    const sticker = (sourcePack?.stickers || []).find((s) => s.id === stickerId);
    if (!sticker) return;

    const title = await promptModal('Куда добавить', 'Введите название пака (создастся при отсутствии):');
    if (!title) return;
    const normalized = String(title).trim();
    let target = packs.find((p) => String(p.title || '').toLowerCase() === normalized.toLowerCase());
    if (!target) {
        target = { id: `pack_${Date.now()}`, title: normalized, stickers: [] };
        packs.push(target);
    }
    const clone = { ...sticker, id: `${String(sticker.id || 'sticker')}_copy_${Date.now()}` };
    target.stickers.unshift(clone);
    if (target.stickers.length > 120) target.stickers.pop();
    saveStickerPacks(packs);
    renderMyStickerList();
    renderStickerPackList();
    renderStickersTab();
    showToast(`Изменения сохранены: добавлено в пак "${target.title}"`);
};

window.saveStickerFromEditor = () => {
    if (_stickerSaveLock) {
        showToast('Подождите немного перед следующим сохранением');
        return;
    }
    const canvas = document.getElementById('stickerCanvas');
    if (!canvas) return;

    const dataUrl = _composeStickerCanvasDataUrl(200);

    const packs = getStickerPacks();
    if (_editingStickerRef) {
        const ep = packs.find((p) => p.id === _editingStickerRef.packId);
        const es = (ep?.stickers || []).find((s) => s.id === _editingStickerRef.stickerId);
        if (es) {
            es.src = dataUrl;
            es.type = 'custom';
            delete es.frames;
            delete es.delay;
        } else {
            const pack = getActiveTargetPack(packs);
            pack.stickers.unshift({ id: `sticker_${Date.now()}`, src: dataUrl, type: 'custom' });
        }
    } else {
        const pack = getActiveTargetPack(packs);
        pack.stickers.unshift({ id: `sticker_${Date.now()}`, src: dataUrl, type: 'custom' });
        if (pack.stickers.length > 120) pack.stickers.pop();
    }
    saveStickerPacks(packs);

    _stickerSaveLock = true;
    const saveMsg = _editingStickerRef ? 'Изменения сохранены: стикер обновлен' : 'Изменения сохранены: стикер сохранен';
    showToast(saveMsg);
    _editingStickerRef = null;
    renderMyStickerList();
    renderStickerPackList();
    renderStickersTab();
    closeSettings();
    setTimeout(() => { _stickerSaveLock = false; }, 1800);
};

function renderMyStickerList() {
    const list = document.getElementById('myStickerList');
    const count = document.getElementById('myStickerCount');
    if (!list) return;
    list.innerHTML = '';
    const packs = getStickerPacks();
    const stickers = packs.flatMap((p) => (p.stickers || []).map((s) => ({ ...s, _packId: p.id })));
    if (count) count.textContent = `(${stickers.length})`;
    if (stickers.length === 0) {
        list.innerHTML = '<div style="opacity:0.4;font-size:13px;">Нет сохранённых стикеров</div>';
        return;
    }
    stickers.forEach((s, i) => {
        const wrap = document.createElement('div');
        wrap.style.cssText = 'position:relative;width:72px;height:72px;background:rgba(255,255,255,0.03);border:1px solid var(--glass-border);border-radius:12px;overflow:hidden;';
        const img = document.createElement('img');
        img.src = s.src;
        img.style.cssText = 'width:72px;height:72px;object-fit:contain;cursor:pointer;';
        img.onclick = () => editStickerFromLibrary(s._packId, s.id);

        const toolbar = document.createElement('div');
        toolbar.style.cssText = 'position:absolute;right:4px;top:4px;display:flex;gap:4px;';
        const makeBtn = (label, onClick, tone = '') => {
            const b = document.createElement('button');
            b.className = `btn-user-action ${tone}`.trim();
            b.textContent = label;
            b.style.cssText = 'padding:2px 6px;font-size:10px;min-height:20px;';
            b.onclick = onClick;
            return b;
        };
        const editBtn = makeBtn('✏️', (e) => { e.stopPropagation(); editStickerFromLibrary(s._packId, s.id); });
        const delBtn = makeBtn('🗑', async (e) => {
            e.stopPropagation();
            const ok = await confirmModal('Удаление стикера', 'Удалить стикер?');
            if (!ok) return;
            const packsNow = getStickerPacks();
            const target = stickers[i];
            const p = packsNow.find((x) => x.id === target._packId);
            if (p) p.stickers = (p.stickers || []).filter((x) => x.id !== target.id);
            saveStickerPacks(packsNow);
            if (_editingStickerRef && _editingStickerRef.stickerId === target.id && _editingStickerRef.packId === target._packId) {
                _editingStickerRef = null;
            }
            renderMyStickerList();
            renderStickerPackList();
            renderStickersTab();
        }, 'danger');
        toolbar.appendChild(editBtn);
        toolbar.appendChild(delBtn);
        wrap.appendChild(img);
        wrap.appendChild(toolbar);
        list.appendChild(wrap);
    });
}

window.exportGif = () => {
    if (_stickerSaveLock) {
        showToast('Подождите немного перед следующим экспортом');
        return;
    }
    if (_gifFrames.length === 0) { showToast('Нет кадров', 'error'); return; }

    // Сохраняем текущий кадр
    const canvas = document.getElementById('stickerCanvas');
    if (canvas) {
        _gifFrames[_activeFrame] = _composeStickerCanvasDataUrl(200);
    }

    // Нормализуем все кадры до 200x200
    const normalized = _gifFrames.map(fd => new Promise(res => {
        const oc = document.createElement('canvas');
        oc.width = 200; oc.height = 200;
        const img = new Image();
        img.onload = () => { oc.getContext('2d').drawImage(img, 0, 0, 200, 200); res(oc.toDataURL('image/png')); };
        img.src = fd;
    }));

    Promise.all(normalized).then(frames => {
        const packs = getStickerPacks();
        const delay = Number(document.getElementById('frameDelay')?.value || 200);
        if (_editingStickerRef) {
            const ep = packs.find((p) => p.id === _editingStickerRef.packId);
            const es = (ep?.stickers || []).find((s) => s.id === _editingStickerRef.stickerId);
            if (es) {
                es.src = frames[0];
                es.frames = frames;
                es.delay = delay;
                es.type = 'gif';
            } else {
                const pack = getActiveTargetPack(packs);
                pack.stickers.unshift({ id: `gif_${Date.now()}`, src: frames[0], frames, delay, type: 'gif' });
                if (pack.stickers.length > 120) pack.stickers.pop();
            }
        } else {
            const pack = getActiveTargetPack(packs);
            pack.stickers.unshift({ id: `gif_${Date.now()}`, src: frames[0], frames, delay, type: 'gif' });
            if (pack.stickers.length > 120) pack.stickers.pop();
        }
        saveStickerPacks(packs);
        _stickerSaveLock = true;
        const saveMsg = _editingStickerRef ? `Изменения сохранены: GIF обновлен (${frames.length} кадров)` : `Изменения сохранены: GIF сохранен (${frames.length} кадров)`;
        showToast(saveMsg);
        _editingStickerRef = null;
        renderMyStickerList();
        renderStickerPackList();
        renderStickersTab();
        closeSettings();
        setTimeout(() => { _stickerSaveLock = false; }, 2200);
    });
};

window.createStickerPack = () => {
    const input = document.getElementById('stickerPackNameInput');
    const name = String(input?.value || '').trim();
    if (!name) { showToast('Введите название пака', 'error'); return; }
    const packs = getStickerPacks();
    const id = `pack_${Date.now()}`;
    packs.push({ id, title: name, stickers: [] });
    saveStickerPacks(packs);
    _activeStickerPack = id;
    if (input) input.value = '';
    renderMyStickerList();
    renderStickerPackList();
    renderStickersTab();
    showToast('Пак создан');
};

function renderStickerPackList() {
    const root = document.getElementById('stickerPackList');
    if (!root) return;
    const packs = getStickerPacks();
    root.innerHTML = '';
    packs.forEach((p) => {
        const btn = document.createElement('button');
        btn.className = `sticker-pack-chip ${_activeStickerPack === p.id ? 'active' : ''}`;
        btn.textContent = `${p.title} (${(p.stickers || []).length})`;
        btn.onclick = () => {
            _activeStickerPack = p.id;
            renderStickerPackList();
            renderStickersTab();
        };
        root.appendChild(btn);
    });
}

// Инициализация редактора при переходе на вкладку
function onStickerEditorTabOpen() {
    setTimeout(() => {
        initStickerEditor();
        _renderStickerTextLayers();
        renderMyStickerList();
        renderStickerPackList();
    }, 50);
}


// ═══════════════════════════════════════════════════════════════════
// МЕДИА ВЬЮВЕР — листание стрелочками
// ═══════════════════════════════════════════════════════════════════

function collectMediaItems() {
    // Собираем все медиа в текущем открытом чате
    const items = [];
    document.querySelectorAll('.file-wrapper[data-type="image"], .file-wrapper[data-type="video"]').forEach(fw => {
        items.push({
            url:  fw.dataset.url,
            key:  fw.dataset.key,
            name: fw.dataset.name,
            type: fw.dataset.type,
            mime: fw.dataset.mime || ''
        });
    });
    return items;
}

window.openVideoNoteViewer = async (url, key, name, mime = 'video/webm') => {
    document.getElementById('mediaViewer')?.remove();
    const overlay = document.createElement('div');
    overlay.id = 'mediaViewer';
    overlay.style.cssText = `
        position:fixed;inset:0;background:rgba(0,0,0,0.9);z-index:9999;
        display:flex;align-items:center;justify-content:center;flex-direction:column;gap:10px;
    `;
    overlay.innerHTML = `
        <div id="mvClose" style="position:absolute;top:16px;right:20px;font-size:28px;color:white;cursor:pointer;opacity:0.7;z-index:10;user-select:none;">✕</div>
        <div style="position:absolute;top:20px;left:50%;transform:translateX(-50%);color:white;font-size:13px;opacity:0.7;max-width:70%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${(name || '').replace(/</g, '&lt;')}</div>
        <div id="mvContent" style="width:min(80vw,460px);height:min(80vw,460px);max-width:78vh;max-height:78vh;border-radius:50%;overflow:hidden;display:flex;align-items:center;justify-content:center;background:#020617;"></div>
    `;
    document.body.appendChild(overlay);
    const content = overlay.querySelector('#mvContent');
    content.innerHTML = `<div style="color:white;font-size:13px;opacity:0.6;">⏳ Загрузка...</div>`;
    try {
        const blob = await getDecryptedMediaBlob(url, key, mime || 'video/webm');
        const blobUrl = URL.createObjectURL(blob);
        content.innerHTML = '';
        const vid = document.createElement('video');
        vid.src = blobUrl;
        vid.autoplay = true;
        hardenVideoElementUi(vid);
        vid.playsInline = true;
        vid.style.cssText = 'width:100%;height:100%;object-fit:cover;transform:scaleX(-1);';
        vid.onclick = (e) => {
            e.stopPropagation();
            if (vid.paused) vid.play().catch(() => {});
            else vid.pause();
        };
        content.appendChild(vid);
    } catch {
        content.innerHTML = `<div style="color:#f87171;font-size:13px;">Ошибка загрузки</div>`;
    }
    const close = () => overlay.remove();
    overlay.querySelector('#mvClose').onclick = close;
    overlay.onclick = (e) => { if (e.target === overlay) close(); };
};

window.openMediaViewer = async (url, key, name, type) => {
    // Закрываем если уже открыт
    document.getElementById('mediaViewer')?.remove();

    const items   = collectMediaItems();
    let curIndex  = items.findIndex(i => i.url === url);
    if (curIndex === -1) curIndex = 0;

    // Создаём оверлей
    const overlay = document.createElement('div');
    overlay.id = 'mediaViewer';
    overlay.style.cssText = `
        position:fixed;inset:0;background:rgba(0,0,0,0.92);z-index:9999;
        display:flex;align-items:center;justify-content:center;
        flex-direction:column;gap:12px;
    `;

    overlay.innerHTML = `
        <div id="mvClose" style="position:absolute;top:16px;right:20px;font-size:28px;color:white;cursor:pointer;opacity:0.7;z-index:10;user-select:none;">✕</div>
        <div id="mvName" style="position:absolute;top:18px;left:50%;transform:translateX(-50%);color:white;font-size:13px;opacity:0.7;max-width:60%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"></div>
        <div id="mvCounter" style="position:absolute;top:44px;left:50%;transform:translateX(-50%);color:white;font-size:12px;opacity:0.5;"></div>

        <!-- Стрелка влево -->
        <button id="mvPrev" style="position:absolute;left:16px;top:50%;transform:translateY(-50%);
            width:48px;height:48px;border-radius:50%;background:rgba(255,255,255,0.12);border:none;
            color:white;font-size:24px;cursor:pointer;display:flex;align-items:center;justify-content:center;
            transition:background 0.15s;z-index:10;">‹</button>

        <!-- Стрелка вправо -->
        <button id="mvNext" style="position:absolute;right:16px;top:50%;transform:translateY(-50%);
            width:48px;height:48px;border-radius:50%;background:rgba(255,255,255,0.12);border:none;
            color:white;font-size:24px;cursor:pointer;display:flex;align-items:center;justify-content:center;
            transition:background 0.15s;z-index:10;">›</button>

        <!-- Контент -->
        <div id="mvContent" style="max-width:90vw;max-height:80vh;display:flex;align-items:center;justify-content:center;"></div>

        <!-- Кнопка скачать -->
        <button id="mvDownload" style="background:rgba(255,255,255,0.1);border:1px solid rgba(255,255,255,0.2);
            color:white;padding:8px 18px;border-radius:8px;cursor:pointer;font-size:13px;
            transform:none;box-shadow:none;width:auto;height:auto;">⬇ Скачать</button>
    `;
    document.body.appendChild(overlay);

    async function loadItem(idx) {
        const item = items[idx];
        const content = document.getElementById('mvContent');
        const nameEl  = document.getElementById('mvName');
        const counter = document.getElementById('mvCounter');
        const prevBtn = document.getElementById('mvPrev');
        const nextBtn = document.getElementById('mvNext');

        nameEl.textContent    = item.name || '';
        counter.textContent   = items.length > 1 ? `${idx + 1} / ${items.length}` : '';
        prevBtn.style.display = (idx === 0) ? 'none' : 'flex';
        nextBtn.style.display = (idx === items.length - 1) ? 'none' : 'flex';

        content.innerHTML = `<div style="color:white;font-size:13px;opacity:0.6;">⏳ Загрузка...</div>`;

        try {
            const decryptedBlob = await getDecryptedMediaBlob(item.url, item.key, item.mime || (item.type === 'video' || item.type === 'video_note' ? 'video/mp4' : 'image/jpeg'));

            if (item.type === 'video' || item.type === 'video_note') {
                const blobUrl = URL.createObjectURL(decryptedBlob);
                content.innerHTML = '';
                const vid = document.createElement('video');
                vid.src     = blobUrl;
                hardenVideoElementUi(vid);
                vid.autoplay = true;
                vid.playsInline = true;
                vid.onclick = (e) => {
                    e.stopPropagation();
                    if (vid.paused) vid.play().catch(() => {});
                    else vid.pause();
                };
                vid.style.cssText = item.type === 'video_note'
                    ? 'max-width:min(78vw,460px);max-height:min(78vw,460px);aspect-ratio:1/1;border-radius:50%;object-fit:cover;transform:scaleX(-1);'
                    : 'max-width:88vw;max-height:76vh;border-radius:12px;';
                content.appendChild(vid);
            } else {
                const blobUrl = URL.createObjectURL(decryptedBlob);
                content.innerHTML = '';
                const img = document.createElement('img');
                img.src   = blobUrl;
                img.draggable = false;
                img.oncontextmenu = (e) => e.preventDefault();
                img.style.cssText = 'max-width:88vw;max-height:76vh;object-fit:contain;border-radius:12px;';
                content.appendChild(img);
            }

            const downloadBtn = document.getElementById('mvDownload');
            if (downloadBtn) {
                if (item.type === 'video_note') {
                    downloadBtn.style.display = 'none';
                } else {
                    downloadBtn.style.display = 'inline-flex';
                    downloadBtn.onclick = async () => {
                        const a  = document.createElement('a');
                        a.href   = URL.createObjectURL(decryptedBlob);
                        a.download = item.name || 'file';
                        a.click();
                    };
                }
            }

        } catch(e) {
            content.innerHTML = `<div style="color:#f87171;font-size:13px;">Ошибка загрузки</div>`;
        }
    }

    // Навигация
    document.getElementById('mvPrev').onclick = () => { if (curIndex > 0) loadItem(--curIndex); };
    document.getElementById('mvNext').onclick = () => { if (curIndex < items.length - 1) loadItem(++curIndex); };

    // Клавиатура
    function onKey(e) {
        if (e.key === 'ArrowLeft' && curIndex > 0) loadItem(--curIndex);
        if (e.key === 'ArrowRight' && curIndex < items.length - 1) loadItem(++curIndex);
        if (e.key === 'Escape') closeViewer();
    }
    document.addEventListener('keydown', onKey);

    // Закрытие
    function closeViewer() {
        overlay.remove();
        document.removeEventListener('keydown', onKey);
    }
    document.getElementById('mvClose').onclick = closeViewer;
    overlay.onclick = e => { if (e.target === overlay) closeViewer(); };

    // Hover на стрелки
    ['mvPrev','mvNext'].forEach(id => {
        const btn = document.getElementById(id);
        btn.onmouseenter = () => btn.style.background = 'rgba(255,255,255,0.25)';
        btn.onmouseleave = () => btn.style.background = 'rgba(255,255,255,0.12)';
    });

    loadItem(curIndex);
};









async function processInviteJoinFromLink() {
    const params = new URLSearchParams(window.location.search);
    const tokenFromUrl = (params.get('invite') || '').trim();
    const token = tokenFromUrl || (localStorage.getItem('pending_invite_token') || '').trim();
    if (!token || !username) return;

    try {
        const res = await fetch('/api/group_join_by_invite', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, token })
        });
        const data = await res.json();
        if (!res.ok || !data.group_id) throw new Error(data.error || 'join_failed');

        localStorage.removeItem('pending_invite_token');
        if (tokenFromUrl) {
            const cleanUrl = `${window.location.origin}${window.location.pathname}`;
            window.history.replaceState({}, '', cleanUrl);
        }
        await syncMyContacts();
        showToast(data.already_member ? 'Вы уже в этой группе' : 'Вы добавлены в группу по ссылке');
        try {
            let hasKey = false;
            for (let i = 0; i < 8; i++) {
                const gi = await fetch(`/api/group_info/${encodeURIComponent(data.group_id)}`);
                const g = await gi.json();
                hasKey = !!g?.encrypted_keys?.[username];
                if (hasKey) break;
                await new Promise((r) => setTimeout(r, 650));
            }
            if (hasKey) {
                window.openChat(data.group_id);
            } else {
                showToast('Вы добавлены в группу. Ключ пока не выдан владельцем.', 'info');
            }
        } catch {}
    } catch {
        if (tokenFromUrl) {
            const cleanUrl = `${window.location.origin}${window.location.pathname}`;
            window.history.replaceState({}, '', cleanUrl);
        }
        localStorage.removeItem('pending_invite_token');
        showToast('Ссылка-приглашение недействительна', 'error');
    }
}

document.addEventListener("DOMContentLoaded", async () => {
    const path = window.location.pathname;
    if (path === ROUTES.MAIN && username) showAppLoader("Загрузка чатов и ключей...");
    else if (path === ROUTES.LOGIN) showAppLoader("Подготовка входа...");
    else if (path === ROUTES.REGISTER) showAppLoader("Подготовка регистрации...");
    else showAppLoader("Запуск приложения...");

    initMobileViewportBehavior();
    enforceMobileSearchLayout();
    window.addEventListener('resize', () => {
        setTimeout(enforceMobileSearchLayout, 40);
    }, { passive: true });
    initMobileEdgeBackGesture();
    initMobileTapMessageMenu();
    initComposerButtons();
    bindChatPinsScrollTracking();
    initNativeTooltipSuppression();
    document.querySelectorAll('input, textarea').forEach((el) => {
        if (!el) return;
        el.setAttribute('autocomplete', 'off');
        el.setAttribute('autocorrect', 'off');
        el.setAttribute('autocapitalize', 'off');
        el.setAttribute('spellcheck', 'false');
        el.setAttribute('data-lpignore', 'true');
        el.setAttribute('data-form-type', 'other');
        if (el.id === 'messageInput' || el.id === 'searchUser') {
            el.setAttribute('autocomplete', 'new-password');
        }
        el.addEventListener('focus', () => {
            el.setAttribute('autocomplete', 'new-password');
            el.setAttribute('autocorrect', 'off');
            el.setAttribute('autocapitalize', 'off');
            el.setAttribute('spellcheck', 'false');
        }, { passive: true });
    });
    document.addEventListener('gesturestart', (e) => e.preventDefault(), { passive: false });
    document.addEventListener('gesturechange', (e) => e.preventDefault(), { passive: false });
    document.addEventListener('gestureend', (e) => e.preventDefault(), { passive: false });
    document.addEventListener('touchmove', (e) => {
        if (e.touches && e.touches.length > 1) e.preventDefault();
    }, { passive: false });
    if (window.location.pathname === "/" && username) {
        initAttachmentMenu();
    }
    if (path === ROUTES.MAIN && username) {
        const searchInput = document.getElementById('searchUser');
        const searchResults = document.getElementById('search-results');
        if (searchInput) {
            searchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') {
                    searchInput.value = '';
                    if (searchResults) searchResults.innerHTML = '';
                    clearSearchMode();
                }
            });
            searchInput.addEventListener('blur', () => {
                setTimeout(() => {
                    if (!searchInput.value.trim() && !(searchResults?.children?.length)) {
                        clearSearchMode();
                    }
                }, 120);
            });
        }
        const meLabel = document.getElementById("me");
        if (meLabel) meLabel.innerText = username || '';

        const hadOfflineContacts = restoreContactsFromOfflineCache(username);
        if (hadOfflineContacts) {
            clearSearchMode();
            loadAppearanceSettings();
            hideAppLoader(80);
        }
        setTimeout(() => hideAppLoader(220), hadOfflineContacts ? 900 : 1800);

        // Фоновая инициализация: не блокируем интерфейс из-за сети.
        (async () => {
            try {
                await ensureAuthTokenForCurrentUser();
            } catch {}
            try {
                await getMyPersistentKeys();
            } catch {}
            try {
                await syncMyContacts();
            } catch {}
            scheduleOfflinePreload();
            try {
                await processInviteJoinFromLink();
            } catch {}
            try {
                await loadStoriesFeed();
            } catch {}
            loadAppearanceSettings();
            clearSearchMode();
            if (!hadOfflineContacts) hideAppLoader(150);
        })().catch((e) => {
            console.error('main init failed:', e);
            clearSearchMode();
            if (!hadOfflineContacts) hideAppLoader(160);
        });
    } else {
        hideAppLoader(180);
    }
    const modal = document.getElementById('customModal');
    if (modal) {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.style.display = 'none';
            }
        });
    }
    document.addEventListener('click', ensureDesktopNotificationsPermission, { once: true });
    document.addEventListener('contextmenu', (e) => {
        if (e.target.closest('#mediaViewer') || e.target.closest('.file-wrapper.image-attach') || e.target.closest('.file-wrapper.video-attach')) {
            e.preventDefault();
        }
    });
    document.addEventListener('dragstart', (e) => {
        const t = e.target;
        if (t instanceof HTMLImageElement || t instanceof HTMLVideoElement) {
            e.preventDefault();
        }
    });
});

