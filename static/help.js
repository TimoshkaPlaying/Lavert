function getMe() {
    const url = new URL(window.location.href);
    const q = (url.searchParams.get('me') || '').trim().toLowerCase();
    return q || (localStorage.getItem('username') || '').trim().toLowerCase();
}

function getRequestedTab() {
    const url = new URL(window.location.href);
    const t = (url.searchParams.get('tab') || '').trim().toLowerCase();
    return t || 'guide';
}

function fmtTs(ts) {
    const d = new Date(Number(ts || 0) * 1000);
    if (Number.isNaN(d.getTime())) return '';
    return d.toLocaleString();
}

function esc(s) {
    return String(s || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

let _forumData = [];
let _refreshTimer = null;
let _activeTab = 'guide';
let _searchQuery = '';

function setForumStatus(text) {
    const el = document.getElementById('forumStatus');
    if (el) el.textContent = text;
}

function getAuthToken() {
    const me = getMe();
    if (!me) return "";
    return String(localStorage.getItem(`auth_token_${me}`) || "");
}

function authJsonHeaders() {
    const headers = { 'Content-Type': 'application/json' };
    const token = getAuthToken();
    if (token) headers['X-Auth-Token'] = token;
    return headers;
}

function switchTab(tab) {
    _activeTab = tab;
    document.querySelectorAll('.nav-btn').forEach((b) => b.classList.remove('active'));
    document.querySelector(`.nav-btn[data-tab="${tab}"]`)?.classList.add('active');
    document.querySelectorAll('.tab').forEach((s) => s.classList.remove('active'));
    document.getElementById(`tab-${tab}`)?.classList.add('active');
    if (tab === 'forum') {
        loadForum().catch(() => {});
        startForumAutoRefresh();
    } else {
        stopForumAutoRefresh();
    }
}

function bindTabs() {
    document.querySelectorAll('.nav-btn[data-tab]').forEach((btn) => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
}

function getFilteredTopics() {
    const q = _searchQuery.trim().toLowerCase();
    if (!q) return _forumData;
    return (_forumData || []).filter((t) => {
        const base = `${t.title || ''}\n${t.body || ''}\n${t.author || ''}`.toLowerCase();
        if (base.includes(q)) return true;
        const replies = Array.isArray(t.replies) ? t.replies : [];
        return replies.some((r) => `${r.body || ''}\n${r.author || ''}`.toLowerCase().includes(q));
    });
}

async function loadForum(silent = false) {
    const me = getMe();
    if (!silent) setForumStatus('Загрузка тем...');
    try {
        const res = await fetch(`/api/help/forum?me=${encodeURIComponent(me)}`);
        const data = await res.json();
        _forumData = Array.isArray(data.topics) ? data.topics : [];
        renderForum();
        const total = _forumData.length;
        const shown = getFilteredTopics().length;
        setForumStatus(`Тем: ${shown}${shown !== total ? ` из ${total}` : ''}. Обновлено: ${new Date().toLocaleTimeString()}`);
    } catch {
        if (!silent) setForumStatus('Не удалось загрузить форум');
    }
}

function renderForum() {
    const root = document.getElementById('forumList');
    if (!root) return;
    const topics = getFilteredTopics();
    if (!topics.length) {
        root.innerHTML = '<div class="card">Темы не найдены. Создайте первую.</div>';
        return;
    }
    root.innerHTML = '';
    topics.forEach((t) => {
        const replies = Array.isArray(t.replies) ? t.replies : [];
        const el = document.createElement('div');
        el.className = 'topic';
        el.innerHTML = `
            <div class="topic-title">${esc(t.title)}</div>
            <div class="topic-meta">от @${esc(t.author)} • ${fmtTs(t.created_at)} • ответов: ${replies.length}</div>
            <div class="topic-body">${esc(t.body)}</div>
            <div class="topic-replies">${replies.map((r) => `
                <div class="reply ${r.is_moderator ? 'mod' : ''}">
                    <div>${esc(r.body)}</div>
                    <div class="reply-meta">@${esc(r.author)} ${r.is_moderator ? '• модератор' : ''} • ${fmtTs(r.created_at)}</div>
                </div>
            `).join('')}</div>
            <div class="reply-box">
                <input type="text" id="reply_${t.id}" placeholder="Ответить в тему..." maxlength="5000">
                <button data-topic="${t.id}" type="button">Ответить</button>
            </div>
        `;
        const btn = el.querySelector('button[data-topic]');
        btn?.addEventListener('click', () => replyTopic(t.id));
        const input = el.querySelector(`#reply_${t.id}`);
        input?.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                replyTopic(t.id);
            }
        });
        root.appendChild(el);
    });
}

async function createTopic() {
    const me = getMe();
    const titleEl = document.getElementById('topicTitle');
    const bodyEl = document.getElementById('topicBody');
    const title = (titleEl?.value || '').trim();
    const body = (bodyEl?.value || '').trim();
    if (!me || !title || !body) {
        setForumStatus('Заполни заголовок и описание');
        return;
    }
    setForumStatus('Публикация темы...');
    const res = await fetch('/api/help/forum_topic', {
        method: 'POST',
        headers: authJsonHeaders(),
        body: JSON.stringify({ author: me, title, body, auth_token: getAuthToken() })
    });
    if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setForumStatus(data?.error === 'auth_required' ? 'Нужна повторная авторизация' : data?.error === 'rate_limited' ? 'Слишком часто. Подожди немного.' : 'Не удалось создать тему');
        return;
    }
    if (titleEl) titleEl.value = '';
    if (bodyEl) bodyEl.value = '';
    await loadForum();
}

async function replyTopic(topicId) {
    const me = getMe();
    const input = document.getElementById(`reply_${topicId}`);
    const body = (input?.value || '').trim();
    if (!me || !body) return;
    setForumStatus('Отправка ответа...');
    const res = await fetch('/api/help/forum_reply', {
        method: 'POST',
        headers: authJsonHeaders(),
        body: JSON.stringify({ author: me, topic_id: topicId, body, auth_token: getAuthToken() })
    });
    if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setForumStatus(data?.error === 'auth_required' ? 'Нужна повторная авторизация' : data?.error === 'rate_limited' ? 'Слишком часто. Подожди немного.' : 'Не удалось отправить ответ');
        return;
    }
    if (input) input.value = '';
    await loadForum(true);
}

function startForumAutoRefresh() {
    stopForumAutoRefresh();
    _refreshTimer = window.setInterval(() => {
        if (_activeTab === 'forum') loadForum(true).catch(() => {});
    }, 20000);
}

function stopForumAutoRefresh() {
    if (_refreshTimer) {
        window.clearInterval(_refreshTimer);
        _refreshTimer = null;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    bindTabs();
    document.getElementById('createTopicBtn')?.addEventListener('click', createTopic);
    document.getElementById('forumRefreshBtn')?.addEventListener('click', () => loadForum().catch(() => {}));
    document.getElementById('forumSearch')?.addEventListener('input', (e) => {
        _searchQuery = String(e.target?.value || '');
        renderForum();
        const total = _forumData.length;
        const shown = getFilteredTopics().length;
        setForumStatus(`Тем: ${shown}${shown !== total ? ` из ${total}` : ''}`);
    });
    const initialTab = ['guide', 'formatting', 'forum'].includes(getRequestedTab()) ? getRequestedTab() : 'guide';
    switchTab(initialTab);
});
