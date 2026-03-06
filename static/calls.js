(() => {
  const username = (localStorage.getItem('username') || '').trim().toLowerCase();
  if (!username || typeof io === 'undefined') return;

  const socket = io();
  const CALL_PREFS_KEY = `levart_call_prefs_${username}`;
  const state = {
    username,
    currentChat: null,
    activeCallId: null,
    activeChatId: null,
    inCall: false,
    callMode: 'audio',
    participants: new Set(),
    peerConnections: new Map(),
    remoteStreams: new Map(),
    mediaState: new Map(),
    localStream: null,
    rawCameraTrack: null,
    processedCameraTrack: null,
    bgProcessor: null,
    screenStream: null,
    screenOwner: null,
    headphonesEnabled: true,
    micWasEnabledBeforeHeadphonesOff: false,
    audioBoostCtx: null,
    audioBoostNodes: new Map(),
    noiseProcessor: null,
    noiseAudioCtx: null,
    noiseMicSource: null,
    noiseGateAnalyser: null,
    noiseGateGain: null,
    noiseGateRaf: null,
    noiseGateOpen: false,
    noiseSuppEnabled: false,
    noiseSuppBusy: false,
    speakerTestCtx: null,
    lastCallChatId: null,
    micVolume: 1.0,
    speakerVolume: 1.0,
    videoQuality: 'high',
    mirrorCamera: true,
    micLevelCtx: null,
    micLevelAnalyser: null,
    allowDrawAll: false,
    controlGranted: false,
    incomingInvite: null,
    audioDeviceId: '',
    videoDeviceId: '',
    speakerDeviceId: '',
    backgroundMode: 'none',
    backgroundImage: '',
    backgroundPending: false,
    manualPinnedMain: false,
    micTest: {
      stream: null,
      recorder: null,
      chunks: [],
      blobUrl: ''
    },
    camTestStream: null,
    camTestPreviewStream: null,
    bgThumbCache: {},
    drawStrokes: [],
    strokeMap: new Map(),
    annotationByOwner: {},
    annotationOwner: null,
    draw: {
      active: false,
      tool: 'pen',
      color: '#ff4d4f',
      size: 2,
      lastX: 0,
      lastY: 0,
      currentStrokeId: null
    },
    ring: {
      ctx: null,
      mode: null,
      intervalId: null,
      timeoutIds: []
    },
    outgoingRingStopTimer: null,
    dialTargets: [],
    avatarCache: {},
    pinnedMainSourceId: null,
    mainAspect: 16 / 9,
    activeCallsByChat: {},
    avatarRenderToken: 0,
    currentMainSourceId: null,
    remoteScreenTrackIds: {},
    pendingRejoin: null,
    callMinimized: false
  };

  function logCallSilent(scope, err) {
    try {
      console.warn(`[calls] ${scope}`, err || '');
    } catch {}
  }

  function isMobileViewport() {
    return !!(window.matchMedia && window.matchMedia('(max-width: 900px)').matches);
  }

  function loadCallPrefs() {
    try {
      const raw = localStorage.getItem(CALL_PREFS_KEY);
      if (!raw) return;
      const p = JSON.parse(raw);
      if (p && typeof p === 'object') {
        if (typeof p.noiseSuppEnabled === 'boolean') state.noiseSuppEnabled = p.noiseSuppEnabled;
        if (typeof p.videoQuality === 'string') state.videoQuality = p.videoQuality;
        if (typeof p.mirrorCamera === 'boolean') state.mirrorCamera = p.mirrorCamera;
        if (typeof p.micVolume === 'number') state.micVolume = Math.max(0, Math.min(2, p.micVolume));
        if (typeof p.speakerVolume === 'number') state.speakerVolume = Math.max(0, Math.min(2, p.speakerVolume));
        if (typeof p.audioDeviceId === 'string') state.audioDeviceId = p.audioDeviceId;
        if (typeof p.videoDeviceId === 'string') state.videoDeviceId = p.videoDeviceId;
        if (typeof p.speakerDeviceId === 'string') state.speakerDeviceId = p.speakerDeviceId;
        if (typeof p.backgroundMode === 'string') state.backgroundMode = p.backgroundMode;
        if (typeof p.backgroundImage === 'string') state.backgroundImage = p.backgroundImage;
      }
    } catch (e) {
      logCallSilent('loadCallPrefs', e);
    }
  }

  function saveCallPrefs() {
    try {
      localStorage.setItem(CALL_PREFS_KEY, JSON.stringify({
        noiseSuppEnabled: !!state.noiseSuppEnabled,
        videoQuality: state.videoQuality || 'high',
        mirrorCamera: state.mirrorCamera !== false,
        micVolume: Number(state.micVolume ?? 1),
        speakerVolume: Number(state.speakerVolume ?? 1),
        audioDeviceId: state.audioDeviceId || '',
        videoDeviceId: state.videoDeviceId || '',
        speakerDeviceId: state.speakerDeviceId || '',
        backgroundMode: state.backgroundMode || 'none',
        backgroundImage: state.backgroundImage || ''
      }));
    } catch (e) {
      logCallSilent('saveCallPrefs', e);
    }
  }

  loadCallPrefs();

  const ui = {};
  socket.on('connect', () => {
    socket.emit('user_online', { username });
    try {
      const raw = localStorage.getItem('active_call');
      if (raw) state.pendingRejoin = JSON.parse(raw);
    } catch (e) {
      logCallSilent('socket.connect.pendingRejoin', e);
    }
  });

  const icons = {
    phone: '<svg viewBox="0 0 24 24"><path d="M6.6 10.8a15.7 15.7 0 0 0 6.6 6.6l2.2-2.2a1.5 1.5 0 0 1 1.6-.36c1.1.36 2.3.56 3.5.56a1.5 1.5 0 0 1 1.5 1.5v3.5a1.5 1.5 0 0 1-1.5 1.5C10.5 22 2 13.5 2 3.5A1.5 1.5 0 0 1 3.5 2h3.5a1.5 1.5 0 0 1 1.5 1.5c0 1.2.2 2.4.56 3.5.16.52.02 1.08-.36 1.46L6.6 10.8z"/></svg>',
    video: '<svg viewBox="0 0 24 24"><path d="M15 8.5V6a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h9a2 2 0 0 0 2-2v-2.5l7 3.5v-14l-7 3.5z"/></svg>',
    mic: '<svg viewBox="0 0 24 24"><path d="M12 15a3 3 0 0 0 3-3V6a3 3 0 1 0-6 0v6a3 3 0 0 0 3 3zm5-3a1 1 0 0 1 2 0 7 7 0 0 1-6 6.92V22h3a1 1 0 1 1 0 2H8a1 1 0 1 1 0-2h3v-3.08A7 7 0 0 1 5 12a1 1 0 1 1 2 0 5 5 0 1 0 10 0z"/></svg>',
    headphones: '<svg viewBox="0 0 24 24"><path d="M12 3a9 9 0 0 0-9 9v4a3 3 0 0 0 3 3h2v-8H5v1a7 7 0 1 1 14 0v-1h-3v8h2a3 3 0 0 0 3-3v-4a9 9 0 0 0-9-9z"/></svg>',
    cam: '<svg viewBox="0 0 24 24"><path d="M17 7h-2.17l-1.41-1.41A2 2 0 0 0 12 5H8a2 2 0 0 0-2 2H4a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h13a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2zm5 2v8l-3-2v-4l3-2z"/></svg>',
    screen: '<svg viewBox="0 0 24 24"><path d="M3 4h18a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2h-7v2h3a1 1 0 1 1 0 2H8a1 1 0 1 1 0-2h3v-2H3a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2zm0 2v11h18V6H3z"/></svg>',
    hand: '<svg viewBox="0 0 24 24"><path d="M7 11V5a1.5 1.5 0 0 1 3 0v4h1V4a1.5 1.5 0 0 1 3 0v5h1V6a1.5 1.5 0 0 1 3 0v6.2l.72-.5a2 2 0 0 1 2.78.45 2 2 0 0 1-.34 2.62l-4.2 3.7A5 5 0 0 1 14.6 20H11a5 5 0 0 1-5-5v-4a1.5 1.5 0 0 1 3 0z"/></svg>',
    draw: '<svg viewBox="0 0 24 24"><path d="M3 17.25V21h3.75L18.81 8.94l-3.75-3.75L3 17.25zm17.71-10.04a1 1 0 0 0 0-1.41l-2.5-2.5a1 1 0 0 0-1.41 0l-1.3 1.29 3.75 3.75 1.46-1.13z"/></svg>',
    chat: '<svg viewBox="0 0 24 24"><path d="M4 3h16a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H8l-4 3v-3H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z"/></svg>',
    settings: '<svg viewBox="0 0 24 24"><path d="M19.14 12.94c.04-.31.06-.62.06-.94s-.02-.63-.06-.94l2.03-1.58a.5.5 0 0 0 .12-.64l-1.92-3.32a.5.5 0 0 0-.6-.22l-2.39.96a7.24 7.24 0 0 0-1.63-.94l-.36-2.54a.5.5 0 0 0-.5-.42h-3.84a.5.5 0 0 0-.5.42l-.36 2.54c-.58.22-1.13.53-1.63.94l-2.39-.96a.5.5 0 0 0-.6.22L2.71 8.84a.5.5 0 0 0 .12.64l2.03 1.58c-.04.31-.06.62-.06.94s.02.63.06.94l-2.03 1.58a.5.5 0 0 0-.12.64l1.92 3.32a.5.5 0 0 0 .6.22l2.39-.96c.5.4 1.05.72 1.63.94l.36 2.54a.5.5 0 0 0 .5.42h3.84a.5.5 0 0 0 .5-.42l.36-2.54c.58-.22 1.13-.53 1.63-.94l2.39.96a.5.5 0 0 0 .6-.22l1.92-3.32a.5.5 0 0 0-.12-.64l-2.03-1.58zM12 15.5A3.5 3.5 0 1 1 12 8a3.5 3.5 0 0 1 0 7.5z"/></svg>',
    volumeHigh: '<svg viewBox="0 0 24 24"><path d="M3 10v4h4l5 4V6L7 10H3zm12.5 2a3.5 3.5 0 0 0-2.12-3.22v6.44A3.5 3.5 0 0 0 15.5 12zm-2.12-8.74v2.09a7 7 0 0 1 0 13.3v2.09a9 9 0 0 0 0-17.48z"/></svg>',
    volumeMid: '<svg viewBox="0 0 24 24"><path d="M3 10v4h4l5 4V6L7 10H3zm12.5 2a3.5 3.5 0 0 0-2.12-3.22v6.44A3.5 3.5 0 0 0 15.5 12z"/></svg>',
    volumeLow: '<svg viewBox="0 0 24 24"><path d="M3 10v4h4l5 4V6L7 10H3z"/></svg>',
    plane: '<svg viewBox="0 0 24 24"><path d="M21.7 2.3a1 1 0 0 0-1.02-.23L2.64 8.57a1 1 0 0 0 .07 1.89l7.6 2.22 2.22 7.6a1 1 0 0 0 1.89.07l6.5-18.04a1 1 0 0 0-.22-1.01zM5.86 9.24l11.92-4.3-6.9 6.9-5.02-2.6zm7.92 8.9-1.58-5.05 6.9-6.9-4.3 11.95-.02-.01z"/></svg>',
    end: '<svg viewBox="0 0 24 24"><path d="M12 9c4.2 0 7.9 1.5 10 3.8l-2.3 2.3c-1.5-1.6-4.5-2.6-7.7-2.6s-6.2 1-7.7 2.6L2 12.8C4.1 10.5 7.8 9 12 9zm0 0"/></svg>'
  };

  function notify(text) {
    if (typeof window.showToast === 'function') {
      window.showToast(text, 'success');
      return;
    }
    const c = document.getElementById('toast-container');
    if (!c) return;
    const el = document.createElement('div');
    el.className = 'toast success';
    el.textContent = text;
    c.appendChild(el);
    setTimeout(() => el.remove(), 3000);
  }

  function mkButton(id, icon, title, cls = '') {
    const b = document.createElement('button');
    b.id = id;
    b.className = `call-btn ${cls}`.trim();
    b.title = title;
    b.innerHTML = icon;
    return b;
  }

  function initDrawToolUi() {
    if (!ui.drawColorRow) return;
    const COLORS = ['#ff4d4f', '#22c55e', '#3b82f6', '#f59e0b', '#a855f7', '#ffffff'];
    ui.drawColorRow.innerHTML = '';
    COLORS.forEach((c, i) => {
      const b = document.createElement('button');
      b.className = 'draw-color-dot' + (i === 0 ? ' active' : '');
      b.title = `Цвет ${c}`;
      b.style.background = c;
      b.addEventListener('click', () => {
        state.draw.color = c;
        ui.drawColorRow.querySelectorAll('.draw-color-dot').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        setDrawTool('pen');
      });
      ui.drawColorRow.appendChild(b);
    });
  }

  function setDrawTool(tool) {
    state.draw.tool = tool === 'erase' ? 'erase' : 'pen';
    ui.drawPenBtn?.classList.toggle('active', state.draw.tool === 'pen');
    ui.drawEraseBtn?.classList.toggle('active', state.draw.tool === 'erase');
  }

  async function getAvatar(user) {
    const u = String(user || '').trim().toLowerCase();
    if (!u) return '';
    if (state.avatarCache[u] !== undefined) return state.avatarCache[u];
    try {
      const res = await fetch(`/api/user_profile/${encodeURIComponent(u)}?me=${encodeURIComponent(username)}`);
      if (!res.ok) {
        state.avatarCache[u] = '';
        return '';
      }
      const data = await res.json();
      state.avatarCache[u] = data.avatar || '';
      return state.avatarCache[u];
    } catch {
      state.avatarCache[u] = '';
      return '';
    }
  }

  function makeInitial(name) {
    const n = String(name || '?').trim();
    return (n[0] || '?').toUpperCase();
  }

  function ensureAudioContext() {
    if (state.ring.ctx) return state.ring.ctx;
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return null;
    state.ring.ctx = new Ctx();
    return state.ring.ctx;
  }

  function beep(freq = 740, duration = 160, volume = 0.035) {
    const ctx = ensureAudioContext();
    if (!ctx) return;
    if (ctx.state === 'suspended') {
      ctx.resume().catch(() => {});
    }
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'sine';
    osc.frequency.value = freq;
    gain.gain.value = volume;
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start();
    osc.stop(ctx.currentTime + duration / 1000);
  }

  function stopRinging() {
    state.ring.mode = null;
    if (typeof window.hideCallToast === 'function') {
      window.hideCallToast();
    }
    if (state.outgoingRingStopTimer) {
      clearTimeout(state.outgoingRingStopTimer);
      state.outgoingRingStopTimer = null;
    }
    if (state.ring.intervalId) {
      clearInterval(state.ring.intervalId);
      state.ring.intervalId = null;
    }
    for (const tid of state.ring.timeoutIds) clearTimeout(tid);
    state.ring.timeoutIds = [];
  }

  function startRinging(mode) {
    if (state.ring.mode === mode) return;
    stopRinging();
    state.ring.mode = mode;
    if (typeof window.showCallToast === 'function') {
      window.showCallToast(
        mode === 'incoming' ? 'Входящий звонок...' : 'Идут гудки...',
        mode
      );
    }

    if (mode === 'incoming') {
      const run = () => {
        beep(880, 140);
        const t = setTimeout(() => beep(880, 140), 220);
        state.ring.timeoutIds.push(t);
      };
      run();
      state.ring.intervalId = setInterval(run, 1000);
      return;
    }

    if (mode === 'outgoing') {
      const run = () => beep(460, 250, 0.028);
      run();
      state.ring.intervalId = setInterval(run, 1200);
    }
  }

  function makeDraggable(handle, target) {
    let pressed = false;
    let dragging = false;
    let pointerId = null;
    let sx = 0, sy = 0, ox = 0, oy = 0;

    const stopAll = () => {
      pressed = false;
      dragging = false;
      pointerId = null;
      handle.classList.remove('dragging');
    };

    handle.addEventListener('pointerdown', (e) => {
      if (e.button !== 0) return;
      if (e.target.closest('button,select,input,textarea,a')) return;
      pressed = true;
      dragging = false;
      pointerId = e.pointerId;
      sx = e.clientX;
      sy = e.clientY;
      const rect = target.getBoundingClientRect();
      ox = rect.left;
      oy = rect.top;
      target.style.transform = 'none';
      handle.classList.add('dragging');
      handle.setPointerCapture(e.pointerId);
    });

    handle.addEventListener('pointermove', (e) => {
      if (!pressed || pointerId !== e.pointerId) return;
      if ((e.buttons & 1) !== 1) {
        stopAll();
        return;
      }
      const dx = e.clientX - sx;
      const dy = e.clientY - sy;
      if (!dragging && (Math.abs(dx) > 3 || Math.abs(dy) > 3)) {
        dragging = true;
      }
      if (!dragging) return;
      const nx = ox + dx;
      const ny = oy + dy;
      target.style.left = `${Math.max(8, nx)}px`;
      target.style.top = `${Math.max(8, ny)}px`;
    });

    handle.addEventListener('pointerup', stopAll);
    handle.addEventListener('pointercancel', stopAll);
    window.addEventListener('mouseup', stopAll);
  }

  function makeResizable(target) {
    const dirs = ['n', 'e', 's', 'w', 'ne', 'nw', 'se', 'sw'];
    dirs.forEach((d) => {
      const h = document.createElement('div');
      h.className = `call-resize-handle call-resize-${d}`;
      h.dataset.dir = d;
      target.appendChild(h);
    });

    let activeDir = '';
    let pointerId = null;
    let sx = 0;
    let sy = 0;
    let start = null;

    const stop = () => {
      activeDir = '';
      pointerId = null;
      document.body.classList.remove('call-resizing');
    };

    target.addEventListener('pointerdown', (e) => {
      const handle = e.target.closest('.call-resize-handle');
      if (!handle || e.button !== 0) return;
      e.preventDefault();
      const rect = target.getBoundingClientRect();
      activeDir = handle.dataset.dir || '';
      pointerId = e.pointerId;
      sx = e.clientX;
      sy = e.clientY;
      start = { left: rect.left, top: rect.top, width: rect.width, height: rect.height };
      target.style.transform = 'none';
      target.setPointerCapture(e.pointerId);
      document.body.classList.add('call-resizing');
    });

    target.addEventListener('pointermove', (e) => {
      if (!activeDir || pointerId !== e.pointerId || !start) return;
      const minW = parseInt(ui.panel.style.minWidth || '360', 10);
      const minH = parseInt(ui.panel.style.minHeight || '250', 10);
      const maxW = parseInt(ui.panel.style.maxWidth || `${window.innerWidth - 16}`, 10);
      const maxH = parseInt(ui.panel.style.maxHeight || `${window.innerHeight - 16}`, 10);
      let width = start.width;
      let height = start.height;
      let left = start.left;
      let top = start.top;
      const dx = e.clientX - sx;
      const dy = e.clientY - sy;

      if (activeDir.includes('e')) width = start.width + dx;
      if (activeDir.includes('s')) height = start.height + dy;
      if (activeDir.includes('w')) {
        width = start.width - dx;
        left = start.left + dx;
      }
      if (activeDir.includes('n')) {
        height = start.height - dy;
        top = start.top + dy;
      }

      if (width < minW) {
        if (activeDir.includes('w')) left -= (minW - width);
        width = minW;
      }
      if (width > maxW) {
        if (activeDir.includes('w')) left += (width - maxW);
        width = maxW;
      }
      if (height < minH) {
        if (activeDir.includes('n')) top -= (minH - height);
        height = minH;
      }
      if (height > maxH) {
        if (activeDir.includes('n')) top += (height - maxH);
        height = maxH;
      }

      const sideGap = 10;
      const leftExtra = !ui.chatPanel.classList.contains('hidden') ? (ui.chatPanel.getBoundingClientRect().width + sideGap) : 0;
      const rightExtra = !ui.settingsBox.classList.contains('hidden') ? (ui.settingsBox.getBoundingClientRect().width + sideGap) : 0;
      const minLeft = 8 + leftExtra;
      const maxLeft = Math.max(minLeft, window.innerWidth - rightExtra - 8 - width);
      const maxTop = Math.max(8, window.innerHeight - 8 - height);
      left = Math.max(minLeft, Math.min(maxLeft, left));
      top = Math.max(8, Math.min(maxTop, top));

      target.style.width = `${Math.round(width)}px`;
      target.style.height = `${Math.round(height)}px`;
      target.style.left = `${Math.round(left)}px`;
      target.style.top = `${Math.round(top)}px`;
      resizeCanvas();
      enforcePanelBounds();
    });

    target.addEventListener('pointerup', stop);
    target.addEventListener('pointercancel', stop);
    window.addEventListener('mouseup', stop);
  }

  function makeFloatingDraggable(handle, target) {
    if (!handle || !target) return;
    let active = false;
    let pid = null;
    let sx = 0;
    let sy = 0;
    let ox = 0;
    let oy = 0;
    const stop = () => {
      active = false;
      pid = null;
    };
    handle.addEventListener('pointerdown', (e) => {
      if (e.button !== 0) return;
      if (document.fullscreenElement !== ui.panel) return;
      active = true;
      pid = e.pointerId;
      sx = e.clientX;
      sy = e.clientY;
      const r = target.getBoundingClientRect();
      ox = r.left;
      oy = r.top;
      target.style.left = `${ox}px`;
      target.style.top = `${oy}px`;
      target.style.right = 'auto';
      target.style.bottom = 'auto';
      target.classList.add('floating-free');
      handle.setPointerCapture(e.pointerId);
    });
    handle.addEventListener('pointermove', (e) => {
      if (!active || pid !== e.pointerId) return;
      const nx = ox + (e.clientX - sx);
      const ny = oy + (e.clientY - sy);
      const maxX = window.innerWidth - target.offsetWidth - 8;
      const maxY = window.innerHeight - target.offsetHeight - 8;
      target.style.left = `${Math.max(8, Math.min(maxX, nx))}px`;
      target.style.top = `${Math.max(8, Math.min(maxY, ny))}px`;
    });
    handle.addEventListener('pointerup', stop);
    handle.addEventListener('pointercancel', stop);
  }

  function getCurrentChat() {
    const c = (window.currentChat || '').trim();
    return c || null;
  }

  async function resolveTargets(chatId) {
    if (!chatId) return [];
    if (chatId.startsWith('group_')) {
      try {
        const res = await fetch(`/api/group_info/${chatId}`);
        if (!res.ok) return [];
        const data = await res.json();
        return (data.members || []).map(v => String(v).toLowerCase()).filter(v => v && v !== username);
      } catch {
        return [];
      }
    }
    return [chatId.toLowerCase()].filter(v => v !== username);
  }

  function createLayout() {
    const chatHeader = document.getElementById('chatHeader');
    const chatHint = document.getElementById('chatHeaderHint');
    const headerActions = document.createElement('div');
    headerActions.id = 'callHeaderActions';
    headerActions.className = 'call-header-actions-inline hidden';
    const startAudioBtn = mkButton('callStartAudio', icons.phone, 'Аудиозвонок');
    const startVideoBtn = mkButton('callStartVideo', icons.video, 'Видеозвонок');
    headerActions.append(startAudioBtn, startVideoBtn);

    const liveIndicator = document.createElement('button');
    liveIndicator.id = 'callLiveIndicator';
    liveIndicator.className = 'call-live-indicator hidden';
    liveIndicator.textContent = 'Идет звонок';
    liveIndicator.title = 'Войти в активный звонок';
    if (chatHeader) {
      if (chatHint && chatHint.parentNode === chatHeader) {
        chatHeader.insertBefore(headerActions, chatHint);
        chatHeader.insertBefore(liveIndicator, chatHint);
      } else {
        chatHeader.append(headerActions);
        chatHeader.append(liveIndicator);
      }
    }

    const incoming = document.createElement('div');
    incoming.id = 'callIncoming';
    incoming.className = 'call-incoming hidden';
    incoming.innerHTML = `
      <div class="call-incoming-title">Входящий звонок</div>
      <div class="call-incoming-sub" id="callIncomingSub"></div>
      <div class="call-incoming-actions"></div>
    `;
    const inActions = incoming.querySelector('.call-incoming-actions');
    const accept = mkButton('callAccept', icons.phone, 'Принять', 'ok');
    const reject = mkButton('callReject', icons.end, 'Отклонить', 'danger');
    inActions.append(accept, reject);

    const panel = document.createElement('div');
    panel.id = 'callPanel';
    panel.className = 'call-panel hidden';
    panel.innerHTML = `
      <div class="call-panel-header">
        <div class="call-panel-headline">
          <div>
          <div class="call-title" id="callTitle">Звонок</div>
          <div class="call-sub" id="callSub">Подключение...</div>
          </div>
        </div>
        <button id="callMinimizeBtn" class="call-btn call-minimize-btn" title="Свернуть">—</button>
      </div>
      <div class="call-avatars" id="callAvatars"></div>
      <div class="call-status-text" id="callStatusText">Подключение...</div>
      <div class="call-stage" id="callStage">
        <div class="call-videos" id="callVideos"></div>
        <canvas id="callDrawCanvas"></canvas>
      </div>
      <div id="callAudioRack" class="call-audio-rack"></div>
      <audio id="callTestSpeakerAudio" class="hidden"></audio>
      <div class="call-side-panel call-side-left hidden" id="callChatPanel">
        <div class="call-side-title call-side-title-row"><span>Чат звонка</span><button id="callChatClose" class="call-side-close" type="button" aria-label="Закрыть">✕</button></div>
        <div class="call-chat-list" id="callChatList"></div>
        <div class="call-chat-send">
          <input id="callChatInput" placeholder="Сообщение...">
          <button id="callChatSend" title="Отправить">${icons.plane}</button>
        </div>
      </div>
      <div class="call-side-panel call-side-right hidden" id="callSettings">
        <div class="call-side-title call-side-title-row"><span>Настройки</span><button id="callSettingsClose" class="call-side-close" type="button" aria-label="Закрыть">✕</button></div>
        <div class="call-settings-grid">
          <label class="call-select-wrap">
            Камера
            <select id="callCamSelect"></select>
          </label>
          <label class="call-select-wrap">
            Микрофон
            <select id="callMicSelect"></select>
          </label>
          <label class="call-select-wrap">
            Динамики
            <select id="callSpeakerSelect"></select>
          </label>
          <label class="call-switch-wrap">
            <input type="checkbox" id="callNoiseSuppression">
            <span>Шумоизоляция</span>
          </label>
          <label class="call-select-wrap">
            Доступ к рисованию
            <select id="callDrawPermSelect">
              <option value="off">Только я</option>
              <option value="on">Все</option>
            </select>
          </label>
          <button id="callAdvancedToggle" class="call-adv-toggle" type="button">Открыть расширенные настройки</button>
        </div>
        <div id="callAdvancedSettings" class="call-advanced hidden">
          <div class="call-adv-back-row">
            <button id="callAdvancedBack" class="call-adv-back" type="button">Назад</button>
            <button id="callAdvancedClose" class="call-side-close" type="button" aria-label="Закрыть">✕</button>
          </div>
          <div class="call-adv-layout">
            <div class="call-adv-content">
              <div id="callAdvPaneAudio" class="call-adv-pane">
                <div class="call-adv-section">
                  <div class="call-adv-title">Микрофон</div>
                  <label class="call-select-wrap">
                    Устройство
                    <select id="callMicSelectAdv"></select>
                  </label>
                  <label class="call-select-wrap" style="margin-top:8px">
                    Громкость микрофона
                    <div style="display:flex;align-items:center;gap:8px;margin-top:4px">
                      <input type="range" id="callMicVolumeSlider" min="0" max="200" value="100" style="flex:1;accent-color:#22c55e">
                      <span id="callMicVolumeVal" style="font-size:11px;color:#93c5fd;min-width:36px">100%</span>
                    </div>
                  </label>
                  <label class="call-switch-wrap" style="margin-top:8px">
                    <input type="checkbox" id="callNoiseSuppAdv">
                    <span>Шумоизоляция (Web Audio)</span>
                  </label>
                  <div class="call-adv-row" style="margin-top:10px">
                    <button id="callTestMicBtn" type="button" class="call-adv-btn">Записать голос</button>
                    <button id="callMicPlayBtn" type="button" class="call-adv-btn hidden">▶ Воспроизвести</button>
                    <button id="callMicResetBtn" type="button" class="call-adv-btn hidden">✕ Сброс</button>
                  </div>
                  <div class="call-mic-meter" style="margin-top:8px"><div id="callMicLevel"></div></div>
                  <audio id="callMicPlayback" class="hidden" controls></audio>
                </div>
              </div>
              <div id="callAdvPaneSound" class="call-adv-pane hidden">
                <div class="call-adv-section">
                  <div class="call-adv-title">Звук</div>
                  <label class="call-select-wrap">
                    Динамики
                    <select id="callSpeakerSelectAdv"></select>
                  </label>
                  <label class="call-select-wrap" style="margin-top:8px">
                    Громкость звука
                    <div style="display:flex;align-items:center;gap:8px;margin-top:4px">
                      <input type="range" id="callSpeakerVolumeSlider" min="0" max="200" value="100" style="flex:1;accent-color:#3b82f6">
                      <span id="callSpeakerVolumeVal" style="font-size:11px;color:#93c5fd;min-width:36px">100%</span>
                    </div>
                  </label>
                  <button id="callTestSpeakerBtn" type="button" class="call-adv-btn" style="margin-top:10px">🔊 Проверить звук</button>
                </div>
              </div>
              <div id="callAdvPaneVideo" class="call-adv-pane hidden">
                <div class="call-adv-section">
                  <div class="call-adv-title">Камера</div>
                  <label class="call-select-wrap">
                    Устройство
                    <select id="callCamSelectAdv"></select>
                  </label>
                  <label class="call-select-wrap" style="margin-top:8px">
                    Качество видео
                    <select id="callVideoQualityAdv">
                      <option value="high">Высокое (720p 30fps)</option>
                      <option value="medium">Среднее (480p 24fps)</option>
                      <option value="low">Низкое (360p 15fps)</option>
                    </select>
                  </label>
                  <label class="call-switch-wrap" style="margin-top:8px">
                    <input type="checkbox" id="callMirrorCamAdv" checked>
                    <span>Зеркальное отражение</span>
                  </label>
                  <button id="callTestCamBtn" type="button" class="call-adv-btn" style="margin-top:8px">📷 Проверить камеру</button>
                  <div class="call-cam-test-wrap">
                    <video id="callTestCamVideo" class="call-test-cam hidden" autoplay playsinline muted></video>
                  </div>
                </div>
                <div class="call-adv-section" style="margin-top:10px">
                  <div class="call-adv-title">Виртуальный фон</div>
                  <label class="call-select-wrap hidden">
                    Тип фона
                    <select id="callBackgroundSelect">
                      <option value="none">Без фона</option>
                      <option value="blur">Размытие</option>
                      <option value="blur-strong">Сильное размытие</option>
                      <option value="dark">Затемнение</option>
                      <option value="image">Своя картинка</option>
                      <option value="preset-office">🏢 Офис</option>
                      <option value="preset-nature">🌿 Природа</option>
                      <option value="preset-city">🌆 Город</option>
                      <option value="preset-space">🌌 Космос</option>
                      <option value="preset-studio">🎙️ Студия</option>
                    </select>
                  </label>
                  <div id="callBgPresetsRow" class="call-bg-presets call-bg-zoom-grid"></div>
                  <label class="call-select-wrap hidden" id="callBgImageWrap" style="margin-top:8px">
                    Картинка (только для "Своя")
                    <input type="file" id="callBgImageInput" accept="image/*">
                  </label>
                  <canvas id="callBgPreview" class="call-bg-preview hidden" width="160" height="90"></canvas>
                </div>
              </div>
            </div>
            <div class="call-adv-nav">
              <button id="callAdvTabAudio" class="call-adv-tab active" type="button">🎤 Аудио</button>
              <button id="callAdvTabSound" class="call-adv-tab" type="button">🔊 Звук</button>
              <button id="callAdvTabVideo" class="call-adv-tab" type="button">📷 Видео</button>
            </div>
          </div>
        </div>
      </div>
      <div class="call-draw-tools hidden" id="callDrawTools">
        <div class="call-float-grip" id="callDrawGrip">⋮⋮</div>
        <button id="drawToolPen" class="draw-tool-btn active" title="Кисть">✏️</button>
        <button id="drawToolErase" class="draw-tool-btn" title="Ластик">🩹</button>
        <button id="drawClearBtn" class="draw-tool-btn" title="Стереть все">🧹</button>
        <div class="draw-size-row" id="drawSizeRow">
          <button class="draw-size-dot active" data-size="2"><span></span></button>
          <button class="draw-size-dot" data-size="4"><span></span></button>
          <button class="draw-size-dot" data-size="7"><span></span></button>
        </div>
        <div class="draw-color-row" id="drawColorRow"></div>
      </div>
      <div class="call-controls" id="callControls">
        <div class="call-float-grip" id="callControlsGrip">⋮⋮</div>
      </div>
    `;

    const controls = panel.querySelector('#callControls');
    const micBtn = mkButton('callMicBtn', icons.mic, 'Микрофон');
    const hpBtn = mkButton('callHeadphonesBtn', icons.headphones, 'Наушники');
    const volumeBtn = mkButton('callVolumeBtn', icons.volumeHigh, 'Громкость');
    const camBtn = mkButton('callCamBtn', icons.cam, 'Камера');
    const screenBtn = mkButton('callScreenBtn', icons.screen, 'Демонстрация');
    const drawBtn = mkButton('callDrawBtn', icons.draw, 'Рисование');
    const controlBtn = mkButton('callControlBtn', icons.hand, 'Запросить управление');
    const chatBtn = mkButton('callChatBtn', icons.chat, 'Чат звонка');
    const settingsBtn = mkButton('callSettingsBtn', icons.settings, 'Настройки');
    const moreBtn = mkButton('callMoreBtn', '⋯', 'Ещё');
    moreBtn.classList.add('call-more-btn');
    const endBtn = mkButton('callEndBtn', icons.end, 'Завершить', 'danger');
    controls.append(micBtn, hpBtn, volumeBtn, camBtn, screenBtn, drawBtn, controlBtn, chatBtn, settingsBtn, moreBtn, endBtn);

    const mobileMoreMenu = document.createElement('div');
    mobileMoreMenu.id = 'callMobileMoreMenu';
    mobileMoreMenu.className = 'call-mobile-more hidden';
    mobileMoreMenu.innerHTML = `
      <button type="button" data-action="screen">${icons.screen}<span>Демонстрация</span></button>
      <button type="button" data-action="draw">${icons.draw}<span>Рисование</span></button>
      <button type="button" data-action="control">${icons.hand}<span>Запросить доступ</span></button>
      <button type="button" data-action="chat">${icons.chat}<span>Чат звонка</span></button>
      <button type="button" data-action="settings">${icons.settings}<span>Настройки</span></button>
      <button type="button" data-action="advanced">${icons.settings}<span>Расширенные</span></button>
    `;
    panel.appendChild(mobileMoreMenu);

    const reopenBtn = document.createElement('button');
    reopenBtn.id = 'callReopenBtn';
    reopenBtn.className = 'call-reopen-chip hidden';
    reopenBtn.textContent = 'Вернуться к звонку';
    document.body.append(incoming, panel, reopenBtn);

    ui.headerActions = headerActions;
    ui.liveIndicator = liveIndicator;
    ui.reopenBtn = reopenBtn;
    ui.startAudioBtn = startAudioBtn;
    ui.startVideoBtn = startVideoBtn;
    ui.incoming = incoming;
    ui.incomingSub = incoming.querySelector('#callIncomingSub');
    ui.acceptBtn = accept;
    ui.rejectBtn = reject;
    ui.panel = panel;
    ui.title = panel.querySelector('#callTitle');
    ui.sub = panel.querySelector('#callSub');
    ui.statusText = panel.querySelector('#callStatusText');
    ui.avatars = panel.querySelector('#callAvatars');
    ui.stage = panel.querySelector('#callStage');
    ui.videos = panel.querySelector('#callVideos');
    ui.canvas = panel.querySelector('#callDrawCanvas');
    ui.micBtn = micBtn;
    ui.minimizeBtn = panel.querySelector('#callMinimizeBtn');
    ui.hpBtn = hpBtn;
    ui.volumeBtn = volumeBtn;
    ui.camBtn = camBtn;
    ui.screenBtn = screenBtn;
    ui.drawBtn = drawBtn;
    ui.controlBtn = controlBtn;
    ui.chatBtn = chatBtn;
    ui.settingsBtn = settingsBtn;
    ui.moreBtn = moreBtn;
    ui.mobileMoreMenu = mobileMoreMenu;
    ui.endBtn = endBtn;
    ui.chatPanel = panel.querySelector('#callChatPanel');
    ui.chatClose = panel.querySelector('#callChatClose');
    ui.chatList = panel.querySelector('#callChatList');
    ui.chatInput = panel.querySelector('#callChatInput');
    ui.chatSend = panel.querySelector('#callChatSend');
    ui.settingsBox = panel.querySelector('#callSettings');
    ui.drawPermSelect = panel.querySelector('#callDrawPermSelect');
    ui.drawTools = panel.querySelector('#callDrawTools');
    ui.drawPenBtn = panel.querySelector('#drawToolPen');
    ui.drawEraseBtn = panel.querySelector('#drawToolErase');
    ui.drawClearBtn = panel.querySelector('#drawClearBtn');
    ui.drawSizeRow = panel.querySelector('#drawSizeRow');
    ui.drawColorRow = panel.querySelector('#drawColorRow');
    ui.controlsGrip = panel.querySelector('#callControlsGrip');
    ui.drawGrip = panel.querySelector('#callDrawGrip');
    ui.audioRack = panel.querySelector('#callAudioRack');
    ui.testSpeakerAudio = panel.querySelector('#callTestSpeakerAudio');
    ui.camSelect = panel.querySelector('#callCamSelect');
    ui.micSelect = panel.querySelector('#callMicSelect');
    ui.speakerSelect = panel.querySelector('#callSpeakerSelect');
    ui.noiseSuppression = panel.querySelector('#callNoiseSuppression');
    ui.advancedToggle = panel.querySelector('#callAdvancedToggle');
    ui.advancedSettings = panel.querySelector('#callAdvancedSettings');
    ui.advancedHost = panel.querySelector('#callSettings');
    ui.advancedBack = panel.querySelector('#callAdvancedBack');
    ui.settingsClose = panel.querySelector('#callSettingsClose');
    ui.advancedClose = panel.querySelector('#callAdvancedClose');
    ui.advTabAudio = panel.querySelector('#callAdvTabAudio');
    ui.advTabSound = panel.querySelector('#callAdvTabSound');
    ui.advTabVideo = panel.querySelector('#callAdvTabVideo');
    ui.advPaneAudio = panel.querySelector('#callAdvPaneAudio');
    ui.advPaneSound = panel.querySelector('#callAdvPaneSound');
    ui.advPaneVideo = panel.querySelector('#callAdvPaneVideo');
    ui.camSelectAdv = panel.querySelector('#callCamSelectAdv');
    ui.micSelectAdv = panel.querySelector('#callMicSelectAdv');
    ui.speakerSelectAdv = panel.querySelector('#callSpeakerSelectAdv');
    ui.testMicBtn = panel.querySelector('#callTestMicBtn');
    ui.micPlayBtn = panel.querySelector('#callMicPlayBtn');
    ui.micResetBtn = panel.querySelector('#callMicResetBtn');
    ui.micPlayback = panel.querySelector('#callMicPlayback');
    ui.testSpeakerBtn = panel.querySelector('#callTestSpeakerBtn');
    ui.testCamBtn = panel.querySelector('#callTestCamBtn');
    ui.testCamVideo = panel.querySelector('#callTestCamVideo');
    ui.backgroundSelect = panel.querySelector('#callBackgroundSelect');
    ui.bgImageWrap = panel.querySelector('#callBgImageWrap');
    ui.bgImageInput = panel.querySelector('#callBgImageInput');
    ui.bgPresetsRow = panel.querySelector('#callBgPresetsRow');
    ui.bgPreviewCanvas = panel.querySelector('#callBgPreview');
    ui.micVolumeSlider = panel.querySelector('#callMicVolumeSlider');
    ui.micVolumeVal = panel.querySelector('#callMicVolumeVal');
    ui.speakerVolumeSlider = panel.querySelector('#callSpeakerVolumeSlider');
    ui.speakerVolumeVal = panel.querySelector('#callSpeakerVolumeVal');
    ui.noiseSuppAdv = panel.querySelector('#callNoiseSuppAdv');
    ui.videoQualityAdv = panel.querySelector('#callVideoQualityAdv');
    ui.mirrorCamAdv = panel.querySelector('#callMirrorCamAdv');

    if (!isMobileViewport()) {
      makeDraggable(panel.querySelector('.call-panel-header'), panel);
      makeResizable(panel);
      makeFloatingDraggable(ui.controlsGrip, controls);
      makeFloatingDraggable(ui.drawGrip, ui.drawTools);
    }
    setupCopyGuards(panel);

    if (ui.noiseSuppression) ui.noiseSuppression.checked = !!state.noiseSuppEnabled;
    if (ui.noiseSuppAdv) ui.noiseSuppAdv.checked = !!state.noiseSuppEnabled;
    if (ui.videoQualityAdv) ui.videoQualityAdv.value = state.videoQuality || 'high';
    if (ui.mirrorCamAdv) ui.mirrorCamAdv.checked = state.mirrorCamera !== false;
    if (ui.backgroundSelect) ui.backgroundSelect.value = state.backgroundMode || 'none';
    if (ui.micVolumeSlider) {
      ui.micVolumeSlider.value = String(Math.round((state.micVolume ?? 1) * 100));
      if (ui.micVolumeVal) ui.micVolumeVal.textContent = `${ui.micVolumeSlider.value}%`;
    }
    if (ui.speakerVolumeSlider) {
      ui.speakerVolumeSlider.value = String(Math.round((state.speakerVolume ?? 1) * 100));
      if (ui.speakerVolumeVal) ui.speakerVolumeVal.textContent = `${ui.speakerVolumeSlider.value}%`;
    }
    updateCallVolumeButtonState();
    updateBgPresetsUI();

    initDrawToolUi();
    setupDrawing();
    updatePanelLimits();
  }

  function updateHeaderActionsVisibility() {
    const chat = getCurrentChat();
    state.currentChat = chat;
    if (!ui.headerActions) return;
    ui.headerActions.classList.toggle('hidden', !chat);
    ui.startAudioBtn.disabled = state.inCall;
    ui.startVideoBtn.disabled = state.inCall;
    ui.startAudioBtn.classList.toggle('disabled', state.inCall);
    ui.startVideoBtn.classList.toggle('disabled', state.inCall);
    updateLiveIndicator();
  }

  function getActiveCallForCurrentChat() {
    const chat = getCurrentChat();
    if (!chat) return null;
    const info = state.activeCallsByChat[chat];
    if (!info || !info.active) return null;
    return info;
  }

  function normalizeChatKeys(chatId) {
    const c = String(chatId || '').trim().toLowerCase();
    if (!c) return [];
    if (c.startsWith('group_')) return [c];
    if (c.includes('_')) {
      const parts = c.split('_');
      const peer = parts.find(p => p && p !== username) || c;
      return [c, peer];
    }
    return [c, [username, c].sort().join('_')];
  }

  function updateLiveIndicator() {
    if (!ui.liveIndicator) return;
    const info = getActiveCallForCurrentChat();
    const canJoin = !!info && !state.inCall;
    const canRestore = !!state.inCall && !!state.callMinimized;
    ui.liveIndicator.classList.toggle('hidden', !(canJoin || canRestore));
    if (!(canJoin || canRestore)) return;

    let text = canRestore ? 'Вернуться к звонку' : 'Идет звонок';
    const participants = Array.isArray(info?.participants) ? info.participants : [];
    if (!canRestore && info?.lone_since && participants.length <= 1) {
      const sec = Math.max(0, 180 - Math.floor((Date.now() / 1000) - Number(info.lone_since)));
      const m = Math.floor(sec / 60).toString().padStart(2, '0');
      const s = (sec % 60).toString().padStart(2, '0');
      text = `Ожидание звонка • ${m}:${s}`;
    }
    ui.liveIndicator.textContent = text;
  }

  function setCallMinimized(flag) {
    state.callMinimized = !!flag;
    ui.panel?.classList.toggle('minimized', state.callMinimized);
    ui.reopenBtn?.classList.toggle('hidden', !(state.inCall && state.callMinimized));
    updateLiveIndicator();
  }

  function getSpeakerVolumeBand(v) {
    const vol = Math.max(0, Math.min(1, Number(v) || 0));
    if (vol <= 0.45) return 'low';
    if (vol <= 0.8) return 'mid';
    return 'high';
  }

  function updateCallVolumeButtonState() {
    if (!ui.volumeBtn) return;
    const band = getSpeakerVolumeBand(state.speakerVolume);
    if (band === 'high') ui.volumeBtn.innerHTML = icons.volumeHigh;
    else if (band === 'mid') ui.volumeBtn.innerHTML = icons.volumeMid;
    else ui.volumeBtn.innerHTML = icons.volumeLow;
    ui.volumeBtn.title = `Громкость звонка: ${Math.round((state.speakerVolume || 0) * 100)}%`;
  }

  function cycleSpeakerVolume() {
    const presets = [0.35, 0.7, 1.0];
    const current = Math.max(0, Math.min(1, Number(state.speakerVolume) || 0));
    let idx = 0;
    if (current > 0.85) idx = 2;
    else if (current > 0.5) idx = 1;
    idx = (idx + 1) % presets.length;
    state.speakerVolume = presets[idx];
    if (ui.speakerVolumeSlider) {
      ui.speakerVolumeSlider.value = String(Math.round(state.speakerVolume * 100));
    }
    if (ui.speakerVolumeVal) {
      ui.speakerVolumeVal.textContent = `${Math.round(state.speakerVolume * 100)}%`;
    }
    applySpeakerVolume();
    updateCallVolumeButtonState();
    saveCallPrefs();
    notify(`Громкость звонка: ${Math.round(state.speakerVolume * 100)}%`);
  }

  function setupEvents() {
    const closeMobileMoreMenu = () => {
      ui.mobileMoreMenu?.classList.add('hidden');
    };
    const openMobileOverlay = (kind) => {
      if (kind === 'chat') {
        ui.settingsBox.classList.add('hidden');
        ui.chatPanel.classList.remove('hidden');
        ui.panel.classList.add('mobile-overlay-open');
      } else if (kind === 'settings') {
        ui.chatPanel.classList.add('hidden');
        ui.settingsBox.classList.remove('hidden');
        if (!ui.advancedSettings.classList.contains('hidden')) toggleAdvancedSettings();
        ui.panel.classList.add('mobile-overlay-open');
      } else if (kind === 'advanced') {
        ui.chatPanel.classList.add('hidden');
        ui.settingsBox.classList.remove('hidden');
        if (ui.advancedSettings.classList.contains('hidden')) toggleAdvancedSettings();
        ui.panel.classList.add('mobile-overlay-open');
      } else {
        ui.panel.classList.remove('mobile-overlay-open');
      }
      enforcePanelBounds();
    };

    ui.startAudioBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      startCall('audio');
    });
    ui.startVideoBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      startCall('video');
    });
    ui.startAudioBtn.addEventListener('pointerdown', (e) => e.stopPropagation());
    ui.startVideoBtn.addEventListener('pointerdown', (e) => e.stopPropagation());
    ui.acceptBtn.addEventListener('click', acceptIncoming);
    ui.rejectBtn.addEventListener('click', rejectIncoming);
    ui.endBtn.addEventListener('click', () => endCall(true));
    ui.micBtn.addEventListener('click', toggleMic);
    ui.hpBtn.addEventListener('click', toggleHeadphones);
    ui.volumeBtn?.addEventListener('click', cycleSpeakerVolume);
    ui.camBtn.addEventListener('click', toggleCam);
    ui.screenBtn.addEventListener('click', toggleScreenShare);
    ui.drawBtn.addEventListener('click', toggleDrawMode);
    ui.drawPenBtn.addEventListener('click', () => setDrawTool('pen'));
    ui.drawEraseBtn.addEventListener('click', () => setDrawTool('erase'));
    ui.drawClearBtn.addEventListener('click', clearDrawings);
    ui.drawSizeRow.querySelectorAll('.draw-size-dot').forEach((btn) => {
      btn.addEventListener('click', () => {
        state.draw.size = Number(btn.dataset.size || 3);
        ui.drawSizeRow.querySelectorAll('.draw-size-dot').forEach(x => x.classList.remove('active'));
        btn.classList.add('active');
      });
    });
    ui.controlBtn.addEventListener('click', requestControl);
    ui.chatBtn.addEventListener('click', () => {
      closeMobileMoreMenu();
      if (isMobileViewport()) {
        openMobileOverlay('chat');
        return;
      }
      ui.chatPanel.classList.toggle('hidden');
      enforcePanelBounds();
    });
    ui.settingsBtn.addEventListener('click', () => {
      closeMobileMoreMenu();
      if (isMobileViewport()) {
        openMobileOverlay('settings');
        return;
      }
      const willHide = !ui.settingsBox.classList.contains('hidden');
      ui.settingsBox.classList.toggle('hidden');
      if (willHide && !ui.advancedSettings.classList.contains('hidden')) {
        toggleAdvancedSettings();
      }
      refreshDevices().catch(() => {});
      enforcePanelBounds();
    });
    ui.settingsClose?.addEventListener('click', () => {
      if (!ui.advancedSettings.classList.contains('hidden')) toggleAdvancedSettings();
      ui.settingsBox.classList.add('hidden');
      ui.panel.classList.remove('mobile-overlay-open');
      closeMobileMoreMenu();
      enforcePanelBounds();
    });
    ui.chatClose?.addEventListener('click', () => {
      ui.chatPanel.classList.add('hidden');
      ui.panel.classList.remove('mobile-overlay-open');
      closeMobileMoreMenu();
      enforcePanelBounds();
    });
    ui.moreBtn?.addEventListener('click', (e) => {
      e.stopPropagation();
      if (!isMobileViewport()) return;
      ui.mobileMoreMenu?.classList.toggle('hidden');
    });
    ui.mobileMoreMenu?.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-action]');
      if (!btn) return;
      const action = btn.dataset.action || '';
      closeMobileMoreMenu();
      if (action === 'screen') ui.screenBtn.click();
      else if (action === 'draw') ui.drawBtn.click();
      else if (action === 'control') ui.controlBtn.click();
      else if (action === 'chat') openMobileOverlay('chat');
      else if (action === 'settings') openMobileOverlay('settings');
      else if (action === 'advanced') openMobileOverlay('advanced');
    });
    document.addEventListener('click', (e) => {
      if (!isMobileViewport()) return;
      if (e.target.closest('#callMoreBtn') || e.target.closest('#callMobileMoreMenu')) return;
      closeMobileMoreMenu();
    });
    ui.chatSend.addEventListener('click', sendCallChat);
    ui.chatInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') sendCallChat();
    });
    ui.drawPermSelect.addEventListener('change', onDrawPermChanged);
    ui.liveIndicator.addEventListener('click', async (e) => {
      e.stopPropagation();
      if (state.inCall && state.callMinimized) {
        setCallMinimized(false);
        return;
      }
      const info = getActiveCallForCurrentChat();
      if (!info) return;
      await joinExistingCall(info.call_id, info.chat_id, info.mode || 'audio');
    });
    ui.reopenBtn.addEventListener('click', () => setCallMinimized(false));
    ui.minimizeBtn?.addEventListener('click', () => setCallMinimized(true));
    ui.camSelect.addEventListener('change', switchCamera);
    ui.micSelect.addEventListener('change', switchMicrophone);
    ui.speakerSelect.addEventListener('change', switchSpeaker);
    ui.camSelectAdv.addEventListener('change', () => {
      ui.camSelect.value = ui.camSelectAdv.value;
      switchCamera();
    });
    ui.micSelectAdv.addEventListener('change', () => {
      ui.micSelect.value = ui.micSelectAdv.value;
      switchMicrophone();
    });
    ui.speakerSelectAdv.addEventListener('change', () => {
      ui.speakerSelect.value = ui.speakerSelectAdv.value;
      switchSpeaker();
    });
    ui.noiseSuppression.addEventListener('change', () => {
      setNoiseSuppression(ui.noiseSuppression.checked);
    });
    // Advanced settings sync
    if (ui.noiseSuppAdv) {
      ui.noiseSuppAdv.addEventListener('change', () => {
        setNoiseSuppression(ui.noiseSuppAdv.checked);
      });
    }
    if (ui.micVolumeSlider) {
      ui.micVolumeSlider.addEventListener('input', () => {
        const v = Number(ui.micVolumeSlider.value);
        if (ui.micVolumeVal) ui.micVolumeVal.textContent = `${v}%`;
        state.micVolume = v / 100;
        applyMicVolume();
        saveCallPrefs();
      });
    }
    if (ui.speakerVolumeSlider) {
      ui.speakerVolumeSlider.addEventListener('input', () => {
        const v = Number(ui.speakerVolumeSlider.value);
        if (ui.speakerVolumeVal) ui.speakerVolumeVal.textContent = `${v}%`;
        state.speakerVolume = v / 100;
        applySpeakerVolume();
        updateCallVolumeButtonState();
        saveCallPrefs();
      });
    }
    if (ui.videoQualityAdv) {
      ui.videoQualityAdv.addEventListener('change', () => {
        state.videoQuality = ui.videoQualityAdv.value;
        saveCallPrefs();
        applyVideoQuality().catch(() => {});
      });
    }
    if (ui.mirrorCamAdv) {
      ui.mirrorCamAdv.addEventListener('change', () => {
        state.mirrorCamera = ui.mirrorCamAdv.checked;
        saveCallPrefs();
        applyLocalMirrorState();
      });
    }
    ui.advancedToggle.addEventListener('click', toggleAdvancedSettings);
    ui.advancedBack.addEventListener('click', toggleAdvancedSettings);
    ui.advancedClose?.addEventListener('click', () => {
      if (!ui.advancedSettings.classList.contains('hidden')) toggleAdvancedSettings();
      ui.settingsBox.classList.add('hidden');
      ui.panel.classList.remove('mobile-overlay-open');
      closeMobileMoreMenu();
      enforcePanelBounds();
    });
    ui.advTabAudio.addEventListener('click', () => setAdvancedTab('audio'));
    ui.advTabSound.addEventListener('click', () => setAdvancedTab('sound'));
    ui.advTabVideo.addEventListener('click', () => setAdvancedTab('video'));
    ui.testMicBtn.addEventListener('click', toggleMicTest);
    ui.micPlayBtn.addEventListener('click', playMicRecording);
    ui.micResetBtn.addEventListener('click', resetMicRecording);
    ui.testSpeakerBtn.addEventListener('click', testSpeaker);
    ui.testCamBtn.addEventListener('click', toggleCamTest);
    ui.backgroundSelect.addEventListener('change', onBackgroundModeChanged);
    ui.bgImageInput.addEventListener('change', onBgImageChosen);

    window.addEventListener('beforeunload', () => {
      if (state.inCall && state.activeCallId) {
        socket.emit('call_leave', { call_id: state.activeCallId, username });
      }
    });
    if (navigator.mediaDevices?.addEventListener) {
      navigator.mediaDevices.addEventListener('devicechange', () => {
        refreshDevices().catch(() => {});
      });
    }
    document.addEventListener('fullscreenchange', onFullscreenChanged);
    window.addEventListener('resize', () => {
      if (!isMobileViewport()) return;
      ui.mobileMoreMenu?.classList.add('hidden');
      enforcePanelBounds();
    });

    setInterval(() => {
      updateHeaderActionsVisibility();
      updateLiveIndicator();
    }, 400);
  }

  function setupCopyGuards(panel) {
    const allowed = (target) => {
      const el = target instanceof Element ? target : target?.parentElement;
      return !!el?.closest('.call-chat-row, #callChatInput');
    };
    panel.addEventListener('copy', (e) => {
      if (!allowed(e.target)) e.preventDefault();
    });
    panel.addEventListener('cut', (e) => {
      if (!allowed(e.target)) e.preventDefault();
    });
    panel.addEventListener('selectstart', (e) => {
      if (!allowed(e.target)) e.preventDefault();
    });
  }

  async function startCall(mode) {
    const chatId = getCurrentChat();
    if (!chatId || state.inCall) return;
    if (String(chatId).startsWith('group_')) {
      try {
        const res = await fetch(`/api/group_permissions/${encodeURIComponent(chatId)}?me=${encodeURIComponent(username)}`);
        const data = await res.json();
        if (res.ok) {
          const perms = data.permissions || {};
          const defaults = perms.defaults || {};
          const member = (perms.members || {})[username] || {};
          const canStart = (String(data.owner || '').toLowerCase() === username)
            ? true
            : (Object.prototype.hasOwnProperty.call(member, 'can_start_calls') ? !!member.can_start_calls : !!defaults.can_start_calls);
          if (!canStart) {
            notify('У вас нет прав на запуск звонков в этой группе');
            return;
          }
        }
      } catch (e) {
        logCallSilent('startCall.groupPermissions', e);
      }
    }

    const targets = await resolveTargets(chatId);
    if (!targets.length) {
      notify('Нет участников для звонка');
      return;
    }

    state.activeCallId = `call_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    state.activeChatId = chatId;
    state.lastCallChatId = chatId;
    state.dialTargets = targets.slice();
    state.inCall = true;
    state.callMode = mode;
    state.participants = new Set([username]);

    const ok = await setupLocalMedia(mode === 'video');
    if (!ok) {
      cleanupCall();
      return;
    }

    showPanel();
    updateCallMeta();
    startRinging('outgoing');
    state.outgoingRingStopTimer = setTimeout(() => {
      if (state.ring.mode === 'outgoing') {
        stopRinging();
        updateCallStatusText();
      }
    }, 30000);

    socket.emit('call_invite', {
      call_id: state.activeCallId,
      chat_id: chatId,
      from: username,
      targets,
      mode
    });

    socket.emit('call_join', {
      call_id: state.activeCallId,
      chat_id: chatId,
      username
    });
    localStorage.setItem('active_call', JSON.stringify({ call_id: state.activeCallId, chat_id: chatId, mode }));
  }

  async function joinExistingCall(callId, chatId, mode = 'audio') {
    if (!callId || !chatId || state.inCall) return;

    state.activeCallId = callId;
    state.activeChatId = chatId;
    state.lastCallChatId = chatId;
    state.inCall = true;
    state.callMode = mode;
    state.participants = new Set([username]);

    if (window.currentChat !== chatId && typeof window.openChat === 'function') {
      window.openChat(chatId).catch?.(() => {});
    }

    const ok = await setupLocalMedia(mode === 'video');
    if (!ok) {
      cleanupCall();
      return;
    }

    showPanel();
    updateCallMeta();

    socket.emit('call_join', {
      call_id: state.activeCallId,
      chat_id: state.activeChatId,
      username
    });
    localStorage.setItem('active_call', JSON.stringify({ call_id: state.activeCallId, chat_id: chatId, mode }));
  }

  function showIncoming(invite) {
    state.incomingInvite = invite;
    ui.incomingSub.textContent = `${invite.from} • ${invite.mode === 'video' ? 'Видео' : 'Аудио'}`;
    ui.incoming.classList.remove('hidden');
    startRinging('incoming');
  }

  async function acceptIncoming() {
    const invite = state.incomingInvite;
    if (!invite || state.inCall) return;

    state.activeCallId = invite.call_id;
    state.activeChatId = invite.chat_id;
    state.lastCallChatId = invite.chat_id;
    state.dialTargets = [invite.from];
    state.inCall = true;
    state.callMode = invite.mode || 'audio';
    state.participants = new Set([username]);

    const ok = await setupLocalMedia(state.callMode === 'video');
    if (!ok) {
      cleanupCall();
      return;
    }

    showPanel();
    updateCallMeta();
    stopRinging();

    socket.emit('call_join', {
      call_id: state.activeCallId,
      chat_id: state.activeChatId,
      username
    });
    localStorage.setItem('active_call', JSON.stringify({ call_id: state.activeCallId, chat_id: state.activeChatId, mode: state.callMode }));

    hideIncoming();
  }

  function rejectIncoming() {
    const invite = state.incomingInvite;
    if (invite?.call_id) {
      socket.emit('call_decline', {
        call_id: invite.call_id,
        username
      });
    }
    stopRinging();
    hideIncoming();
  }

  function hideIncoming() {
    ui.incoming.classList.add('hidden');
    state.incomingInvite = null;
    if (state.ring.mode === 'incoming') stopRinging();
  }

  function showPanel() {
    ui.panel.classList.remove('hidden');
    setCallMinimized(false);
    resetPanelToDefault();
    ui.chatPanel.classList.add('hidden');
    ui.settingsBox.classList.add('hidden');
    ui.panel.classList.remove('mobile-overlay-open');
    ui.mobileMoreMenu?.classList.add('hidden');
    updatePanelLimits();
    renderVideos();
    updateControlButtons();
    updateHeaderActionsVisibility();
    renderParticipantAvatars();
    enforcePanelBounds();
  }

  function resetPanelToDefault() {
    if (isMobileViewport()) {
      ui.panel.style.transform = 'none';
      ui.panel.style.left = '0';
      ui.panel.style.top = '0';
      ui.panel.style.right = '0';
      ui.panel.style.bottom = '0';
      ui.panel.style.width = '100vw';
      ui.panel.style.height = '100dvh';
      return;
    }
    const baseW = 760;
    const baseH = 620;
    const w = Math.max(420, Math.min(baseW, window.innerWidth - 24));
    const h = Math.max(320, Math.min(baseH, window.innerHeight - 24));
    ui.panel.style.transform = 'none';
    ui.panel.style.width = `${Math.round(w)}px`;
    ui.panel.style.height = `${Math.round(h)}px`;
    ui.panel.style.left = `${Math.max(8, Math.round((window.innerWidth - w) / 2))}px`;
    ui.panel.style.top = `${Math.max(52, Math.round((window.innerHeight - h) / 2))}px`;
  }

  function hidePanel() {
    ui.panel.classList.add('hidden');
    setCallMinimized(false);
    ui.settingsBox.classList.add('hidden');
    ui.panel.classList.remove('mobile-overlay-open');
    ui.mobileMoreMenu?.classList.add('hidden');
  }

  function updateCallMeta() {
    ui.title.textContent = state.activeChatId?.startsWith('group_') ? 'Групповой звонок' : 'Личный звонок';
    ui.sub.textContent = `Участников: ${state.participants.size} • E2E`;
    updateCallStatusText();
  }

  function updateCallStatusText() {
    if (!ui.statusText) return;
    const connected = state.participants.size > 1;
    if (!connected && state.dialTargets.length === 1) {
      ui.statusText.textContent = state.ring.mode === 'outgoing'
        ? `Вызываем @${state.dialTargets[0]}...`
        : `Ожидание @${state.dialTargets[0]}...`;
      return;
    }
    if (!connected) {
      ui.statusText.textContent = 'Подключение...';
      return;
    }
    ui.statusText.textContent = 'На связи';
  }

  async function setupLocalMedia(withVideo) {
    try {
      const micId = ui.micSelect?.value || state.audioDeviceId || '';
      const camId = ui.camSelect?.value || state.videoDeviceId || '';
      const qPresets = {
        high:   { width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 30 } },
        medium: { width: { ideal: 854 }, height: { ideal: 480 }, frameRate: { ideal: 24 } },
        low:    { width: { ideal: 640 }, height: { ideal: 360 }, frameRate: { ideal: 15 } }
      };
      const videoConstraints = withVideo ? {
        ...(qPresets[state.videoQuality] || qPresets.high),
        ...(camId ? { deviceId: { exact: camId } } : {})
      } : false;
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          ...(micId ? { deviceId: { exact: micId } } : {}),
          echoCancellation: true,
          noiseSuppression: false,
          autoGainControl: false,
          channelCount: 1
        },
        video: videoConstraints
      });
      state.localStream = stream;
      state.rawCameraTrack = stream.getVideoTracks?.()[0] || null;
      if (state.rawCameraTrack) {
        await applyBackgroundMode();
      }
      if (state.backgroundPending && state.rawCameraTrack) {
        await applyBackgroundMode();
      }
      await refreshDevices();
      await switchSpeaker();
      renderVideos();
      await refreshTestCamPreview();
      if (state.noiseSuppEnabled) {
        await toggleNoiseSuppression(true);
      }
      return true;
    } catch (err) {
      // Fallback: try without exact device constraint
      try {
        const stream = await navigator.mediaDevices.getUserMedia({
          audio: { echoCancellation: true, autoGainControl: false, channelCount: 1 },
          video: withVideo ? { width: { ideal: 1280 }, height: { ideal: 720 } } : false
        });
        state.localStream = stream;
        state.rawCameraTrack = stream.getVideoTracks?.()[0] || null;
        if (state.rawCameraTrack) {
          await applyBackgroundMode();
        }
        if (state.backgroundPending && state.rawCameraTrack) {
          await applyBackgroundMode();
        }
        await refreshDevices();
        await switchSpeaker();
        renderVideos();
        await refreshTestCamPreview();
        if (state.noiseSuppEnabled) {
          await toggleNoiseSuppression(true);
        }
        return true;
      } catch {
        notify('Нет доступа к камере/микрофону');
        return false;
      }
    }
  }

  async function refreshDevices() {
    if (!navigator.mediaDevices?.enumerateDevices) return;
    const devices = await navigator.mediaDevices.enumerateDevices();
    const cams = devices.filter(d => d.kind === 'videoinput');
    const mics = devices.filter(d => d.kind === 'audioinput');
    const speakers = devices.filter(d => d.kind === 'audiooutput');

    const fillSelect = (select, list, fallback) => {
      select.innerHTML = '';
      list.forEach((d, i) => {
        const fullLabel = String(d.label || `${fallback} ${i + 1}`);
        const opt = document.createElement('option');
        opt.value = d.deviceId;
        opt.textContent = fullLabel;
        opt.title = fullLabel;
        select.appendChild(opt);
      });
    };
    fillSelect(ui.camSelect, cams, 'Камера');
    fillSelect(ui.micSelect, mics, 'Микрофон');
    fillSelect(ui.speakerSelect, speakers, 'Динамики');
    fillSelect(ui.camSelectAdv, cams, 'Камера');
    fillSelect(ui.micSelectAdv, mics, 'Микрофон');
    fillSelect(ui.speakerSelectAdv, speakers, 'Динамики');

    if (state.videoDeviceId && cams.some(d => d.deviceId === state.videoDeviceId)) {
      ui.camSelect.value = state.videoDeviceId;
    }
    if (state.audioDeviceId && mics.some(d => d.deviceId === state.audioDeviceId)) {
      ui.micSelect.value = state.audioDeviceId;
    }
    if (state.speakerDeviceId && speakers.some(d => d.deviceId === state.speakerDeviceId)) {
      ui.speakerSelect.value = state.speakerDeviceId;
    }

    const vTrack = state.localStream?.getVideoTracks()[0];
    const aTrack = state.localStream?.getAudioTracks()[0];
    if (vTrack) ui.camSelect.value = vTrack.getSettings().deviceId || ui.camSelect.value;
    if (aTrack) ui.micSelect.value = aTrack.getSettings().deviceId || ui.micSelect.value;
    if (state.speakerDeviceId) ui.speakerSelect.value = state.speakerDeviceId;
    state.videoDeviceId = ui.camSelect.value || state.videoDeviceId;
    state.audioDeviceId = ui.micSelect.value || state.audioDeviceId;
    state.speakerDeviceId = ui.speakerSelect.value || state.speakerDeviceId;
    saveCallPrefs();
    ui.camSelectAdv.value = ui.camSelect.value;
    ui.micSelectAdv.value = ui.micSelect.value;
    ui.speakerSelectAdv.value = ui.speakerSelect.value;
  }

  function getCameraTrack() {
    return state.localStream?.getVideoTracks()?.[0] || null;
  }

  function getScreenTrack() {
    return state.screenStream?.getVideoTracks()?.[0] || null;
  }

  function getAudioTrack() {
    return state.localStream?.getAudioTracks()?.[0] || null;
  }

  function ensurePeerConnection(peer) {
    if (state.peerConnections.has(peer)) return state.peerConnections.get(peer);

    const pc = new RTCPeerConnection({
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' }
      ],
      iceCandidatePoolSize: 10,
      bundlePolicy: 'max-bundle',
      rtcpMuxPolicy: 'require'
    });

    const a = getAudioTrack();
    const cam = getCameraTrack();
    const scr = getScreenTrack();
    if (a) pc.addTrack(a, state.localStream);
    if (cam) pc.addTrack(cam, state.localStream);
    if (scr) pc.addTrack(scr, state.screenStream);

    pc.onicecandidate = (event) => {
      if (!event.candidate) return;
      socket.emit('call_signal', {
        call_id: state.activeCallId,
        from: username,
        target: peer,
        candidate: event.candidate
      });
    };

    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === 'failed') {
        if (shouldInitiateOffer(peer) && pc.signalingState === 'stable') {
          pc.restartIce?.();
          createOffer(peer).catch(() => {});
        }
      }
    };

    pc.ontrack = (event) => {
      let peerStream = state.remoteStreams.get(peer);
      if (!peerStream) {
        peerStream = new MediaStream();
        state.remoteStreams.set(peer, peerStream);
      }
      if (event.track && !peerStream.getTracks().some(t => t.id === event.track.id)) {
        peerStream.addTrack(event.track);
      }
      if (event.streams && event.streams[0]) {
        const incoming = event.streams[0];
        for (const t of incoming.getTracks()) {
          if (!peerStream.getTracks().some(x => x.id === t.id)) peerStream.addTrack(t);
        }
      }
      event.track.onunmute = () => renderVideos();
      renderVideos();
    };

    pc.onconnectionstatechange = () => {
      if (pc.connectionState === 'failed') {
        state.remoteStreams.delete(peer);
        state.peerConnections.delete(peer);
        renderVideos();
        if (state.participants.has(peer) && state.inCall) {
          setTimeout(async () => {
            if (!state.inCall || !state.participants.has(peer)) return;
            ensurePeerConnection(peer);
            if (shouldInitiateOffer(peer)) await createOffer(peer);
          }, 1500);
        }
      } else if (pc.connectionState === 'closed') {
        state.remoteStreams.delete(peer);
        state.peerConnections.delete(peer);
        renderVideos();
      }
    };

    state.peerConnections.set(peer, pc);
    return pc;
  }

  function shouldInitiateOffer(peer) {
    return username < peer;
  }

  async function createOffer(peer) {
    const pc = ensurePeerConnection(peer);
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    socket.emit('call_signal', {
      call_id: state.activeCallId,
      from: username,
      target: peer,
      sdp: pc.localDescription
    });
  }

  async function renegotiatePeer(peer) {
    const pc = state.peerConnections.get(peer);
    if (!pc) return;
    if (!['stable', 'have-local-offer'].includes(pc.signalingState)) return;
    if (pc.connectionState === 'closed') return;
    try {
      await createOffer(peer);
    } catch (e) {
      // Ignore renegotiation errors - connection may still work
    }
  }

  async function handleSignal(data) {
    const peer = (data.from || '').toLowerCase();
    if (!peer || peer === username || data.call_id !== state.activeCallId) return;

    const pc = ensurePeerConnection(peer);
    if (pc.connectionState === 'closed') return;

    if (data.sdp) {
      try {
        // Guard against glare (simultaneous offers)
        if (data.sdp.type === 'offer' && pc.signalingState === 'have-local-offer') {
          // Rollback our offer if remote has priority
          if (peer < username) {
            await pc.setLocalDescription({ type: 'rollback' });
          } else {
            return; // We have priority, ignore their offer
          }
        }
        await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
        if (data.sdp.type === 'offer') {
          const answer = await pc.createAnswer();
          await pc.setLocalDescription(answer);
          socket.emit('call_signal', {
            call_id: state.activeCallId,
            from: username,
            target: peer,
            sdp: pc.localDescription
          });
        }
      } catch (e) {
        // SDP error - try to recover by recreating connection
        if (state.participants.has(peer) && state.inCall) {
          state.peerConnections.get(peer)?.close();
          state.peerConnections.delete(peer);
          state.remoteStreams.delete(peer);
          setTimeout(async () => {
            if (!state.inCall || !state.participants.has(peer)) return;
            ensurePeerConnection(peer);
            if (shouldInitiateOffer(peer)) await createOffer(peer);
          }, 500);
        }
      }
      return;
    }

    if (data.candidate) {
      try {
        if (pc.remoteDescription) {
          await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
        }
      } catch {
        // ignore stale candidate
      }
    }
  }

  async function toggleMic() {
    const track = state.localStream?.getAudioTracks()[0];
    if (!track) return;
    track.enabled = !track.enabled;
    ui.micBtn.classList.toggle('off', !track.enabled);
    if (track.enabled && !state.headphonesEnabled) {
      state.headphonesEnabled = true;
      ui.hpBtn.classList.remove('off');
      syncRemoteAudioElements();
    }
    emitMediaState();
  }

  function toggleHeadphones() {
    state.headphonesEnabled = !state.headphonesEnabled;
    ui.hpBtn.classList.toggle('off', !state.headphonesEnabled);
    if (!state.headphonesEnabled) {
      const track = state.localStream?.getAudioTracks()[0];
      if (track && track.enabled) {
        state.micWasEnabledBeforeHeadphonesOff = true;
        track.enabled = false;
        ui.micBtn.classList.add('off');
        emitMediaState();
      }
    } else {
      const track = state.localStream?.getAudioTracks()[0];
      if (track && state.micWasEnabledBeforeHeadphonesOff) {
        track.enabled = true;
        ui.micBtn.classList.remove('off');
        emitMediaState();
      }
      state.micWasEnabledBeforeHeadphonesOff = false;
    }
    syncRemoteAudioElements();
  }

  async function toggleCam() {
    const track = state.rawCameraTrack || state.localStream?.getVideoTracks?.()[0];
    if (!track) {
      try {
        const cam = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
        const t = cam.getVideoTracks()[0];
        if (!state.localStream) state.localStream = new MediaStream();
        state.rawCameraTrack = t;
        state.localStream.addTrack(t);
        await applyBackgroundMode();
      } catch {
        notify('Камера недоступна');
        return;
      }
    } else {
      track.enabled = !track.enabled;
      const current = state.localStream?.getVideoTracks?.()[0];
      if (current && current !== track) current.enabled = track.enabled;
      ui.camBtn.classList.toggle('off', !track.enabled);
    }
    renderVideos();
    refreshTestCamPreview().catch(() => {});
    emitMediaState();
  }

  async function switchCamera() {
    const id = ui.camSelect.value;
    if (!id) return;
    state.videoDeviceId = id;
    saveCallPrefs();
    try {
      const s = await navigator.mediaDevices.getUserMedia({ video: { deviceId: { exact: id } }, audio: false });
      const newTrack = s.getVideoTracks()[0];
      const old = state.rawCameraTrack || state.localStream?.getVideoTracks()?.[0];
      if (old) {
        state.localStream.removeTrack(old);
        old.stop();
      }
      stopBackgroundProcessor();
      if (!state.localStream) state.localStream = new MediaStream();
      state.rawCameraTrack = newTrack;
      state.localStream.addTrack(newTrack);
      await applyVideoQuality();
      await applyBackgroundMode();
      if (state.backgroundPending) {
        await applyBackgroundMode();
      }
      ui.camSelectAdv.value = ui.camSelect.value;
      renderVideos();
      await refreshTestCamPreview();
      emitMediaState();
    } catch {
      notify('Не удалось переключить камеру');
    }
  }

  async function switchMicrophone() {
    const id = ui.micSelect.value;
    if (!id) return;
    state.audioDeviceId = id;
    saveCallPrefs();
    if (state.noiseSuppEnabled) {
      await toggleNoiseSuppression(true);
      ui.micSelectAdv.value = ui.micSelect.value;
      return;
    }
    try {
      const s = await navigator.mediaDevices.getUserMedia({
        audio: {
          deviceId: { exact: id },
          noiseSuppression: false,
          echoCancellation: true,
          autoGainControl: false,
          channelCount: 1
        },
        video: false
      });
      const newTrack = s.getAudioTracks()[0];
      const old = state.localStream?.getAudioTracks()?.[0];
      if (old) {
        state.localStream.removeTrack(old);
        old.stop();
      }
      if (!state.localStream) state.localStream = new MediaStream();
      state.localStream.addTrack(newTrack);
      for (const pc of state.peerConnections.values()) {
        const sender = pc.getSenders().find(x => x.track && x.track.kind === 'audio');
        if (sender) {
          await sender.replaceTrack(newTrack);
        } else {
          pc.addTrack(newTrack, state.localStream);
        }
      }
      emitMediaState();
      ui.micSelectAdv.value = ui.micSelect.value;
      renderVideos();
    } catch {
      notify('Не удалось переключить микрофон');
    }
  }

  async function switchSpeaker() {
    state.speakerDeviceId = ui.speakerSelect.value || '';
    saveCallPrefs();
    ui.speakerSelectAdv.value = ui.speakerSelect.value;
    const applySink = async (el) => {
      if (!el || !state.speakerDeviceId) return;
      if (typeof el.setSinkId !== 'function') return;
      try {
        await el.setSinkId(state.speakerDeviceId);
      } catch {
        notify('Выбор динамиков не поддерживается в этом браузере');
      }
    };
    ui.audioRack.querySelectorAll('audio').forEach((el) => {
      applySink(el).catch?.(() => {});
    });
    if (ui.testSpeakerAudio) {
      applySink(ui.testSpeakerAudio).catch?.(() => {});
    }
  }

  function setNoiseSuppression(enabled) {
    const checked = !!enabled;
    const actual = checked;    // was: !checked (inverted — fixed)
    state.noiseSuppEnabled = actual;
    if (ui.noiseSuppression) ui.noiseSuppression.checked = checked;
    if (ui.noiseSuppAdv) ui.noiseSuppAdv.checked = checked;
    saveCallPrefs();
    toggleNoiseSuppression(actual).catch(() => {});
  }

  async function toggleNoiseSuppression(forcedEnabled) {
    const enabled = typeof forcedEnabled === 'boolean' ? forcedEnabled : !!ui.noiseSuppression.checked;
    if (state.noiseSuppBusy) return;
    state.noiseSuppBusy = true;
    state.noiseSuppEnabled = enabled;
    const micId = ui.micSelect.value;

    // Stop existing noise processor
    if (state.noiseProcessor) {
      try { state.noiseProcessor.disconnect(); } catch {}
      try { state.noiseProcessor.port?.close(); } catch {}
      state.noiseProcessor = null;
    }
    if (state.noiseGateRaf) {
      cancelAnimationFrame(state.noiseGateRaf);
      state.noiseGateRaf = null;
    }
    if (state.noiseGateAnalyser) {
      try { state.noiseGateAnalyser.disconnect(); } catch {}
      state.noiseGateAnalyser = null;
    }
    if (state.noiseGateGain) {
      try { state.noiseGateGain.disconnect(); } catch {}
      state.noiseGateGain = null;
    }
    state.noiseGateOpen = false;
    if (state.noiseAudioCtx) {
      try { state.noiseAudioCtx.close(); } catch {}
      state.noiseAudioCtx = null;
    }
    if (state.noiseMicSource) {
      try { state.noiseMicSource.disconnect(); } catch {}
      state.noiseMicSource = null;
    }

    if (!enabled) {
      // Re-acquire mic without noise constraints to get clean track back
      try {
        const s = await navigator.mediaDevices.getUserMedia({
          audio: {
            deviceId: micId ? { exact: micId } : undefined,
            noiseSuppression: false,
            echoCancellation: true,
            autoGainControl: false,
            channelCount: 1
          },
          video: false
        });
        const newTrack = s.getAudioTracks()[0];
        const old = state.localStream?.getAudioTracks()?.[0];
        if (old) { state.localStream.removeTrack(old); old.stop(); }
        if (!state.localStream) state.localStream = new MediaStream();
        state.localStream.addTrack(newTrack);
        for (const pc of state.peerConnections.values()) {
          const sender = pc.getSenders().find(x => x.track?.kind === 'audio');
          if (sender) await sender.replaceTrack(newTrack);
        }
        emitMediaState();
        notify('Шумоизоляция выключена');
        state.noiseSuppBusy = false;
        return;
      } catch {
        notify('Не удалось изменить настройки микрофона');
        const checked = enabled ? false : true;
        if (ui.noiseSuppression) ui.noiseSuppression.checked = checked;
        if (ui.noiseSuppAdv) ui.noiseSuppAdv.checked = checked;
        state.noiseSuppEnabled = !enabled;
        state.noiseSuppBusy = false;
        return;
      }
    }

    // Enable: browser NS + stricter WebAudio chain (gate + filters)
    try {
      const s = await navigator.mediaDevices.getUserMedia({
        audio: {
          deviceId: micId ? { exact: micId } : undefined,
          noiseSuppression: true,
          echoCancellation: true,
          autoGainControl: false,
          channelCount: 1
        },
        video: false
      });
      const rawTrack = s.getAudioTracks()[0];

      const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      state.noiseAudioCtx = audioCtx;

      const source = audioCtx.createMediaStreamSource(new MediaStream([rawTrack]));
      state.noiseMicSource = source;

      // Filters + compressor for speech cleanup
      const highPass = audioCtx.createBiquadFilter();
      highPass.type = 'highpass';
      highPass.frequency.value = 110;
      highPass.Q.value = 0.7;

      const notch50 = audioCtx.createBiquadFilter();
      notch50.type = 'notch';
      notch50.frequency.value = 50;
      notch50.Q.value = 8;

      const notch60 = audioCtx.createBiquadFilter();
      notch60.type = 'notch';
      notch60.frequency.value = 60;
      notch60.Q.value = 8;

      const lowPass = audioCtx.createBiquadFilter();
      lowPass.type = 'lowpass';
      lowPass.frequency.value = 4200;
      lowPass.Q.value = 0.7;

      const compressor = audioCtx.createDynamicsCompressor();
      compressor.threshold.value = -24;
      compressor.knee.value = 14;
      compressor.ratio.value = 10;
      compressor.attack.value = 0.003;
      compressor.release.value = 0.24;

      const presence = audioCtx.createBiquadFilter();
      presence.type = 'peaking';
      presence.frequency.value = 1900;
      presence.Q.value = 1.1;
      presence.gain.value = 2.5;

      const gateGain = audioCtx.createGain();
      gateGain.gain.value = 0.9;
      state.noiseGateGain = gateGain;

      const analyser = audioCtx.createAnalyser();
      analyser.fftSize = 1024;
      analyser.smoothingTimeConstant = 0.85;
      state.noiseGateAnalyser = analyser;

      const dest = audioCtx.createMediaStreamDestination();
      source.connect(highPass);
      highPass.connect(notch50);
      notch50.connect(notch60);
      notch60.connect(lowPass);
      lowPass.connect(compressor);
      compressor.connect(presence);
      presence.connect(analyser);
      analyser.connect(gateGain);
      gateGain.connect(dest);

      state.noiseProcessor = compressor;
      const processedTrack = dest.stream.getAudioTracks()[0];

      const old = state.localStream?.getAudioTracks()?.[0];
      if (old) { state.localStream.removeTrack(old); old.stop(); }
      if (!state.localStream) state.localStream = new MediaStream();
      state.localStream.addTrack(processedTrack);

      for (const pc of state.peerConnections.values()) {
        const sender = pc.getSenders().find(x => x.track?.kind === 'audio');
        if (sender) await sender.replaceTrack(processedTrack);
      }
      emitMediaState();

      const data = new Uint8Array(analyser.fftSize);
      let noiseFloor = 0.0075;
      const calEndAt = performance.now() + 900;
      let gateHoldUntil = 0;
      const gateClosedGain = 0.015;

      const gateTick = () => {
        if (!state.noiseGateAnalyser || !state.noiseGateGain || !state.noiseAudioCtx) return;
        analyser.getByteTimeDomainData(data);
        let sum = 0;
        for (let i = 0; i < data.length; i++) {
          const v = (data[i] - 128) / 128;
          sum += v * v;
        }
        const rms = Math.sqrt(sum / data.length);
        const now = performance.now();
        if (now < calEndAt || !state.noiseGateOpen) {
          noiseFloor = (noiseFloor * 0.92) + (rms * 0.08);
        }
        const openThreshold = Math.max(0.019, noiseFloor * 2.7);
        const closeThreshold = Math.max(0.013, noiseFloor * 2.05);

        if (rms > openThreshold) {
          state.noiseGateOpen = true;
          gateHoldUntil = now + 95;
        } else if (now > gateHoldUntil && rms < closeThreshold) {
          state.noiseGateOpen = false;
        }

        const target = state.noiseGateOpen ? 0.9 : gateClosedGain;
        gateGain.gain.setTargetAtTime(target, audioCtx.currentTime, state.noiseGateOpen ? 0.012 : 0.13);
        state.noiseGateRaf = requestAnimationFrame(gateTick);
      };
      gateTick();

      notify('Шумоизоляция включена');
    } catch {
      notify('Не удалось применить шумоизоляцию');
      const checked = enabled ? false : true;
      if (ui.noiseSuppression) ui.noiseSuppression.checked = checked;
      if (ui.noiseSuppAdv) ui.noiseSuppAdv.checked = checked;
      state.noiseSuppEnabled = !enabled;
    }
    state.noiseSuppBusy = false;
  }

  function toggleAdvancedSettings() {
    const willOpen = ui.advancedSettings.classList.contains('hidden');
    if (willOpen) {
      ui.advancedSettings.classList.remove('hidden');
      ui.advancedSettings.classList.add('advanced-overlay-root');
      ui.panel.appendChild(ui.advancedSettings);
      // Sync advanced UI state
      if (ui.noiseSuppAdv) ui.noiseSuppAdv.checked = ui.noiseSuppression?.checked ?? false;
      if (ui.mirrorCamAdv) ui.mirrorCamAdv.checked = state.mirrorCamera !== false;
      if (ui.videoQualityAdv) ui.videoQualityAdv.value = state.videoQuality || 'high';
      if (ui.backgroundSelect) ui.backgroundSelect.value = state.backgroundMode || 'none';
      updateBgPresetsUI();
      if (ui.micVolumeSlider) {
        ui.micVolumeSlider.value = Math.round((state.micVolume ?? 1) * 100);
        if (ui.micVolumeVal) ui.micVolumeVal.textContent = `${ui.micVolumeSlider.value}%`;
      }
      if (ui.speakerVolumeSlider) {
        ui.speakerVolumeSlider.value = Math.round((state.speakerVolume ?? 1) * 100);
        if (ui.speakerVolumeVal) ui.speakerVolumeVal.textContent = `${ui.speakerVolumeSlider.value}%`;
      }
      startMicLevelMeter().catch(() => {});
    } else {
      stopMicTest();
      resetMicRecording();
      stopMicLevelMeter();
      ui.advancedSettings.classList.add('hidden');
      ui.advancedSettings.classList.remove('advanced-overlay-root');
      ui.advancedHost.appendChild(ui.advancedSettings);
    }
    ui.advancedToggle.textContent = willOpen ? 'Скрыть расширенные настройки' : 'Открыть расширенные настройки';
    ui.panel.classList.toggle('advanced-open', willOpen);
    if (isMobileViewport()) {
      ui.panel.classList.toggle('mobile-overlay-open', willOpen || !ui.chatPanel.classList.contains('hidden') || !ui.settingsBox.classList.contains('hidden'));
    }
    if (willOpen) setAdvancedTab('audio');
    enforcePanelBounds();
  }

  function setAdvancedTab(tab) {
    const map = {
      audio: ui.advPaneAudio,
      sound: ui.advPaneSound,
      video: ui.advPaneVideo
    };
    Object.entries(map).forEach(([k, el]) => el.classList.toggle('hidden', k !== tab));
    ui.advTabAudio.classList.toggle('active', tab === 'audio');
    ui.advTabSound.classList.toggle('active', tab === 'sound');
    ui.advTabVideo.classList.toggle('active', tab === 'video');
    if (tab === 'video') {
      refreshTestCamPreview().catch(() => {});
    }
  }

  function applyMicVolume() {
    if (!state.noiseAudioCtx) return;
    // If noise processor active, gain is already on source - handled via compressor
    // For direct track we'd need a GainNode - implemented here
  }

  function applySpeakerVolume() {
    const vol = Math.max(0, Math.min(2, state.speakerVolume));
    ui.audioRack?.querySelectorAll('audio').forEach(el => {
      const peer = String(el.dataset.peer || '');
      const stream = el.srcObject;
      if (isMobileViewport() && stream && state.headphonesEnabled) {
        const Ctx = window.AudioContext || window.webkitAudioContext;
        if (Ctx) {
          if (!state.audioBoostCtx || state.audioBoostCtx.state === 'closed') {
            state.audioBoostCtx = new Ctx();
          }
          let node = state.audioBoostNodes.get(peer);
          if (!node || node.stream !== stream) {
            if (node) {
              try { node.source.disconnect(); } catch {}
              try { node.gain.disconnect(); } catch {}
              state.audioBoostNodes.delete(peer);
            }
            try {
              const source = state.audioBoostCtx.createMediaStreamSource(stream);
              const gain = state.audioBoostCtx.createGain();
              source.connect(gain);
              gain.connect(state.audioBoostCtx.destination);
              node = { stream, source, gain };
              state.audioBoostNodes.set(peer, node);
            } catch {
              node = null;
            }
          }
          if (node?.gain) {
            try { state.audioBoostCtx.resume?.(); } catch {}
            node.gain.gain.value = Math.max(0.6, Math.min(2.8, vol * 2.1));
            el.volume = 0;
            el.muted = false;
            return;
          }
        }
      } else {
        const n = state.audioBoostNodes.get(peer);
        if (n) {
          try { n.source.disconnect(); } catch {}
          try { n.gain.disconnect(); } catch {}
          state.audioBoostNodes.delete(peer);
        }
      }
      el.volume = Math.min(1, vol);
    });
    if (ui.testSpeakerAudio) {
      ui.testSpeakerAudio.volume = Math.min(1, vol);
    }
    updateCallVolumeButtonState();
  }

  async function applyVideoQuality() {
    const track = state.rawCameraTrack || state.localStream?.getVideoTracks()?.[0];
    if (!track) return;
    const presets = {
      high:   { width: 1280, height: 720, frameRate: 30 },
      medium: { width: 854,  height: 480, frameRate: 24 },
      low:    { width: 640,  height: 360, frameRate: 15 }
    };
    const preset = presets[state.videoQuality] || presets.high;
    try {
      await track.applyConstraints({
        width: { ideal: preset.width },
        height: { ideal: preset.height },
        frameRate: { ideal: preset.frameRate }
      });
      if (state.backgroundMode && state.backgroundMode !== 'none') {
        await applyBackgroundMode();
      }
      await refreshTestCamPreview();
    } catch {}
  }

  async function applyMicConstraints() {
    const micId = ui.micSelect?.value || '';
    try {
      const s = await navigator.mediaDevices.getUserMedia({
        audio: {
          deviceId: micId ? { exact: micId } : undefined,
          echoCancellation: true,
          noiseSuppression: ui.noiseSuppression?.checked ?? false,
          autoGainControl: false,
          channelCount: 1
        },
        video: false
      });
      const newTrack = s.getAudioTracks()[0];
      const old = state.localStream?.getAudioTracks()?.[0];
      if (old) { state.localStream.removeTrack(old); old.stop(); }
      if (!state.localStream) state.localStream = new MediaStream();
      state.localStream.addTrack(newTrack);
      for (const pc of state.peerConnections.values()) {
        const sender = pc.getSenders().find(x => x.track?.kind === 'audio');
        if (sender) await sender.replaceTrack(newTrack);
      }
    } catch {}
  }

  // Background preset gradient generation (no external images needed)
  const BG_PRESETS = {
    'preset-office': {
      draw: (ctx, w, h) => {
        const grad = ctx.createLinearGradient(0, 0, w, h);
        grad.addColorStop(0, '#1a1a2e');
        grad.addColorStop(0.4, '#16213e');
        grad.addColorStop(1, '#0f3460');
        ctx.fillStyle = grad;
        ctx.fillRect(0, 0, w, h);
        // Window blinds effect
        for (let y = 0; y < h; y += 24) {
          ctx.fillStyle = 'rgba(255,255,255,0.03)';
          ctx.fillRect(0, y, w, 12);
        }
        // Desk silhouette
        ctx.fillStyle = '#0a0a1a';
        ctx.fillRect(0, h * 0.75, w, h * 0.25);
        ctx.fillStyle = 'rgba(60,100,200,0.15)';
        ctx.fillRect(0, h * 0.72, w, 4);
      }
    },
    'preset-nature': {
      draw: (ctx, w, h) => {
        // Sky
        const sky = ctx.createLinearGradient(0, 0, 0, h * 0.6);
        sky.addColorStop(0, '#87ceeb');
        sky.addColorStop(1, '#c8e6f5');
        ctx.fillStyle = sky;
        ctx.fillRect(0, 0, w, h * 0.6);
        // Ground
        const ground = ctx.createLinearGradient(0, h * 0.6, 0, h);
        ground.addColorStop(0, '#4a7c59');
        ground.addColorStop(1, '#2d5a3d');
        ctx.fillStyle = ground;
        ctx.fillRect(0, h * 0.6, w, h * 0.4);
        // Sun
        ctx.fillStyle = 'rgba(255,220,0,0.9)';
        ctx.beginPath();
        ctx.arc(w * 0.8, h * 0.15, h * 0.08, 0, Math.PI * 2);
        ctx.fill();
        // Trees
        const drawTree = (x, th) => {
          ctx.fillStyle = '#1a3a1a';
          ctx.beginPath();
          ctx.moveTo(x, h * 0.6);
          ctx.lineTo(x - th * 0.4, h * 0.6);
          ctx.lineTo(x, h * 0.6 - th);
          ctx.lineTo(x + th * 0.4, h * 0.6);
          ctx.closePath();
          ctx.fill();
        };
        [0.1, 0.2, 0.85, 0.92].forEach(xr => drawTree(w * xr, h * 0.3));
      }
    },
    'preset-city': {
      draw: (ctx, w, h) => {
        const grad = ctx.createLinearGradient(0, 0, 0, h);
        grad.addColorStop(0, '#0a0a1a');
        grad.addColorStop(0.5, '#1a1a3e');
        grad.addColorStop(1, '#2a1a3e');
        ctx.fillStyle = grad;
        ctx.fillRect(0, 0, w, h);
        // City skyline
        const buildings = [[0.05,0.6],[0.12,0.45],[0.2,0.55],[0.3,0.4],[0.38,0.5],[0.48,0.35],[0.55,0.5],[0.65,0.4],[0.75,0.55],[0.85,0.45],[0.92,0.6]];
        buildings.forEach(([x, topRatio], i) => {
          const bw = w * 0.08;
          const by = h * topRatio;
          ctx.fillStyle = `hsl(${220 + i * 5}, 30%, ${12 + i % 3 * 4}%)`;
          ctx.fillRect(w * x, by, bw, h - by);
          // Windows
          for (let wy = by + 6; wy < h - 6; wy += 10) {
            for (let wx = w * x + 4; wx < w * x + bw - 4; wx += 8) {
              if (Math.random() > 0.4) {
                ctx.fillStyle = `rgba(255,220,100,${0.3 + Math.random() * 0.5})`;
                ctx.fillRect(wx, wy, 4, 5);
              }
            }
          }
        });
      }
    },
    'preset-space': {
      draw: (ctx, w, h) => {
        ctx.fillStyle = '#000005';
        ctx.fillRect(0, 0, w, h);
        // Stars
        for (let i = 0; i < 200; i++) {
          const x = Math.random() * w;
          const y = Math.random() * h;
          const r = Math.random() * 1.5;
          ctx.fillStyle = `rgba(255,255,255,${0.4 + Math.random() * 0.6})`;
          ctx.beginPath();
          ctx.arc(x, y, r, 0, Math.PI * 2);
          ctx.fill();
        }
        // Nebula
        const neb = ctx.createRadialGradient(w*0.3, h*0.4, 0, w*0.3, h*0.4, w*0.4);
        neb.addColorStop(0, 'rgba(100,0,200,0.15)');
        neb.addColorStop(0.5, 'rgba(0,100,200,0.08)');
        neb.addColorStop(1, 'transparent');
        ctx.fillStyle = neb;
        ctx.fillRect(0, 0, w, h);
        // Planet
        const planet = ctx.createRadialGradient(w*0.75, h*0.25, 0, w*0.75, h*0.25, h*0.15);
        planet.addColorStop(0, '#4a6fa5');
        planet.addColorStop(0.7, '#2a3f6f');
        planet.addColorStop(1, '#0a1a3a');
        ctx.fillStyle = planet;
        ctx.beginPath();
        ctx.arc(w*0.75, h*0.25, h*0.15, 0, Math.PI * 2);
        ctx.fill();
      }
    },
    'preset-studio': {
      draw: (ctx, w, h) => {
        // Dark backdrop
        ctx.fillStyle = '#0d0d0d';
        ctx.fillRect(0, 0, w, h);
        // Subtle gradient spotlight
        const spot = ctx.createRadialGradient(w/2, 0, 0, w/2, h/2, w*0.7);
        spot.addColorStop(0, 'rgba(60,80,120,0.35)');
        spot.addColorStop(0.6, 'rgba(20,30,60,0.15)');
        spot.addColorStop(1, 'transparent');
        ctx.fillStyle = spot;
        ctx.fillRect(0, 0, w, h);
        // Floor line
        ctx.fillStyle = 'rgba(80,100,140,0.3)';
        ctx.fillRect(0, h * 0.75, w, 1);
        // Floor reflection
        const floor = ctx.createLinearGradient(0, h*0.75, 0, h);
        floor.addColorStop(0, 'rgba(60,80,120,0.2)');
        floor.addColorStop(1, 'rgba(0,0,0,0)');
        ctx.fillStyle = floor;
        ctx.fillRect(0, h*0.75, w, h*0.25);
      }
    }
  };

  function generatePresetBackground(presetName, w = 640, h = 360) {
    const preset = BG_PRESETS[presetName];
    if (!preset) return '';
    const canvas = document.createElement('canvas');
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext('2d');
    preset.draw(ctx, w, h);
    return canvas.toDataURL('image/jpeg', 0.85);
  }

  const BG_CARD_ITEMS = [
    { mode: 'none', title: 'Без фона' },
    { mode: 'blur', title: 'Размытие' },
    { mode: 'blur-strong', title: 'Сильное размытие' },
    { mode: 'dark', title: 'Затемнение' },
    { mode: 'preset-office', title: 'Офис' },
    { mode: 'preset-nature', title: 'Природа' },
    { mode: 'preset-city', title: 'Город' },
    { mode: 'preset-space', title: 'Космос' },
    { mode: 'preset-studio', title: 'Студия' },
    { mode: 'image', title: 'Своя' }
  ];

  function buildBgThumb(mode, w = 260, h = 146) {
    if (mode.startsWith('preset-')) {
      return generatePresetBackground(mode, w, h);
    }
    const canvas = document.createElement('canvas');
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext('2d');
    const g = ctx.createLinearGradient(0, 0, w, h);
    g.addColorStop(0, '#0f172a');
    g.addColorStop(1, '#1e293b');
    ctx.fillStyle = g;
    ctx.fillRect(0, 0, w, h);

    // Stylized "person" silhouette to make preview close to real usage.
    ctx.fillStyle = '#94a3b8';
    ctx.beginPath();
    ctx.arc(w * 0.5, h * 0.38, h * 0.14, 0, Math.PI * 2);
    ctx.fill();
    ctx.beginPath();
    ctx.ellipse(w * 0.5, h * 0.73, w * 0.18, h * 0.2, 0, 0, Math.PI * 2);
    ctx.fill();

    if (mode === 'none') {
      return canvas.toDataURL('image/jpeg', 0.9);
    }
    if (mode === 'dark') {
      ctx.fillStyle = 'rgba(0,0,0,0.55)';
      ctx.fillRect(0, 0, w, h);
      return canvas.toDataURL('image/jpeg', 0.9);
    }

    const blurCanvas = document.createElement('canvas');
    blurCanvas.width = w;
    blurCanvas.height = h;
    const blurCtx = blurCanvas.getContext('2d');
    blurCtx.filter = mode === 'blur-strong' ? 'blur(22px)' : 'blur(12px)';
    blurCtx.drawImage(canvas, 0, 0, w, h);
    blurCtx.filter = 'none';

    ctx.clearRect(0, 0, w, h);
    ctx.drawImage(blurCanvas, 0, 0, w, h);
    ctx.fillStyle = '#e2e8f0';
    ctx.beginPath();
    ctx.arc(w * 0.5, h * 0.38, h * 0.14, 0, Math.PI * 2);
    ctx.fill();
    ctx.beginPath();
    ctx.ellipse(w * 0.5, h * 0.73, w * 0.18, h * 0.2, 0, 0, Math.PI * 2);
    ctx.fill();
    return canvas.toDataURL('image/jpeg', 0.9);
  }

  function getBgThumbCached(mode) {
    if (mode === 'image') {
      return state.backgroundImage || '';
    }
    if (!state.bgThumbCache[mode]) {
      state.bgThumbCache[mode] = buildBgThumb(mode);
    }
    return state.bgThumbCache[mode];
  }

  function renderBackgroundCards() {
    if (!ui.bgPresetsRow) return;
    ui.bgPresetsRow.innerHTML = '';
    BG_CARD_ITEMS.forEach((item) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'call-bg-preset-item';
      btn.dataset.mode = item.mode;
      if (item.mode === state.backgroundMode) {
        btn.classList.add('active');
      }

      const thumb = document.createElement('div');
      thumb.className = 'call-bg-thumb';
      if (item.mode === 'image' && !state.backgroundImage) {
        thumb.classList.add('call-bg-thumb-upload');
        thumb.textContent = '+';
      } else {
        const src = getBgThumbCached(item.mode);
        if (src) thumb.style.backgroundImage = `url("${src}")`;
      }

      const label = document.createElement('span');
      label.className = 'call-bg-preset-label';
      label.textContent = item.title;

      btn.append(thumb, label);
      btn.addEventListener('click', () => {
        if (item.mode === 'image' && !state.backgroundImage) {
          ui.bgImageInput?.click();
          return;
        }
        if (ui.backgroundSelect) ui.backgroundSelect.value = item.mode;
        onBackgroundModeChanged();
      });
      ui.bgPresetsRow.appendChild(btn);
    });
  }

  function updateBgPresetsUI() {
    if (!ui.bgPresetsRow) return;
    const mode = state.backgroundMode;
    const isImage = mode === 'image';
    ui.bgPresetsRow?.classList.remove('hidden');
    ui.bgImageWrap?.classList.toggle('hidden', !isImage);
    renderBackgroundCards();
    // Update bg preview
    if (ui.bgPreviewCanvas) {
      const preset = BG_PRESETS[mode] || null;
      if (preset || mode === 'image') {
        ui.bgPreviewCanvas.classList.remove('hidden');
        const ctx = ui.bgPreviewCanvas.getContext('2d');
        ctx.clearRect(0, 0, 160, 90);
        if (preset) {
          preset.draw(ctx, 160, 90);
        } else if (state.backgroundImage) {
          const img = new Image();
          img.onload = () => {
            ctx.drawImage(img, 0, 0, 160, 90);
          };
          img.src = state.backgroundImage;
        }
      } else {
        ui.bgPreviewCanvas.classList.add('hidden');
      }
    }
  }

  async function startMicLevelMeter() {
    if (state.micLevelCtx) return;
    if (!state.localStream) return;
    try {
      const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      const analyser = audioCtx.createAnalyser();
      analyser.fftSize = 256;
      const source = audioCtx.createMediaStreamSource(state.localStream);
      source.connect(analyser);
      state.micLevelCtx = audioCtx;
      state.micLevelAnalyser = analyser;
      const buf = new Uint8Array(analyser.frequencyBinCount);
      const tick = () => {
        if (!state.micLevelCtx) return;
        analyser.getByteFrequencyData(buf);
        const avg = buf.reduce((a, b) => a + b, 0) / buf.length;
        const pct = Math.min(100, (avg / 128) * 100);
        const el = document.getElementById('callMicLevel');
        if (el) el.style.width = `${pct}%`;
        requestAnimationFrame(tick);
      };
      tick();
    } catch {}
  }

  function stopMicLevelMeter() {
    if (state.micLevelCtx) {
      try { state.micLevelCtx.close(); } catch {}
      state.micLevelCtx = null;
      state.micLevelAnalyser = null;
    }
  }

  function onBackgroundModeChanged() {
    state.backgroundMode = ui.backgroundSelect?.value || 'none';
    state.backgroundPending = true;
    saveCallPrefs();
    updateBgPresetsUI();
    if (state.backgroundMode.startsWith('preset-')) {
      state.backgroundImage = generatePresetBackground(state.backgroundMode);
    } else if (state.backgroundMode !== 'image') {
      // keep custom image if user had one
    }
    applyBackgroundMode().catch(() => {});
  }

  function onBgImageChosen() {
    const f = ui.bgImageInput.files?.[0];
    if (!f) return;
    const r = new FileReader();
    r.onload = () => {
      state.backgroundImage = String(r.result || '');
      state.bgThumbCache.image = state.backgroundImage;
      if (ui.backgroundSelect) ui.backgroundSelect.value = 'image';
      state.backgroundMode = 'image';
      state.backgroundPending = true;
      saveCallPrefs();
      updateBgPresetsUI();
      applyBackgroundMode().catch(() => {});
    };
    r.readAsDataURL(f);
  }

  async function applyBackgroundMode() {
    const raw = state.rawCameraTrack;
    if (!raw) return;
    if (state.backgroundMode === 'none') {
      stopBackgroundProcessor();
      await setOutgoingCameraTrack(raw, state.localStream);
      renderVideos();
      await refreshTestCamPreview();
      return;
    }
    if (state.backgroundMode === 'image' && !state.backgroundImage) {
      stopBackgroundProcessor();
      await setOutgoingCameraTrack(raw, state.localStream);
      renderVideos();
      await refreshTestCamPreview();
      return;
    }
    // Preset backgrounds use generated image
    if (state.backgroundMode.startsWith('preset-') && !state.backgroundImage) {
      state.backgroundImage = generatePresetBackground(state.backgroundMode);
    }
    const processed = await buildProcessedCameraTrack(raw);
    if (!processed) {
      await setOutgoingCameraTrack(raw, state.localStream);
      renderVideos();
      await refreshTestCamPreview();
      return;
    }
    if (!state.localStream) state.localStream = new MediaStream();
    const current = state.localStream.getVideoTracks?.()[0];
    if (current && current !== raw) {
      state.localStream.removeTrack(current);
      current.stop?.();
    }
    if (!state.localStream.getVideoTracks().includes(processed)) {
      state.localStream.addTrack(processed);
    }
    state.processedCameraTrack = processed;
    await setOutgoingCameraTrack(processed, state.localStream);
    renderVideos();
    await refreshTestCamPreview();
    state.backgroundPending = false;
  }

  async function setOutgoingCameraTrack(track, streamForAdd) {
    const current = state.localStream?.getVideoTracks?.()[0];
    if (current && current !== track) {
      state.localStream.removeTrack(current);
    }
    if (track && !state.localStream.getVideoTracks().includes(track)) {
      state.localStream.addTrack(track);
    }
    await replaceCameraTrack(track, streamForAdd);
  }

  function stopBackgroundProcessor() {
    const bp = state.bgProcessor;
    if (!bp) return;
    bp.active = false;
    try { bp.timer && cancelAnimationFrame(bp.timer); } catch {}
    try { bp.timer && clearTimeout(bp.timer); } catch {}
    try { bp.video.pause(); } catch {}
    try { bp.video.srcObject = null; } catch {}
    try { bp.canvasTrack?.stop?.(); } catch {}
    state.bgProcessor = null;
    state.processedCameraTrack = null;
  }

  async function ensureSelfieSegmentationLoaded() {
    if (window.SelfieSegmentation) return true;
    const loadScript = (src) => new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = src;
      s.async = true;
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    });
    try {
      await loadScript('https://cdn.jsdelivr.net/npm/@mediapipe/selfie_segmentation@0.1/selfie_segmentation.js');
      if (window.SelfieSegmentation) return true;
      await loadScript('https://unpkg.com/@mediapipe/selfie_segmentation/selfie_segmentation.js');
      return !!window.SelfieSegmentation;
    } catch {
      notify('Не удалось загрузить модуль фона');
      return false;
    }
  }

  async function buildProcessedCameraTrack(rawTrack) {
    const ok = await ensureSelfieSegmentationLoaded();
    if (!ok) return null;
    stopBackgroundProcessor();
    const settings = rawTrack.getSettings?.() || {};
    const w = Math.max(320, Number(settings.width || 640));
    const h = Math.max(240, Number(settings.height || 360));
    const fps = Math.max(15, Math.min(30, Number(settings.frameRate || 24)));
    const video = document.createElement('video');
    video.autoplay = true;
    video.playsInline = true;
    video.muted = true;
    video.srcObject = new MediaStream([rawTrack]);
    await video.play().catch(() => {});
    const canvas = document.createElement('canvas');
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext('2d');   // alpha:true required for source-in / destination-over

    const maskCanvas = document.createElement('canvas');
    maskCanvas.width = w; maskCanvas.height = h;
    const maskCtx = maskCanvas.getContext('2d');
    const maskPrev = document.createElement('canvas');
    maskPrev.width = w; maskPrev.height = h;
    const maskPrevCtx = maskPrev.getContext('2d');

    const selfie = new window.SelfieSegmentation({
      locateFile: (file) => `https://cdn.jsdelivr.net/npm/@mediapipe/selfie_segmentation/${file}`
    });
    // Keep model in non-selfie mode so mask orientation matches the drawn frame.
    selfie.setOptions({ modelSelection: 1, selfieMode: false });

    const proc = { active: true, video, canvas, ctx, selfie, timer: null, canvasTrack: null };
    state.bgProcessor = proc;

    const drawCover = (img, cw, ch) => {
      const iw = img.videoWidth || img.naturalWidth || cw;
      const ih = img.videoHeight || img.naturalHeight || ch;
      if (!iw || !ih) return;
      const ir = iw / ih, cr = cw / ch;
      let dw = cw, dh = ch, dx = 0, dy = 0;
      if (ir > cr) { dh = ch; dw = dh * ir; dx = (cw - dw) / 2; }
      else { dw = cw; dh = dw / ir; dy = (ch - dh) / 2; }
      ctx.drawImage(img, dx, dy, dw, dh);
    };

    const bgImg = new Image();
    bgImg.crossOrigin = 'anonymous';
    let bgImgSrc = '';
    const refreshBgImg = () => {
      const src = state.backgroundImage || '';
      if (src !== bgImgSrc) { bgImgSrc = src; bgImg.src = src; }
    };
    refreshBgImg();

    selfie.onResults((results) => {
      if (!proc.active) return;
      refreshBgImg();
      const { width: cw, height: ch } = canvas;

      // Tighter opaque body mask to avoid transparency/bleeding on torso and hands.
      maskCtx.save();
      maskCtx.clearRect(0, 0, cw, ch);
      maskCtx.filter = 'blur(1px) contrast(180%)';
      maskCtx.globalAlpha = 0.08;
      maskCtx.drawImage(maskPrev, 0, 0, cw, ch);
      maskCtx.globalAlpha = 0.96;
      maskCtx.drawImage(results.segmentationMask, 0, 0, cw, ch);
      maskCtx.globalAlpha = 1;
      maskCtx.filter = 'none';
      maskCtx.restore();
      maskPrevCtx.clearRect(0, 0, cw, ch);
      maskPrevCtx.drawImage(maskCanvas, 0, 0, cw, ch);

      ctx.save();
      ctx.clearRect(0, 0, cw, ch);

      // Step 1: draw person (cut by mask via source-in)
      ctx.drawImage(maskCanvas, 0, 0, cw, ch);
      ctx.globalCompositeOperation = 'source-in';
      drawCover(video, cw, ch);

      // Step 2: draw background behind person (destination-over paints UNDER existing pixels)
      ctx.globalCompositeOperation = 'destination-over';
      const mode = state.backgroundMode;
      if (mode === 'blur' || mode === 'blur-strong') {
        ctx.filter = mode === 'blur-strong' ? 'blur(34px)' : 'blur(20px)';
        drawCover(video, cw, ch);
        ctx.filter = 'none';
      } else if (mode === 'dark') {
        ctx.fillStyle = '#000';
        ctx.fillRect(0, 0, cw, ch);
      } else if ((mode === 'image' || mode.startsWith('preset-'))
                 && bgImg.complete && bgImg.naturalWidth > 0) {
        drawCover(bgImg, cw, ch);
      } else {
        ctx.filter = 'blur(20px)';
        drawCover(video, cw, ch);
        ctx.filter = 'none';
      }

      ctx.restore();
    });

    let segInFlight = false;
    let lastSentAt = 0;
    const minFrameMs = Math.max(20, Math.round(1000 / fps));
    const run = async () => {
      if (!proc.active) return;
      if (!segInFlight) {
        const now = performance.now();
        if ((now - lastSentAt) >= minFrameMs) {
          segInFlight = true;
          lastSentAt = now;
          await selfie.send({ image: video }).catch(() => {});
          segInFlight = false;
        }
      }
      proc.timer = requestAnimationFrame(run);
    };
    run();
    const out = canvas.captureStream(fps).getVideoTracks()[0];
    proc.canvasTrack = out;
    out.enabled = rawTrack.enabled;
    return out;
  }

  async function toggleMicTest() {
    const rec = state.micTest.recorder;
    if (rec && rec.state === 'recording') {
      rec.stop();
      ui.testMicBtn.textContent = 'Записать голос';
      return;
    }
    if (state.micTest.stream) {
      stopMicTest();
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          deviceId: ui.micSelect.value ? { exact: ui.micSelect.value } : undefined
        },
        video: false
      });
      state.micTest.stream = stream;
      state.micTest.chunks = [];
      const recorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
      recorder.ondataavailable = (e) => {
        if (e.data && e.data.size > 0) state.micTest.chunks.push(e.data);
      };
      recorder.onstop = () => {
        if (state.micTest.blobUrl) URL.revokeObjectURL(state.micTest.blobUrl);
        const blob = new Blob(state.micTest.chunks, { type: 'audio/webm' });
        state.micTest.blobUrl = URL.createObjectURL(blob);
        ui.micPlayback.src = state.micTest.blobUrl;
        ui.micPlayback.classList.remove('hidden');
        ui.micPlayBtn.classList.remove('hidden');
        ui.micResetBtn.classList.remove('hidden');
        stopMicTest();
      };
      state.micTest.recorder = recorder;
      recorder.start();
      ui.testMicBtn.textContent = 'Остановить запись';
    } catch {
      notify('Не удалось проверить микрофон');
    }
  }

  function stopMicTest() {
    if (state.micTest.stream) {
      state.micTest.stream.getTracks().forEach(t => t.stop());
      state.micTest.stream = null;
    }
    state.micTest.recorder = null;
    ui.testMicBtn.textContent = 'Записать голос';
  }

  function playMicRecording() {
    if (!state.micTest.blobUrl) return;
    ui.micPlayback.currentTime = 0;
    ui.micPlayback.play().catch(() => {});
  }

  function resetMicRecording() {
    if (state.micTest.blobUrl) URL.revokeObjectURL(state.micTest.blobUrl);
    state.micTest.blobUrl = '';
    state.micTest.chunks = [];
    ui.micPlayback.pause();
    ui.micPlayback.src = '';
    ui.micPlayback.classList.add('hidden');
    ui.micPlayBtn.classList.add('hidden');
    ui.micResetBtn.classList.add('hidden');
  }

  function testSpeaker() {
    if (!ui.testSpeakerAudio) return;
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return;
    if (!state.speakerTestCtx || state.speakerTestCtx.state === 'closed') {
      state.speakerTestCtx = new Ctx();
    }
    const ctx = state.speakerTestCtx;
    if (ctx.state === 'suspended') {
      ctx.resume().catch(() => {});
    }

    const dest = ctx.createMediaStreamDestination();
    const gain = ctx.createGain();
    gain.gain.value = 0.08;

    const osc = ctx.createOscillator();
    osc.type = 'sine';
    osc.connect(gain);
    gain.connect(dest);

    ui.testSpeakerAudio.srcObject = dest.stream;
    ui.testSpeakerAudio.volume = Math.min(1, Math.max(0, state.speakerVolume ?? 1));
    ui.testSpeakerAudio.play().catch(() => {});
    if (state.speakerDeviceId && typeof ui.testSpeakerAudio.setSinkId === 'function') {
      ui.testSpeakerAudio.setSinkId(state.speakerDeviceId).catch(() => {});
    }

    const melody = [392, 440, 494, 523, 587, 659, 698, 784];
    const startAt = ctx.currentTime + 0.02;
    melody.forEach((f, i) => {
      osc.frequency.setValueAtTime(f, startAt + i * 0.2);
    });
    osc.start(startAt);
    osc.stop(startAt + melody.length * 0.2 + 0.1);

    osc.onended = () => {
      try { gain.disconnect(); } catch {}
      try { osc.disconnect(); } catch {}
    };
  }

  function stopCamTestPreviewStream() {
    if (state.camTestPreviewStream) {
      try { state.camTestPreviewStream.getTracks().forEach(t => t.stop()); } catch {}
      state.camTestPreviewStream = null;
    }
  }

  async function refreshTestCamPreview() {
    if (!ui.testCamVideo || ui.testCamVideo.classList.contains('hidden')) return;
    const outgoingTrack = state.localStream?.getVideoTracks?.()[0] || state.rawCameraTrack || null;
    if (outgoingTrack) {
      if (state.camTestStream) {
        try { state.camTestStream.getTracks().forEach(t => t.stop()); } catch {}
        state.camTestStream = null;
      }
      stopCamTestPreviewStream();
      const clone = outgoingTrack.clone();
      clone.enabled = outgoingTrack.enabled;
      const preview = new MediaStream([clone]);
      state.camTestPreviewStream = preview;
      ui.testCamVideo.srcObject = preview;
      ui.testCamVideo.classList.toggle('local-camera-mirror', state.mirrorCamera !== false);
      ui.testCamVideo.onloadedmetadata = () => {
        const vw = Number(ui.testCamVideo.videoWidth || 0);
        const vh = Number(ui.testCamVideo.videoHeight || 0);
        if (vw > 0 && vh > 0) {
          ui.testCamVideo.style.aspectRatio = `${vw} / ${vh}`;
        }
      };
      return;
    }

    if (state.camTestStream) {
      ui.testCamVideo.srcObject = state.camTestStream;
      ui.testCamVideo.classList.toggle('local-camera-mirror', state.mirrorCamera !== false);
    }
  }

  async function toggleCamTest() {
    const isShown = !ui.testCamVideo.classList.contains('hidden');
    if (isShown) {
      if (state.camTestStream) {
        state.camTestStream.getTracks().forEach(t => t.stop());
        state.camTestStream = null;
      }
      stopCamTestPreviewStream();
      ui.testCamVideo.srcObject = null;
      ui.testCamVideo.classList.add('hidden');
      ui.testCamBtn.textContent = 'Проверить камеру';
      return;
    }
    ui.testCamVideo.classList.remove('hidden');
    ui.testCamBtn.textContent = 'Остановить камеру';
    await refreshTestCamPreview();
    if (ui.testCamVideo.srcObject) return;

    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        video: {
          deviceId: ui.camSelect.value ? { exact: ui.camSelect.value } : undefined
        },
        audio: false
      });
      state.camTestStream = stream;
      ui.testCamVideo.srcObject = stream;
      ui.testCamVideo.classList.toggle('local-camera-mirror', state.mirrorCamera !== false);
      ui.testCamVideo.onloadedmetadata = () => {
        const vw = Number(ui.testCamVideo.videoWidth || 0);
        const vh = Number(ui.testCamVideo.videoHeight || 0);
        if (vw > 0 && vh > 0) {
          ui.testCamVideo.style.aspectRatio = `${vw} / ${vh}`;
        }
      };
      ui.testCamVideo.classList.remove('hidden');
      ui.testCamBtn.textContent = 'Остановить камеру';
    } catch {
      ui.testCamVideo.classList.add('hidden');
      ui.testCamBtn.textContent = 'Проверить камеру';
      notify('Не удалось проверить камеру');
    }
  }

  function getScreenSender(pc, screenTrackId = '') {
    const id = screenTrackId || getScreenTrack()?.id || '';
    if (!id) return null;
    return pc.getSenders().find(s => s.track && s.track.kind === 'video' && s.track.id === id) || null;
  }

  function getCameraSender(pc, screenTrackId = '') {
    const sid = screenTrackId || getScreenTrack()?.id || '';
    return pc.getSenders().find(s => s.track && s.track.kind === 'video' && (!sid || s.track.id !== sid)) || null;
  }

  async function replaceCameraTrack(track, streamForAdd) {
    const peersToRenegotiate = [];
    const sid = getScreenTrack()?.id || '';
    for (const [peer, pc] of state.peerConnections.entries()) {
      const sender = getCameraSender(pc, sid);
      if (sender && track) {
        await sender.replaceTrack(track);
      } else if (sender && !track) {
        pc.removeTrack(sender);
        peersToRenegotiate.push(peer);
      } else if (!sender && track && streamForAdd) {
        pc.addTrack(track, streamForAdd);
        peersToRenegotiate.push(peer);
      }
    }
    for (const peer of peersToRenegotiate) {
      try { await renegotiatePeer(peer); } catch {}
    }
  }

  async function addScreenTrackToPeers(screenTrack, streamForAdd) {
    const peersToRenegotiate = [];
    for (const [peer, pc] of state.peerConnections.entries()) {
      const exists = getScreenSender(pc, screenTrack.id);
      if (exists) continue;
      pc.addTrack(screenTrack, streamForAdd);
      peersToRenegotiate.push(peer);
    }
    for (const peer of peersToRenegotiate) {
      try { await renegotiatePeer(peer); } catch {}
    }
  }

  async function removeScreenTrackFromPeers(screenTrackId) {
    const peersToRenegotiate = [];
    for (const [peer, pc] of state.peerConnections.entries()) {
      const sender = getScreenSender(pc, screenTrackId);
      if (!sender) continue;
      pc.removeTrack(sender);
      peersToRenegotiate.push(peer);
    }
    for (const peer of peersToRenegotiate) {
      try { await renegotiatePeer(peer); } catch {}
    }
  }

  async function toggleScreenShare() {
    if (state.screenStream) {
      stopScreenShare(true);
      return;
    }

    try {
      const stream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
      state.screenStream = stream;
      const screenTrack = stream.getVideoTracks()[0];
      screenTrack.onended = () => stopScreenShare(true);
      await addScreenTrackToPeers(screenTrack, stream);

      setScreenOwner(username);
      ui.screenBtn.classList.add('active');
      renderVideos();
      socket.emit('call_screen_share', {
        call_id: state.activeCallId,
        from: username,
        sharing: true,
        screen_track_id: screenTrack.id
      });
      updateControlButtons();
    } catch {
      notify('Не удалось начать демонстрацию');
    }
  }

  async function stopScreenShare(emit = false) {
    if (!state.screenStream) return;
    const oldTrack = state.screenStream.getVideoTracks()[0];
    const oldTrackId = oldTrack?.id || '';
    for (const t of state.screenStream.getTracks()) t.stop();
    state.screenStream = null;
    ui.screenBtn.classList.remove('active');
    if (oldTrackId) await removeScreenTrackFromPeers(oldTrackId);

    if (state.screenOwner === username) setScreenOwner(null);
    if (emit) {
      socket.emit('call_screen_share', {
        call_id: state.activeCallId,
        from: username,
        sharing: false,
        screen_track_id: oldTrackId
      });
    }

    updateControlButtons();
    renderVideos();
  }

  function renderVideos() {
    ui.videos.innerHTML = '';
    const sources = buildVideoSources();
    state.lastSources = sources;
    if (sources.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'call-videos-empty';
      empty.textContent = 'Видео не включено';
      ui.videos.append(empty);
      syncRemoteAudioElements();
      resizeCanvas();
      renderParticipantAvatars();
      updateStageVisibility();
      return;
    }

    const mainSource = pickMainSource(sources);
    state.currentMainSourceId = mainSource.id;
    const thumbs = sources.filter(s => s.id !== mainSource.id);

    const main = renderMainVideo(mainSource);
    ui.videos.append(main);

    if (thumbs.length) {
      const overlay = document.createElement('div');
      overlay.className = 'call-thumbs-overlay';
      const plan = getThumbPlan(mainSource, sources);
      plan.forEach(({ source, slot }) => overlay.append(renderThumb(source, slot)));
      ui.videos.append(overlay);
    }

    resizeCanvas();
    syncRemoteAudioElements();
    renderParticipantAvatars();
    updateStageVisibility();
  }

  function hasLiveVideo(stream) {
    if (!stream) return false;
    const tracks = stream.getVideoTracks();
    return tracks.some(t => t.readyState === 'live' && t.enabled !== false);
  }

  function peerCamEnabled(peer) {
    const ms = state.mediaState.get(peer);
    if (!ms) return true;
    return ms.cam !== false;
  }

  function buildVideoSources() {
    const sources = [];

    if (state.screenStream && hasLiveVideo(state.screenStream)) {
      sources.push({
        id: 'screen_local',
        type: 'screen',
        isLocal: true,
        owner: username,
        stream: state.screenStream
      });
    }

    if (state.localStream && hasLiveVideo(state.localStream)) {
      sources.push({
        id: 'cam_local',
        type: 'camera',
        isLocal: true,
        owner: username,
        stream: state.localStream
      });
    }

    for (const [peer, stream] of state.remoteStreams.entries()) {
      const tracks = (stream?.getVideoTracks?.() || []).filter(t => t.readyState === 'live' && !t.muted);
      if (!tracks.length) continue;
      const wantsScreen = !!state.remoteScreenTrackIds[peer];
      let screenTrack = null;
      let camTrack = null;
      if (wantsScreen && tracks.length > 1) {
        const ranked = [...tracks].sort((a, b) => {
          const sa = a.getSettings?.() || {};
          const sb = b.getSettings?.() || {};
          const aa = (sa.width || 1) * (sa.height || 1);
          const ab = (sb.width || 1) * (sb.height || 1);
          return ab - aa;
        });
        screenTrack = ranked[0];
        camTrack = ranked[1];
      }
      for (const t of tracks) {
        const isScreen = wantsScreen ? (t === screenTrack) : false;
        const type = isScreen ? 'screen' : 'camera';
        if (type === 'camera' && !peerCamEnabled(peer)) continue;
        const oneTrackStream = new MediaStream([t]);
        sources.push({
          id: `${type}_${peer}_${t.id}`,
          type,
          isLocal: false,
          owner: peer,
          stream: oneTrackStream,
          trackId: t.id
        });
      }
    }

    return sources;
  }

  function mainPriority(src) {
    if (src.type === 'screen' && !src.isLocal) return 0;
    if (src.type === 'screen' && src.isLocal) return 1;
    if (src.type === 'camera' && !src.isLocal) return 2;
    return 3;
  }

  function thumbPriority(src) {
    if (src.type === 'camera') return 0;
    if (src.type === 'screen' && src.isLocal) return 1;
    return 2;
  }

  function pickLayoutSources(sources) {
    const ownCam = sources.find(s => s.type === 'camera' && s.isLocal) || null;
    const ownScreen = sources.find(s => s.type === 'screen' && s.isLocal) || null;
    const remoteScreen = sources.find(s => s.type === 'screen' && !s.isLocal) || null;
    const remoteCam = sources.find(s => s.type === 'camera' && !s.isLocal) || null;
    return { ownCam, ownScreen, remoteScreen, remoteCam };
  }

  function pickMainSource(sources) {
    const ls = pickLayoutSources(sources);
    const screenSrc = ls.remoteScreen || ls.ownScreen;
    if (state.manualPinnedMain && state.pinnedMainSourceId) {
      const pinnedManual = sources.find(s => s.id === state.pinnedMainSourceId);
      if (pinnedManual) return pinnedManual;
      state.manualPinnedMain = false;
      state.pinnedMainSourceId = null;
    }
    if (state.draw.active && screenSrc && !state.manualPinnedMain) {
      state.pinnedMainSourceId = screenSrc.id;
    }
    if (state.pinnedMainSourceId) {
      const pinned = sources.find(s => s.id === state.pinnedMainSourceId);
      if (pinned) return pinned;
      state.pinnedMainSourceId = null;
    }
    if (ls.remoteScreen) return ls.remoteScreen;
    if (ls.remoteCam) return ls.remoteCam;
    if (ls.ownScreen) return ls.ownScreen;
    if (ls.ownCam) return ls.ownCam;
    return [...sources].sort((a, b) => mainPriority(a) - mainPriority(b))[0];
  }

  function renderMainVideo(src) {
    const wrap = document.createElement('div');
    wrap.className = 'call-main-video';
    const video = document.createElement('video');
    video.autoplay = true;
    video.playsInline = true;
    video.muted = !!src.isLocal;
    decorateLocalCameraVideo(video, src);
    video.srcObject = src.stream;
    video.addEventListener('loadedmetadata', () => {
      const vw = Number(video.videoWidth || 0);
      const vh = Number(video.videoHeight || 0);
      if (vw > 0 && vh > 0) {
        state.mainAspect = Math.max(0.5, Math.min(3, vw / vh));
      } else {
        const track = src.stream?.getVideoTracks?.()[0];
        const s = track?.getSettings?.() || {};
        const r = Number(s.aspectRatio || 0);
        if (r > 0) state.mainAspect = Math.max(0.5, Math.min(3, r));
      }
    }, { once: true });
    const label = document.createElement('div');
    label.className = 'call-main-label';
    label.textContent = src.type === 'screen'
      ? `${src.isLocal ? 'Ваша демонстрация' : `Демонстрация @${src.owner}`}`
      : `${src.isLocal ? 'Вы' : '@' + src.owner}`;

    const fsBtn = document.createElement('button');
    fsBtn.className = 'call-main-fs';
    fsBtn.title = 'На весь экран';
    fsBtn.textContent = '⛶';
    fsBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      togglePanelFullscreen();
    });

    wrap.append(video, label, fsBtn);
    wrap.addEventListener('click', (e) => {
      if (e.target.closest('.call-main-fs')) return;
      cycleMainSource();
    });
    return wrap;
  }

  function renderThumb(src, slotName) {
    const wrap = document.createElement('button');
    wrap.className = `call-thumb call-thumb-${slotName}`;
    wrap.title = 'Сделать главным экраном';
    const video = document.createElement('video');
    video.autoplay = true;
    video.playsInline = true;
    video.muted = true;
    decorateLocalCameraVideo(video, src);
    video.srcObject = src.stream;
    const label = document.createElement('div');
    label.className = 'call-thumb-label';
    label.textContent = src.type === 'screen'
      ? (src.isLocal ? 'Ваша демка' : `Демка @${src.owner}`)
      : (src.isLocal ? 'Вы' : '@' + src.owner);
    wrap.append(video, label);
    wrap.addEventListener('click', () => {
      state.pinnedMainSourceId = src.id;
      state.manualPinnedMain = true;
      renderVideos();
    });
    return wrap;
  }

  function cycleMainSource() {
    const sources = buildVideoSources();
    if (sources.length < 2) return;
    const idx = Math.max(0, sources.findIndex(s => s.id === state.currentMainSourceId));
    const next = sources[(idx + 1) % sources.length];
    state.pinnedMainSourceId = next.id;
    state.manualPinnedMain = true;
    renderVideos();
  }

  function togglePanelFullscreen() {
    if (document.fullscreenElement === ui.panel) {
      document.exitFullscreen?.().catch(() => {});
      return;
    }
    ui.panel.requestFullscreen?.().catch(() => {});
  }

  function onFullscreenChanged() {
    const fs = document.fullscreenElement === ui.panel;
    ui.panel.classList.toggle('fs-mode', fs);
    if (fs) {
      ui.chatPanel.classList.add('call-side-in-fullscreen');
      ui.settingsBox.classList.add('call-side-in-fullscreen');
    } else {
      ui.chatPanel.classList.remove('call-side-in-fullscreen');
      ui.settingsBox.classList.remove('call-side-in-fullscreen');
      // Restore default side panel docking after fullscreen
      if (ui.chatPanel) ui.chatPanel.style.left = '';
      if (ui.settingsBox) ui.settingsBox.style.left = '';
      [ui.drawTools, document.getElementById('callControls')].forEach((el) => {
        if (!el) return;
        el.classList.remove('floating-free');
        el.style.left = '';
        el.style.top = '';
        el.style.right = '';
        el.style.bottom = '';
      });
    }
    enforcePanelBounds();
    resizeCanvas();
    renderVideos();
  }

  function decorateLocalCameraVideo(video, src) {
    if (!(src.isLocal && src.type === 'camera' && src.owner === username)) return;
    video.classList.toggle('local-camera-mirror', state.mirrorCamera !== false);
  }

  function applyLocalMirrorState() {
    if (ui.testCamVideo) {
      ui.testCamVideo.classList.toggle('local-camera-mirror', state.mirrorCamera !== false);
    }
    refreshTestCamPreview().catch(() => {});
    renderVideos();
  }

  function getThumbPlan(mainSource, sources) {
    const plan = [];
    const add = (pred, slot) => {
      const src = sources.find(s => s.id !== mainSource.id && pred(s) && !plan.some(p => p.source.id === s.id));
      if (src) plan.push({ source: src, slot });
    };

    add(s => s.type === 'camera' && !s.isLocal, 'left-top');
    add(s => s.type === 'camera' && s.isLocal, 'right-top');
    add(s => s.type === 'screen' && s.isLocal, 'right-bottom');
    add(s => s.type === 'screen' && !s.isLocal, 'left-bottom');

    const rest = sources.filter(s => s.id !== mainSource.id && !plan.some(p => p.source.id === s.id));
    const fallbackSlots = ['left-top', 'right-top', 'left-bottom', 'right-bottom'];
    rest.forEach((src, i) => plan.push({ source: src, slot: fallbackSlots[i % fallbackSlots.length] }));
    return plan;
  }

  function updateStageVisibility() {
    const visible = buildVideoSources().length > 0;
    const stage = document.getElementById('callStage');
    if (stage) stage.classList.toggle('hidden', !visible);
    updatePanelLimits();
  }

  function updatePanelLimits() {
    const hasVisual = buildVideoSources().length > 0;
    const minW = hasVisual ? 520 : 360;
    const minH = hasVisual ? 420 : 250;
    const maxW = Math.max(minW, window.innerWidth - 16);
    const maxH = Math.max(minH, window.innerHeight - 16);
    ui.panel.style.minWidth = `${minW}px`;
    ui.panel.style.minHeight = `${minH}px`;
    ui.panel.style.maxWidth = `${maxW}px`;
    ui.panel.style.maxHeight = `${maxH}px`;
    enforcePanelBounds();
  }

  function autoFitPanelToMain() {
    if (ui.panel.classList.contains('hidden')) return;
    const hasVisual = buildVideoSources().length > 0;
    if (!hasVisual) return;

    const headerH = ui.panel.querySelector('.call-panel-header')?.offsetHeight || 0;
    const avatarsH = ui.panel.querySelector('#callAvatars')?.offsetHeight || 0;
    const statusH = ui.panel.querySelector('#callStatusText')?.offsetHeight || 0;
    const settingsH = ui.settingsBox?.classList.contains('hidden') ? 0 : (ui.settingsBox.offsetHeight || 0);
    const controlsH = ui.panel.querySelector('#callControls')?.offsetHeight || 0;
    const chromeH = headerH + avatarsH + statusH + settingsH + controlsH + 28;

    const minW = parseInt(ui.panel.style.minWidth || '520', 10);
    const minH = parseInt(ui.panel.style.minHeight || '420', 10);
    const maxW = Math.max(minW, window.innerWidth - 16);
    const maxH = Math.max(minH, window.innerHeight - 16);
    const aspect = Math.max(0.5, Math.min(3, Number(state.mainAspect || (16 / 9))));

    let targetW = Math.min(maxW, Math.max(minW, (maxH - chromeH) * aspect));
    let targetH = chromeH + targetW / aspect;
    if (targetH > maxH) {
      targetH = maxH;
      targetW = (targetH - chromeH) * aspect;
    }

    targetW = Math.max(minW, Math.min(maxW, targetW));
    targetH = Math.max(minH, Math.min(maxH, targetH));

    ui.panel.style.width = `${Math.round(targetW)}px`;
    ui.panel.style.height = `${Math.round(targetH)}px`;
    enforcePanelBounds();
  }

  function enforcePanelBounds() {
    if (isMobileViewport()) {
      ui.stage.style.paddingRight = '';
      ui.stage.style.paddingLeft = '';
      ui.panel.style.left = '0';
      ui.panel.style.top = '0';
      ui.panel.style.right = '0';
      ui.panel.style.bottom = '0';
      ui.panel.style.width = '100vw';
      ui.panel.style.height = '100dvh';
      if (ui.chatPanel) {
        ui.chatPanel.style.width = '100vw';
        ui.chatPanel.style.left = '0';
        ui.chatPanel.style.right = '0';
      }
      if (ui.settingsBox) {
        ui.settingsBox.style.width = '100vw';
        ui.settingsBox.style.left = '0';
        ui.settingsBox.style.right = '0';
      }
      return;
    }
    const isFs = document.fullscreenElement === ui.panel;
    const rect = ui.panel.getBoundingClientRect();
    const sideGap = 10;
    const sideMin = 150;
    if (ui.chatPanel) {
      const leftFree = Math.max(sideMin, Math.floor(rect.left - 8 - sideGap));
      const leftW = Math.min(300, leftFree);
      ui.chatPanel.style.width = `${leftW}px`;
    }
    if (ui.settingsBox) {
      const rightFree = Math.max(sideMin, Math.floor(window.innerWidth - rect.right - 8 - sideGap));
      const rightW = Math.min(300, rightFree);
      ui.settingsBox.style.width = `${rightW}px`;
    }
    if (isFs) {
      const chatOpen = !ui.chatPanel.classList.contains('hidden');
      const settingsOpen = !ui.settingsBox.classList.contains('hidden');
      const wide = Math.min(300, Math.max(220, Math.floor(window.innerWidth * 0.22)));
      if (chatOpen) ui.chatPanel.style.width = `${wide}px`;
      if (settingsOpen) ui.settingsBox.style.width = `${wide}px`;

      const leftDock = chatOpen ? wide + 20 : 0;
      const rightDock = settingsOpen ? wide + 20 : 0;

      if (chatOpen) {
        ui.chatPanel.style.left = '10px';
        ui.chatPanel.style.right = 'auto';
      } else {
        ui.chatPanel.style.left = '';
      }
      if (settingsOpen) {
        ui.settingsBox.style.right = '10px';
        ui.settingsBox.style.left = 'auto';
      } else {
        ui.settingsBox.style.right = '';
      }

      ui.stage.style.paddingLeft = leftDock ? `${leftDock}px` : '';
      ui.stage.style.paddingRight = rightDock ? `${rightDock}px` : '';
      return;
    }
    ui.stage.style.paddingRight = '';
    ui.stage.style.paddingLeft = '';
    ui.chatPanel.style.right = '';
    ui.settingsBox.style.right = '';

    let left = rect.left;
    let top = rect.top;
    const leftExtra = !ui.chatPanel.classList.contains('hidden') ? (ui.chatPanel.getBoundingClientRect().width + sideGap) : 0;
    const rightExtra = !ui.settingsBox.classList.contains('hidden') ? (ui.settingsBox.getBoundingClientRect().width + sideGap) : 0;
    const minLeft = 8 + leftExtra;
    const maxLeft = Math.max(minLeft, window.innerWidth - rect.width - 8 - rightExtra);
    const maxTop = Math.max(8, window.innerHeight - rect.height - 8);
    if (left < minLeft) left = minLeft;
    if (top < 8) top = 8;
    if (left > maxLeft) left = maxLeft;
    if (top > maxTop) top = maxTop;
    ui.panel.style.left = `${Math.round(left)}px`;
    ui.panel.style.top = `${Math.round(top)}px`;
  }

  function syncRemoteAudioElements() {
    const existing = new Map();
    ui.audioRack.querySelectorAll('audio[data-peer]').forEach(el => {
      existing.set(el.dataset.peer, el);
    });

    for (const [peer, stream] of state.remoteStreams.entries()) {
      let el = existing.get(peer);
      if (!el) {
        el = document.createElement('audio');
        el.dataset.peer = peer;
        el.autoplay = true;
        el.playsInline = true;
        ui.audioRack.appendChild(el);
      }
      if (el.srcObject !== stream) {
        el.srcObject = stream;
        // Force play - handles autoplay policy
        const tryPlay = () => {
          const p = el.play?.();
          if (p && typeof p.catch === 'function') p.catch(() => {
            // On first user interaction, retry
            const retry = () => { el.play?.().catch(() => {}); document.removeEventListener('click', retry); };
            document.addEventListener('click', retry, { once: true });
          });
        };
        tryPlay();
      }
      el.muted = !state.headphonesEnabled;
      el.volume = Math.min(1, Math.max(0, state.speakerVolume ?? 1));
      if (state.speakerDeviceId && typeof el.setSinkId === 'function') {
        el.setSinkId(state.speakerDeviceId).catch(() => {});
      }
      applySpeakerVolume();
      existing.delete(peer);
    }

    for (const el of existing.values()) {
      const p = String(el.dataset.peer || '');
      const n = state.audioBoostNodes.get(p);
      if (n) {
        try { n.source.disconnect(); } catch {}
        try { n.gain.disconnect(); } catch {}
        state.audioBoostNodes.delete(p);
      }
      el.srcObject = null;
      el.remove();
    }
  }

  function emitMediaState() {
    const mic = !!state.localStream?.getAudioTracks()?.[0]?.enabled;
    const cam = !!state.localStream?.getVideoTracks()?.[0]?.enabled;
    socket.emit('call_media_state', {
      call_id: state.activeCallId,
      from: username,
      mic,
      cam
    });
  }

  function updateControlButtons() {
    const canRequest = state.screenOwner && state.screenOwner !== username && !state.allowDrawAll && !state.controlGranted;
    ui.controlBtn.classList.toggle('hidden', !canRequest);
    const canDraw = !!(state.screenOwner && (state.screenOwner === username || state.allowDrawAll || state.controlGranted));
    ui.drawBtn.classList.toggle('hidden', !state.screenOwner);
    ui.drawBtn.classList.toggle('disabled', !canDraw);
    ui.drawBtn.title = canDraw ? 'Рисование на демонстрации' : 'Нет прав рисования';
    if (!canDraw) {
      state.draw.active = false;
      ui.drawBtn.classList.remove('active');
      ui.canvas.classList.remove('draw-active');
      ui.drawTools.classList.add('hidden');
    }
    if (ui.drawPermSelect) {
      const canChangePerm = !!(state.screenOwner && (state.screenOwner === username || state.controlGranted));
      ui.drawPermSelect.disabled = !canChangePerm;
      ui.drawPermSelect.value = state.allowDrawAll ? 'on' : 'off';
    }
  }

  function onDrawPermChanged() {
    const canChangePerm = !!(state.screenOwner && (state.screenOwner === username || state.controlGranted));
    if (!canChangePerm) return;
    const allow = ui.drawPermSelect.value === 'on';
    state.allowDrawAll = allow;
    socket.emit('call_annotation_perm', {
      call_id: state.activeCallId,
      from: username,
      allow_all: allow
    });
    updateControlButtons();
  }

  function sendCallChat() {
    const text = (ui.chatInput.value || '').trim();
    if (!text || !state.activeCallId) return;
    ui.chatInput.value = '';
    socket.emit('call_chat', {
      call_id: state.activeCallId,
      from: username,
      message: text
    });
  }

  function appendCallChat(from, text) {
    const nick = String(from || 'user').trim().toLowerCase();
    const msg = String(text || '');
    const row = document.createElement('div');
    row.className = `call-chat-row ${nick === username ? 'mine' : ''}`.trim();
    const nickEl = document.createElement('span');
    nickEl.className = 'call-chat-nick';
    nickEl.style.color = getNickColor(nick);
    nickEl.textContent = `@${nick}`;
    const msgEl = document.createElement('span');
    msgEl.className = 'call-chat-msg';
    msgEl.textContent = `: ${msg}`;
    row.append(nickEl, msgEl);
    ui.chatList.append(row);
    ui.chatList.scrollTop = ui.chatList.scrollHeight;
  }

  function getNickColor(name) {
    const palette = ['#60a5fa', '#34d399', '#f59e0b', '#f472b6', '#a78bfa', '#22d3ee', '#fb7185'];
    let h = 0;
    const s = String(name || '');
    for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    return palette[Math.abs(h) % palette.length];
  }

  function normalizeOwnerKey(owner) {
    const k = String(owner || '').trim().toLowerCase();
    return k || null;
  }

  function cloneStroke(st) {
    return {
      id: String(st?.id || ''),
      from: String(st?.from || ''),
      color: String(st?.color || '#ff4d4f'),
      size: Number(st?.size || 3),
      points: Array.isArray(st?.points) ? st.points.map((p) => ({ x: Number(p.x || 0), y: Number(p.y || 0) })) : []
    };
  }

  function saveCurrentAnnotationsToOwner() {
    const owner = normalizeOwnerKey(state.annotationOwner);
    if (!owner) return;
    state.annotationByOwner[owner] = state.drawStrokes.map(cloneStroke);
  }

  function loadAnnotationsForOwner(owner) {
    const key = normalizeOwnerKey(owner);
    state.annotationOwner = key;
    const src = key ? (state.annotationByOwner[key] || []) : [];
    state.drawStrokes = src.map(cloneStroke);
    state.strokeMap.clear();
    state.drawStrokes.forEach((s) => state.strokeMap.set(s.id, s));
    redrawAnnotations();
  }

  function setScreenOwner(nextOwner) {
    const next = normalizeOwnerKey(nextOwner);
    const prev = normalizeOwnerKey(state.screenOwner);
    if (prev === next) return;
    saveCurrentAnnotationsToOwner();
    state.screenOwner = next;
    loadAnnotationsForOwner(next);
  }

  function requestControl() {
    if (!state.screenOwner || state.screenOwner === username) return;
    socket.emit('call_control_request', {
      call_id: state.activeCallId,
      from: username,
      owner: state.screenOwner
    });
    notify('Запрос на управление отправлен');
  }

  async function endCall(emitEnd) {
    if (!state.inCall) return;
    const callId = state.activeCallId;
    if (callId) {
      socket.emit('call_leave', { call_id: callId, username });
    }

    cleanupCall();
  }

  function cleanupCall() {
    state.inCall = false;
    state.activeCallId = null;
    state.activeChatId = null;
    state.callMode = 'audio';
    state.participants = new Set();
    setScreenOwner(null);
    state.headphonesEnabled = true;
    state.micWasEnabledBeforeHeadphonesOff = false;
    state.allowDrawAll = false;
    state.controlGranted = false;
    state.dialTargets = [];
    state.pinnedMainSourceId = null;
    state.manualPinnedMain = false;
    state.mediaState.clear();
    state.remoteScreenTrackIds = {};
    if (state.audioBoostCtx) {
      try { state.audioBoostCtx.close(); } catch {}
      state.audioBoostCtx = null;
    }
    state.audioBoostNodes.clear();
    state.drawStrokes = [];
    state.strokeMap.clear();
    state.annotationByOwner = {};
    state.annotationOwner = null;
    stopBackgroundProcessor();
    stopMicTest();
    resetMicRecording();
    // Cleanup noise processor
    if (state.noiseProcessor) {
      try { state.noiseProcessor.disconnect(); } catch {}
      state.noiseProcessor = null;
    }
    if (state.noiseGateRaf) {
      cancelAnimationFrame(state.noiseGateRaf);
      state.noiseGateRaf = null;
    }
    if (state.noiseGateAnalyser) {
      try { state.noiseGateAnalyser.disconnect(); } catch {}
      state.noiseGateAnalyser = null;
    }
    if (state.noiseGateGain) {
      try { state.noiseGateGain.disconnect(); } catch {}
      state.noiseGateGain = null;
    }
    state.noiseGateOpen = false;
    if (state.noiseAudioCtx) {
      try { state.noiseAudioCtx.close(); } catch {}
      state.noiseAudioCtx = null;
    }
    if (state.noiseMicSource) {
      try { state.noiseMicSource.disconnect(); } catch {}
      state.noiseMicSource = null;
    }
    if (ui.noiseSuppression) ui.noiseSuppression.checked = false;
    if (ui.noiseSuppAdv) ui.noiseSuppAdv.checked = false;
    state.noiseSuppEnabled = false;
    stopMicLevelMeter();
    if (state.camTestStream) {
      state.camTestStream.getTracks().forEach(t => t.stop());
      state.camTestStream = null;
    }
    stopCamTestPreviewStream();

    if (state.screenStream) {
      for (const t of state.screenStream.getTracks()) t.stop();
      state.screenStream = null;
    }
    if (state.localStream) {
      for (const t of state.localStream.getTracks()) t.stop();
      state.localStream = null;
    }
    if (state.rawCameraTrack) {
      try { state.rawCameraTrack.stop(); } catch {}
      state.rawCameraTrack = null;
    }

    for (const pc of state.peerConnections.values()) {
      pc.close();
    }
    state.peerConnections.clear();
    state.remoteStreams.clear();

    ui.settingsBox.classList.add('hidden');
    ui.chatPanel.classList.add('hidden');
    ui.mobileMoreMenu?.classList.add('hidden');
    ui.panel.classList.remove('mobile-overlay-open');
    ui.drawTools.classList.add('hidden');
    ui.avatars.innerHTML = '';
    ui.audioRack.innerHTML = '';
    ui.chatList.innerHTML = '';
    ui.drawBtn.classList.remove('active');
    ui.drawBtn.classList.add('hidden');
    ui.hpBtn.classList.remove('off');
    ui.advancedSettings.classList.add('hidden');
    ui.advancedSettings.classList.remove('advanced-overlay-root');
    ui.advancedHost.appendChild(ui.advancedSettings);
    ui.advancedToggle.textContent = 'Открыть расширенные настройки';
    ui.testCamVideo.srcObject = null;
    ui.testCamVideo.classList.add('hidden');
    ui.testCamBtn.textContent = 'Проверить камеру';
    setDrawTool('pen');
    ui.screenBtn.classList.remove('active');

    clearCanvas();
    stopRinging();
    hideIncoming();
    hidePanel();
    updateHeaderActionsVisibility();
    if (state.lastCallChatId && typeof window.openChat === 'function') {
      window.openChat(state.lastCallChatId).catch?.(() => {});
    }
    if (typeof window.syncMyContacts === 'function') {
      window.syncMyContacts().catch?.(() => {});
    }
    localStorage.removeItem('active_call');
  }

  function resizeCanvas() {
    const rect = ui.videos.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    ui.canvas.width = Math.max(1, Math.floor(rect.width * dpr));
    ui.canvas.height = Math.max(1, Math.floor(rect.height * dpr));
    const ctx = ui.canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    redrawAnnotations();
  }

  function clearCanvas() {
    const ctx = ui.canvas.getContext('2d');
    const rect = ui.canvas.getBoundingClientRect();
    ctx.clearRect(0, 0, rect.width, rect.height);
  }

  function setupDrawing() {
    const canvas = ui.canvas;
    const getContentRect = (w, h, aspect) => {
      const a = Math.max(0.3, Math.min(4, Number(aspect || state.mainAspect || (16 / 9))));
      const cwByH = h * a;
      let cw = w;
      let ch = h;
      let ox = 0;
      let oy = 0;
      if (cwByH <= w) {
        cw = cwByH;
        ox = (w - cw) / 2;
      } else {
        ch = w / a;
        oy = (h - ch) / 2;
      }
      return { x: ox, y: oy, w: cw, h: ch };
    };

    const pos = (event) => {
      const r = canvas.getBoundingClientRect();
      const e = event.touches?.[0] || event;
      return {
        x: Math.max(0, Math.min(r.width, e.clientX - r.left)),
        y: Math.max(0, Math.min(r.height, e.clientY - r.top)),
        w: r.width,
        h: r.height
      };
    };

    const canDraw = () => !!(state.screenOwner && (state.screenOwner === username || state.allowDrawAll || state.controlGranted));

    const down = (e) => {
      if (!state.draw.active || !canDraw()) return;
      e.preventDefault();
      const p = pos(e);
      state.draw.lastX = p.x;
      state.draw.lastY = p.y;
      canvas.dataset.drag = '1';
      if (state.draw.tool === 'pen') {
        const stroke = {
          id: `st_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          from: username,
          color: state.draw.color,
          size: state.draw.size,
          points: [{ x: p.x, y: p.y }]
        };
        state.draw.currentStrokeId = stroke.id;
        state.drawStrokes.push(stroke);
        state.strokeMap.set(stroke.id, stroke);
      } else {
        eraseAtPoint(p.x, p.y, true);
      }
    };

    const move = (e) => {
      if (canvas.dataset.drag !== '1' || !canDraw()) return;
      e.preventDefault();
      const p = pos(e);
      if (state.draw.tool === 'erase') {
        eraseAtPoint(p.x, p.y, true);
        state.draw.lastX = p.x;
        state.draw.lastY = p.y;
        return;
      }

      const content = getContentRect(p.w, p.h, state.mainAspect);
      const nx1 = (state.draw.lastX - content.x) / content.w;
      const ny1 = (state.draw.lastY - content.y) / content.h;
      const nx2 = (p.x - content.x) / content.w;
      const ny2 = (p.y - content.y) / content.h;

      if (nx1 < 0 || nx1 > 1 || ny1 < 0 || ny1 > 1 || nx2 < 0 || nx2 > 1 || ny2 < 0 || ny2 > 1) {
        state.draw.lastX = p.x;
        state.draw.lastY = p.y;
        return;
      }
      const stroke = state.strokeMap.get(state.draw.currentStrokeId);
      if (stroke) {
        stroke.points.push({ x: p.x, y: p.y });
      }
      redrawAnnotations();

      socket.emit('call_annotation', {
        call_id: state.activeCallId,
        from: username,
        kind: 'stroke',
        screen_owner: state.screenOwner || '',
        stroke_id: state.draw.currentStrokeId,
        points: [
          nx1, ny1, nx2, ny2
        ],
        color: state.draw.color,
        size: state.draw.size,
        aspect: state.mainAspect
      });

      state.draw.lastX = p.x;
      state.draw.lastY = p.y;
    };

    const up = () => {
      canvas.dataset.drag = '0';
      state.draw.currentStrokeId = null;
    };

    canvas.addEventListener('mousedown', down);
    canvas.addEventListener('mousemove', move);
    window.addEventListener('mouseup', up);
    canvas.addEventListener('touchstart', down, { passive: false });
    canvas.addEventListener('touchmove', move, { passive: false });
    window.addEventListener('touchend', up);

    window.addEventListener('resize', resizeCanvas);
  }

  function eraseAtPoint(x, y, emit) {
    const hit = findStrokeAtPoint(x, y);
    if (!hit) return;
    state.strokeMap.delete(hit.id);
    state.drawStrokes = state.drawStrokes.filter(s => s.id !== hit.id);
    redrawAnnotations();
    if (emit) {
      socket.emit('call_annotation', {
        call_id: state.activeCallId,
        from: username,
        kind: 'erase_stroke',
        screen_owner: state.screenOwner || '',
        stroke_id: hit.id
      });
    }
  }

  function findStrokeAtPoint(x, y) {
    const threshold = 14;
    for (let i = state.drawStrokes.length - 1; i >= 0; i--) {
      const s = state.drawStrokes[i];
      for (let p = 1; p < s.points.length; p++) {
        const d = pointSegmentDistance(x, y, s.points[p - 1], s.points[p]);
        if (d <= threshold) return s;
      }
    }
    return null;
  }

  function pointSegmentDistance(px, py, a, b) {
    const vx = b.x - a.x;
    const vy = b.y - a.y;
    const wx = px - a.x;
    const wy = py - a.y;
    const c1 = vx * wx + vy * wy;
    if (c1 <= 0) return Math.hypot(px - a.x, py - a.y);
    const c2 = vx * vx + vy * vy;
    if (c2 <= c1) return Math.hypot(px - b.x, py - b.y);
    const t = c1 / c2;
    const cx = a.x + t * vx;
    const cy = a.y + t * vy;
    return Math.hypot(px - cx, py - cy);
  }

  function redrawAnnotations() {
    clearCanvas();
    const ctx = ui.canvas.getContext('2d');
    for (const s of state.drawStrokes) {
      if (!s.points || s.points.length < 2) continue;
      ctx.save();
      ctx.globalCompositeOperation = 'source-over';
      ctx.strokeStyle = s.color || '#ff4d4f';
      ctx.lineWidth = Number(s.size || 3);
      ctx.lineCap = 'round';
      ctx.beginPath();
      ctx.moveTo(s.points[0].x, s.points[0].y);
      for (let i = 1; i < s.points.length; i++) {
        ctx.lineTo(s.points[i].x, s.points[i].y);
      }
      ctx.stroke();
      ctx.restore();
    }
  }

  function clearDrawings() {
    const canDraw = !!(state.screenOwner && (state.screenOwner === username || state.allowDrawAll || state.controlGranted));
    if (!canDraw) return;
    state.drawStrokes = [];
    state.strokeMap.clear();
    clearCanvas();
    socket.emit('call_annotation', {
      call_id: state.activeCallId,
      from: username,
      kind: 'clear',
      screen_owner: state.screenOwner || ''
    });
  }

  function toggleDrawMode() {
    if (ui.drawBtn.classList.contains('disabled')) return;
    state.draw.active = !state.draw.active;
    ui.drawBtn.classList.toggle('active', state.draw.active);
    ui.canvas.classList.toggle('draw-active', state.draw.active);
    ui.drawTools.classList.toggle('hidden', !state.draw.active);
  }

  async function renderParticipantAvatars() {
    if (!ui.avatars) return;
    const token = ++state.avatarRenderToken;
    const connected = Array.from(state.participants).filter(p => p && p !== username);
    const list = [username];
    if (connected.length > 0) {
      list.push(...connected.slice(0, 5));
    }
    const avatars = await Promise.all(list.map(async (u) => ({ user: u, avatar: await getAvatar(u) })));
    if (token !== state.avatarRenderToken) return;
    ui.avatars.innerHTML = '';
    for (const { user, avatar: av } of avatars) {
      const item = document.createElement('div');
      item.className = 'call-avatar-item';
      const avatar = document.createElement('div');
      avatar.className = 'call-avatar-circle';
      if (av) {
        const img = document.createElement('img');
        img.src = av;
        avatar.appendChild(img);
      } else {
        avatar.textContent = makeInitial(user);
      }
      const label = document.createElement('div');
      label.className = 'call-avatar-label';
      label.textContent = user === username ? 'Вы' : user;
      item.append(avatar, label);
      ui.avatars.appendChild(item);
    }
  }

  function applyRemoteAnnotation(payload) {
    const ownerKey = normalizeOwnerKey(payload.screen_owner || state.screenOwner);
    const activeOwner = normalizeOwnerKey(state.annotationOwner);
    const isActiveOwner = ownerKey === activeOwner;
    if (!ownerKey && !isActiveOwner) return;

    const getOwnerStore = () => {
      if (isActiveOwner) return state.drawStrokes;
      const arr = state.annotationByOwner[ownerKey] || [];
      state.annotationByOwner[ownerKey] = arr;
      return arr;
    };

    if (payload.kind === 'clear') {
      if (isActiveOwner) {
        state.drawStrokes = [];
        state.strokeMap.clear();
        clearCanvas();
      } else if (ownerKey) {
        state.annotationByOwner[ownerKey] = [];
      }
      return;
    }
    if (payload.from === username) return;
    if (payload.kind === 'erase_stroke') {
      const id = String(payload.stroke_id || '');
      if (!id) return;
      if (isActiveOwner) {
        state.strokeMap.delete(id);
        state.drawStrokes = state.drawStrokes.filter(s => s.id !== id);
        redrawAnnotations();
      } else {
        const arr = getOwnerStore();
        state.annotationByOwner[ownerKey] = arr.filter((s) => s.id !== id);
      }
      return;
    }
    if (payload.kind !== 'stroke' || !Array.isArray(payload.points) || payload.points.length !== 4) return;

    if (state.screenOwner) {
      const sources = buildVideoSources();
      const screenSrc = sources.find(s => s.type === 'screen' && !s.isLocal) || sources.find(s => s.type === 'screen');
      if (screenSrc && state.currentMainSourceId !== screenSrc.id && !state.manualPinnedMain) {
        state.pinnedMainSourceId = screenSrc.id;
        renderVideos();
      }
    }

    const aspect = Number(payload.aspect || state.mainAspect || (16 / 9));
    const getContentRect = (w, h, a) => {
      const safe = Math.max(0.3, Math.min(4, a));
      const cwByH = h * safe;
      let cw = w;
      let ch = h;
      let ox = 0;
      let oy = 0;
      if (cwByH <= w) {
        cw = cwByH;
        ox = (w - cw) / 2;
      } else {
        ch = w / safe;
        oy = (h - ch) / 2;
      }
      return { x: ox, y: oy, w: cw, h: ch };
    };
    const content = getContentRect(ui.canvas.clientWidth, ui.canvas.clientHeight, aspect);
    const [x1n, y1n, x2n, y2n] = payload.points;
    const x1 = content.x + x1n * content.w;
    const y1 = content.y + y1n * content.h;
    const x2 = content.x + x2n * content.w;
    const y2 = content.y + y2n * content.h;
    const sid = String(payload.stroke_id || `remote_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`);
    if (isActiveOwner) {
      let stroke = state.strokeMap.get(sid);
      if (!stroke) {
        stroke = {
          id: sid,
          from: payload.from,
          color: payload.color || '#ff4d4f',
          size: Number(payload.size || 3),
          points: [{ x: x1, y: y1 }]
        };
        state.strokeMap.set(sid, stroke);
        state.drawStrokes.push(stroke);
      }
      stroke.points.push({ x: x2, y: y2 });
      redrawAnnotations();
    } else {
      const arr = getOwnerStore();
      let stroke = arr.find((s) => s.id === sid);
      if (!stroke) {
        stroke = {
          id: sid,
          from: payload.from,
          color: payload.color || '#ff4d4f',
          size: Number(payload.size || 3),
          points: [{ x: x1, y: y1 }]
        };
        arr.push(stroke);
      }
      stroke.points.push({ x: x2, y: y2 });
      if (ownerKey) state.annotationByOwner[ownerKey] = arr;
    }
  }

  socket.on('call_incoming', (invite) => {
    if (state.inCall) return;
    try {
      const mode = String(invite?.mode || 'audio');
      const from = String(invite?.from || '');
      const text = mode === 'video' ? 'Входящий видеозвонок' : 'Входящий звонок';
      window.pushDesktopNotification?.(from ? `Звонок от ${from}` : 'Входящий звонок', text, { tag: `call_${invite?.call_id || Date.now()}` });
    } catch {}
    showIncoming(invite);
  });

  socket.on('call_state', (payload) => {
    const chatId = String(payload.chat_id || '');
    if (!chatId) return;
    const keys = normalizeChatKeys(chatId);
    if (payload.active) {
      keys.forEach(k => { state.activeCallsByChat[k] = payload; });
      // Если этот аккаунт уже в участниках, на остальных устройствах убираем входящий экран/гудки.
      const participants = Array.isArray(payload.participants) ? payload.participants.map(v => String(v).toLowerCase()) : [];
      if (participants.includes(username) && state.incomingInvite && state.incomingInvite.call_id === payload.call_id) {
        stopRinging();
        hideIncoming();
      }
      if (state.pendingRejoin && !state.inCall && state.pendingRejoin.call_id === payload.call_id) {
        joinExistingCall(payload.call_id, payload.chat_id, payload.mode || state.pendingRejoin.mode || 'audio').catch(() => {});
        state.pendingRejoin = null;
      }
    } else {
      keys.forEach(k => { delete state.activeCallsByChat[k]; });
    }
    updateLiveIndicator();
    if (window.currentChat && typeof window.syncMyContacts === 'function') {
      window.syncMyContacts().catch?.(() => {});
    }
  });

  socket.on('call_incoming_cancel', (payload) => {
    const cid = String(payload?.call_id || '');
    if (!cid) return;
    if (state.incomingInvite && String(state.incomingInvite.call_id || '') === cid) {
      stopRinging();
      hideIncoming();
    }
  });

  socket.on('call_participants', async (data) => {
    if (data.call_id !== state.activeCallId) return;
    const peers = (data.participants || []).map(v => String(v).toLowerCase()).filter(v => v !== username);
    if (peers.length > 0 && state.ring.mode === 'outgoing') stopRinging();
    setScreenOwner(data.screen_owner || null);
    if (data.screen_owner && data.screen_track_id) {
      state.remoteScreenTrackIds[String(data.screen_owner).toLowerCase()] = String(data.screen_track_id);
    }
    state.allowDrawAll = !!data.allow_draw_all;
    peers.forEach((p) => state.participants.add(p));
    updateCallMeta();
    updateControlButtons();
    renderParticipantAvatars();

    for (const peer of peers) {
      ensurePeerConnection(peer);
      if (shouldInitiateOffer(peer)) {
        await createOffer(peer);
      }
    }
  });

  socket.on('call_user_joined', async (data) => {
    if (data.call_id !== state.activeCallId) return;
    const peer = String(data.username || '').toLowerCase();
    if (!peer || peer === username) return;
    if (state.ring.mode === 'outgoing') stopRinging();
    state.participants.add(peer);
    updateCallMeta();
    renderParticipantAvatars();
    ensurePeerConnection(peer);
    if (shouldInitiateOffer(peer)) {
      await createOffer(peer);
    }
  });

  socket.on('call_user_left', (data) => {
    if (data.call_id !== state.activeCallId) return;
    const peer = String(data.username || '').toLowerCase();
    state.participants.delete(peer);
    updateCallMeta();
    renderParticipantAvatars();

    const pc = state.peerConnections.get(peer);
    if (pc) pc.close();
    state.peerConnections.delete(peer);
    state.remoteStreams.delete(peer);
    delete state.remoteScreenTrackIds[peer];

    if (state.screenOwner === peer) {
      setScreenOwner(null);
    }

    updateControlButtons();
    renderVideos();
  });

  socket.on('call_ended', (data) => {
    if (data.call_id !== state.activeCallId) return;
    notify('Звонок завершен');
    const chatId = state.lastCallChatId;
    cleanupCall();
    if (chatId && typeof window.openChat === 'function') {
      window.openChat(chatId).catch?.(() => {});
    }
  });

  socket.on('call_chat', (payload) => {
    if (payload.call_id !== state.activeCallId) return;
    appendCallChat(payload.from, payload.message);
  });

  socket.on('call_signal', handleSignal);

  socket.on('call_media_state', (payload) => {
    if (payload.call_id !== state.activeCallId) return;
    state.mediaState.set(payload.from, { mic: !!payload.mic, cam: !!payload.cam });
    renderVideos();
  });

  socket.on('call_screen_share', (payload) => {
    if (payload.call_id !== state.activeCallId) return;
    const from = String(payload.from || '').toLowerCase();
    if (from && payload.sharing && payload.screen_track_id) {
      state.remoteScreenTrackIds[from] = String(payload.screen_track_id);
    }
    if (from && !payload.sharing) {
      delete state.remoteScreenTrackIds[from];
    }
    setScreenOwner(payload.screen_owner || (payload.sharing ? payload.from : null));
    updateControlButtons();
    renderVideos();
  });

  socket.on('call_annotation_perm', (payload) => {
    if (payload.call_id !== state.activeCallId) return;
    state.allowDrawAll = !!payload.allow_all;
    if (state.allowDrawAll) {
      state.controlGranted = false;
    }
    updateControlButtons();
  });

  socket.on('call_control_request', (payload) => {
    if (payload.call_id !== state.activeCallId) return;
    if (payload.owner !== username) return;
    const allow = window.confirm(`${payload.from} запрашивает управление демонстрацией. Разрешить?`);
    socket.emit('call_control_response', {
      call_id: state.activeCallId,
      owner: username,
      target: payload.from,
      allow
    });
  });

  socket.on('call_control_response', (payload) => {
    if (payload.call_id !== state.activeCallId) return;
    const target = String(payload.target || '');
    if (target === username) {
      state.controlGranted = !!payload.allow;
      if (payload.allow) {
        state.allowDrawAll = false;
      }
      notify(payload.allow ? 'Доступ к управлению выдан' : 'Доступ к управлению отклонен');
    } else if (state.screenOwner === username && payload.allow) {
      // Владелец экрана выдал доступ конкретному участнику => режим "Только я/по запросу"
      state.allowDrawAll = false;
    }
    if (ui.drawPermSelect && state.screenOwner === username) {
      ui.drawPermSelect.value = state.allowDrawAll ? 'on' : 'off';
    }
    updateControlButtons();
  });

  socket.on('call_annotation', (payload) => {
    if (payload.call_id !== state.activeCallId) return;
    applyRemoteAnnotation(payload);
  });

  document.addEventListener('DOMContentLoaded', () => {
    createLayout();
    setupEvents();
    updateHeaderActionsVisibility();
    window.addEventListener('resize', () => {
      updatePanelLimits();
      enforcePanelBounds();
    });
  });
})();
