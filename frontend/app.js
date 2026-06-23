/* ═══════════════════════════════════════════════════════════════
   DocChat — app.js
   API: FastAPI on localhost:8000
   Endpoints used:
     POST /auth/login-json  → { access_token, user }
     POST /auth/register    → { access_token, user }
     GET  /health           → { postgres, chromadb }
     GET  /stats            → { pg_files, pg_raw_chunks, total_vectors }
     GET  /files            → { files: [{name, file_type, size, uploaded_at, chunks}] }
     POST /upload           → multipart/form-data
     DELETE /files/:name
     GET  /history?file_name=
     POST /chat             → { question, file_name, top_k, chat_history }
     POST /dbt/run          → { dbt_status }
═══════════════════════════════════════════════════════════════ */

const API_URL = window.__API_URL__ || '/api';

/* ─────────────────────────────────────────────────────────────
   STATE
───────────────────────────────────────────────────────────── */
let state = {
  token:        null,
  user:         null,
  messages:     [],
  selectedFile: null,
  showChunks:   false,
  topK:         5,
  theme:        'dark',
  pendingFile:  null,
  isTyping:     false,
};

/* ─────────────────────────────────────────────────────────────
   SESSION PERSISTENCE  (localStorage, 1-hour expiry)
───────────────────────────────────────────────────────────── */
const SESSION_KEY    = 'docchat_session';
const SESSION_TTL_MS = 60 * 60 * 1000;
let   activityTimer  = null;

function saveSession() {
  if (!state.token) return;
  try {
    localStorage.setItem(SESSION_KEY, JSON.stringify({
      token:     state.token,
      user:      state.user,
      theme:     state.theme,
      expiresAt: Date.now() + SESSION_TTL_MS,
    }));
  } catch(e) {}
}
function clearSession() {
  try { localStorage.removeItem(SESSION_KEY); } catch(e) {}
}
function loadSession() {
  try {
    const raw = localStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    const p = JSON.parse(raw);
    if (!p.token || Date.now() > p.expiresAt) { clearSession(); return null; }
    return p;
  } catch(e) { return null; }
}
function resetActivityTimer() {
  clearTimeout(activityTimer);
  activityTimer = setTimeout(() => {
    if (state.token) {
      clearSession();
      forceLogout('Session expired after 1 hour of inactivity.');
    }
  }, SESSION_TTL_MS);
  if (state.token) saveSession();
}
['click','keydown','mousemove','scroll','touchstart'].forEach(ev =>
  document.addEventListener(ev, resetActivityTimer, { passive: true })
);

/* ─────────────────────────────────────────────────────────────
   API HELPERS
───────────────────────────────────────────────────────────── */
async function apiFetch(path, opts = {}) {
  const headers = { ...(opts.headers || {}) };
  if (state.token) headers['Authorization'] = `Bearer ${state.token}`;
  if (!(opts.body instanceof FormData)) headers['Content-Type'] = 'application/json';
  try {
    const res  = await fetch(API_URL + path, { ...opts, headers });
    const data = await res.json().catch(() => ({}));
    if (res.status === 401 && state.token) {
      forceLogout('Session expired. Please sign in again.');
      return { data: {}, status: 401 };
    }
    return { data, status: res.status };
  } catch(e) {
    return { data: { error: e.message }, status: 500 };
  }
}
const apiGet    = p        => apiFetch(p);
const apiPost   = (p,b,f)  => apiFetch(p, { method:'POST', body: f ? b : JSON.stringify(b) });
const apiDelete = p        => apiFetch(p, { method:'DELETE' });

/* ─────────────────────────────────────────────────────────────
   UTILS
───────────────────────────────────────────────────────────── */
function fmtSize(b) {
  if (!b) return '—';
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  return (b/1048576).toFixed(1) + ' MB';
}
function fmtDt(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleDateString('en-IN', { month:'short', day:'numeric' }) + ', ' +
           d.toLocaleTimeString([], { hour:'2-digit', minute:'2-digit' });
  } catch { return '—'; }
}
function extIcon(name) {
  const e = (name || '').split('.').pop().toUpperCase();
  return { PDF:'📕', DOCX:'📘', TXT:'📄', CSV:'📊', XLSX:'📗',
           JSON:'🗂', PPTX:'📙', MD:'📝' }[e] || '📄';
}
function getInitials(name) {
  if (!name) return '?';
  const parts = name.trim().split(' ').filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[parts.length-1][0]).toUpperCase();
  return name.slice(0,2).toUpperCase();
}
function escapeHtml(s) {
  return String(s||'')
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

/* ─────────────────────────────────────────────────────────────
   TOAST
───────────────────────────────────────────────────────────── */
function toast(msg, type = 'info') {
  const el = document.createElement('div');
  el.className = `toast toast-${type}`;
  el.textContent = msg;
  document.getElementById('toastContainer').appendChild(el);
  setTimeout(() => el.remove(), 3500);
}

/* ─────────────────────────────────────────────────────────────
   PROGRESS
───────────────────────────────────────────────────────────── */
function showProgress(on) {
  document.getElementById('globalProgress').classList.toggle('active', on);
}

/* ─────────────────────────────────────────────────────────────
   THEME
───────────────────────────────────────────────────────────── */
function setTheme(t) {
  state.theme = t;
  document.documentElement.setAttribute('data-theme', t);
  document.getElementById('pdDarkBtn')?.classList.toggle('active', t === 'dark');
  document.getElementById('pdLightBtn')?.classList.toggle('active', t === 'light');
}

/* ─────────────────────────────────────────────────────────────
   MODAL HELPERS
───────────────────────────────────────────────────────────── */
function openModal(which) {
  document.getElementById(which + 'Modal').classList.add('show');
  setTimeout(() => {
    document.getElementById(which === 'login' ? 'loginEmail' : 'regName')?.focus();
  }, 80);
}
function closeModal(which) {
  document.getElementById(which + 'Modal').classList.remove('show');
}
function closeAllModals() { closeModal('login'); closeModal('register'); }

/* ─────────────────────────────────────────────────────────────
   PROFILE
───────────────────────────────────────────────────────────── */
function updateProfileUI() {
  const name    = state.user?.name || state.user?.email?.split('@')[0] || 'User';
  const email   = state.user?.email || '';
  const initials = getInitials(name);

  document.getElementById('profileAvatar').textContent  = initials;
  document.getElementById('pdAvatar').textContent       = initials;
  document.getElementById('pdName').textContent         = name;
  document.getElementById('pdEmail').textContent        = email;
}
function openProfile() {
  document.getElementById('profileDropdown').classList.add('open');
  document.getElementById('profileOverlay').classList.add('show');
}
function closeProfile() {
  document.getElementById('profileDropdown').classList.remove('open');
  document.getElementById('profileOverlay').classList.remove('show');
}

/* ─────────────────────────────────────────────────────────────
   FORCE LOGOUT
───────────────────────────────────────────────────────────── */
function forceLogout(reason) {
  clearSession();
  clearTimeout(activityTimer);
  state = { token:null, user:null, messages:[], selectedFile:null,
            pendingFile:null, isTyping:false, showChunks: state.showChunks,
            topK: state.topK, theme: state.theme };
  document.getElementById('appShell').classList.add('hidden');
  document.getElementById('landing').classList.remove('hidden');
  clearChat();
  if (reason) toast(reason, 'warning');
}

/* ─────────────────────────────────────────────────────────────
   ENTER APP
───────────────────────────────────────────────────────────── */
async function enterApp() {
  if (!state.token) return;
  saveSession();
  resetActivityTimer();
  closeAllModals();
  document.getElementById('landing').classList.add('hidden');
  document.getElementById('appShell').classList.remove('hidden');
  updateProfileUI();
  const firstName = (state.user?.name || state.user?.email || 'there').split(' ')[0];
  toast(`Welcome, ${firstName}! 👋`, 'success');
  await loadAll();
}

/* ─────────────────────────────────────────────────────────────
   LOAD ALL
───────────────────────────────────────────────────────────── */
async function loadAll() {
  await loadHealthStats();
  await loadFiles();
  await loadHistory(null);
}

/* ─────────────────────────────────────────────────────────────
   HEALTH + STATS
───────────────────────────────────────────────────────────── */
async function loadHealthStats() {
  const [hr, sr] = await Promise.all([apiGet('/health'), apiGet('/stats')]);
  const h = hr.data, s = sr.data;
  const ok = h.postgres === 'ok';
  const dot = document.getElementById('dbDot');
  const txt = document.getElementById('dbText');
  if (dot) dot.className = 'dot ' + (ok ? 'dot-ok' : 'dot-err');
  if (txt) txt.textContent = ok ? 'DB connected' : 'DB offline';
  document.getElementById('statFiles').textContent   = s.pg_files   ?? s.total_files ?? 0;
  document.getElementById('statChunks').textContent  = s.pg_raw_chunks ?? 0;
  document.getElementById('statVectors').textContent = s.total_vectors ?? 0;
}

/* ─────────────────────────────────────────────────────────────
   FILES
───────────────────────────────────────────────────────────── */
async function loadFiles() {
  const { data } = await apiGet('/files');
  let files = data.files || data.documents || (Array.isArray(data) ? data : []);
  if (files.length && typeof files[0] === 'string')
    files = files.map(f => ({ name:f, file_type:'unknown', chunks:0, size:0, uploaded_at:null }));
  renderFiles(files);
}

function renderFiles(files) {
  const container = document.getElementById('fileList');
  const removeSection = document.getElementById('removeSection');
  const removeSelect  = document.getElementById('removeSelect');

  if (!files.length) {
    container.innerHTML = `
      <div class="no-files">
        <div class="no-files-icon">📂</div>
        No documents yet.<br>Upload a file to get started.
      </div>`;
    if (removeSection) removeSection.style.display = 'none';
    return;
  }

  container.innerHTML = files.map(f => {
    const active = f.name === state.selectedFile ? 'active' : '';
    const badgeCls = f.file_type === 'structured'   ? 'badge-structured' :
                     f.file_type === 'unstructured' ? 'badge-unstructured' : 'badge-unknown';
    const badgeTxt = f.file_type === 'structured'   ? '📊 structured' :
                     f.file_type === 'unstructured' ? '📄 unstructured' : '❔ unknown';
    return `
      <div class="file-card ${active}" data-name="${escapeHtml(f.name)}">
        <div class="fc-row">
          <span class="fc-icon">${extIcon(f.name)}</span>
          <span class="fc-name" title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</span>
          <span class="fc-badge ${badgeCls}">${badgeTxt}</span>
          <button class="fc-delete" data-del="${escapeHtml(f.name)}" title="Remove file">🗑</button>
        </div>
        <div class="fc-meta">
          <span>${f.chunks ?? 0} chunks</span>
          <span>${fmtSize(f.size)}</span>
          <span>${fmtDt(f.uploaded_at)}</span>
        </div>
      </div>`;
  }).join('');

  // File card click → select + load history (ignore delete btn clicks)
  container.querySelectorAll('.file-card').forEach(card => {
    card.addEventListener('click', (e) => {
      if (e.target.closest('.fc-delete')) return; // handled separately
      const name = card.dataset.name;
      const deselect = state.selectedFile === name;
      state.selectedFile = deselect ? null : name;
      updateFilterBanner();
      renderFiles(files);
      if (!deselect) {
        loadHistoryForFile(name);
      } else {
        clearChat();
        loadHistory(null);
      }
    });
  });

  // Delete icon click → confirm modal
  container.querySelectorAll('.fc-delete').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      showConfirmDelete(btn.dataset.del);
    });
  });
}

/* ─────────────────────────────────────────────────────────────
   HISTORY
───────────────────────────────────────────────────────────── */
async function loadHistory(fileName) {
  const url = fileName
    ? `/history?file_name=${encodeURIComponent(fileName)}`
    : '/history';
  const { data } = await apiGet(url);
  const history  = data.history || [];
  state.messages = [];
  for (const item of history) {
    const ts = fmtDt(item.created_at);
    if (item.question) state.messages.push({ role:'user', content:item.question, ts });
    if (item.answer)   state.messages.push({
      role:'assistant', content:item.answer, ts,
      sources:    item.file_name ? [item.file_name] : [],
      chunks:     [],
      model:      item.model      || '',
      sql: item.sql || item.generated_sql || null,
      table_name: item.table_name || null,
      row_count:  item.row_count  ?? null,
      file_type:  item.file_type  || null,
    });
  }
  renderMessages();
}

async function loadHistoryForFile(fileName) {
  await loadHistory(fileName);
  toast(state.messages.length > 0
    ? `📂 Loaded history for ${fileName}`
    : `No history yet for ${fileName}`, 'info');
}

/* ─────────────────────────────────────────────────────────────
   CHAT RENDER
───────────────────────────────────────────────────────────── */
const chatArea = document.getElementById('chatArea');
const emptyState = document.getElementById('emptyState');

function clearChat() {
  state.messages = [];
  renderMessages();
}

function renderMessages() {
  chatArea.innerHTML = '';
  if (!state.messages.length) {
    chatArea.appendChild(emptyState);
    return;
  }
  if (emptyState.parentNode) emptyState.parentNode.removeChild(emptyState);
  state.messages.forEach(msg => chatArea.appendChild(buildMsgEl(msg)));
  scrollBottom();
}

function buildMsgEl(msg) {
  const row = document.createElement('div');
  row.className = 'msg-row ' + msg.role;

  // Avatar
  const av = document.createElement('div');
  if (msg.role === 'user') {
    av.className = 'msg-avatar user-avatar';
    av.textContent = getInitials(state.user?.name || 'U');
  } else {
    av.className = 'msg-avatar bot-avatar';
    av.textContent = '⚡';
  }

  // Body
  const body = document.createElement('div');
  body.className = 'msg-body';

  // Bubble
  const bubble = document.createElement('div');
  bubble.className = 'msg-bubble ' + (msg.role === 'user' ? 'user-bubble' : 'bot-bubble');
  bubble.innerHTML = escapeHtml(msg.content || '').replace(/\n/g, '<br>');
  body.appendChild(bubble);

  // Footer
  const footer = document.createElement('div');
  footer.className = 'msg-footer';
  if (msg.ts) footer.innerHTML += `<span class="msg-time">${msg.ts}</span>`;
  if (msg.model) footer.innerHTML += `<span class="msg-model">${escapeHtml(msg.model)}</span>`;
  if (msg.sources?.length) {
    msg.sources.forEach(s => {
      footer.innerHTML += `<span class="src-pill">📎 ${escapeHtml(s)}</span>`;
    });
  }
  body.appendChild(footer);

  // Chunks
  if (msg.chunks?.length && state.showChunks) {
    const toggle = document.createElement('div');
    toggle.className = 'chunks-toggle';
    toggle.innerHTML = `🔍 ${msg.chunks.length} source chunks`;
    body.appendChild(toggle);

    const expander = document.createElement('div');
    expander.className = 'chunks-expander hidden';
    msg.chunks.forEach((c, i) => {
      const pct = Math.round((c.score || 0) * 100);
      expander.innerHTML += `
        <div class="chunk-item">
          <div class="chunk-meta">Chunk ${i+1} · ${escapeHtml(c.file_name||'')} · score: ${(c.score||0).toFixed(3)}</div>
          <div class="chunk-bar-track"><div class="chunk-bar-fill" style="width:${pct}%"></div></div>
          <div class="chunk-text">${escapeHtml((c.chunk_text||'').slice(0,300))}${(c.chunk_text||'').length>300?'…':''}</div>
        </div>`;
    });
    toggle.addEventListener('click', () => expander.classList.toggle('hidden'));
    body.appendChild(expander);
  }

  // Structured query details (SQL + table + row count)
  if (msg.sql && msg.role === 'assistant') {
    const sqlToggle = document.createElement('div');
    sqlToggle.className = 'chunks-toggle';
    sqlToggle.style.borderColor = 'rgba(34,211,238,0.3)';
    sqlToggle.innerHTML = '📊 Structured Query Details';
    body.appendChild(sqlToggle);

    const sqlExpander = document.createElement('div');
    sqlExpander.className = 'chunks-expander hidden sql-expander';
    sqlExpander.innerHTML = `
      <div class="sql-label">SQL Query</div>
      <pre class="sql-code">${escapeHtml(msg.sql)}</pre>
      ${msg.table_name ? `<div class="sql-meta">Table: <span class="sql-val">${escapeHtml(msg.table_name)}</span></div>` : ''}
      ${msg.row_count != null ? `<div class="sql-meta">Rows returned: <span class="sql-val">${msg.row_count}</span></div>` : ''}
    `;
    sqlToggle.addEventListener('click', () => sqlExpander.classList.toggle('hidden'));
    body.appendChild(sqlExpander);
  }

  row.appendChild(av);
  row.appendChild(body);
  return row;
}

function addMessage(msg) {
  state.messages.push(msg);
  if (emptyState.parentNode) emptyState.parentNode.removeChild(emptyState);
  chatArea.appendChild(buildMsgEl(msg));
  scrollBottom();
}

function scrollBottom() {
  chatArea.scrollTop = chatArea.scrollHeight;
}

/* ─────────────────────────────────────────────────────────────
   TYPING INDICATOR
───────────────────────────────────────────────────────────── */
let typingEl = null;
function showTyping() {
  const botAv = document.createElement('div');
  botAv.className = 'msg-avatar bot-avatar';
  botAv.textContent = '⚡';
  const bubble = document.createElement('div');
  bubble.className = 'typing-bubble';
  bubble.innerHTML = `<div class="typing-dot"></div><div class="typing-dot"></div><div class="typing-dot"></div>`;
  typingEl = document.createElement('div');
  typingEl.className = 'typing-row';
  typingEl.appendChild(botAv);
  typingEl.appendChild(bubble);
  chatArea.appendChild(typingEl);
  scrollBottom();
}
function hideTyping() {
  if (typingEl) { typingEl.remove(); typingEl = null; }
}

/* ─────────────────────────────────────────────────────────────
   FILTER BANNER
───────────────────────────────────────────────────────────── */
function updateFilterBanner() {
  const banner = document.getElementById('filterBanner');
  const fileEl = document.getElementById('filterFile');
  if (state.selectedFile) {
    fileEl.textContent = state.selectedFile;
    banner.classList.remove('hidden');
  } else {
    banner.classList.add('hidden');
  }
}

/* ─────────────────────────────────────────────────────────────
   SEND MESSAGE
───────────────────────────────────────────────────────────── */
const chatInput = document.getElementById('chatInput');
const sendBtn   = document.getElementById('sendBtn');
const charCount = document.getElementById('charCount');

chatInput.addEventListener('input', () => {
  chatInput.style.height = 'auto';
  chatInput.style.height = Math.min(chatInput.scrollHeight, 120) + 'px';
  charCount.textContent = chatInput.value.length;
});

chatInput.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    e.stopPropagation();
    sendMessage();
  }
});

sendBtn.addEventListener('click', sendMessage);

async function sendMessage() {
  const q = (chatInput.value || '').trim();
  if (!q) return;
  if (state.isTyping) { toast('Please wait for the current response…', 'warning'); return; }

  const ts = new Date().toLocaleTimeString([], { hour:'2-digit', minute:'2-digit' });
  addMessage({ role:'user', content:q, ts });
  chatInput.value = '';
  chatInput.style.height = 'auto';
  charCount.textContent = '0';

  state.isTyping    = true;
  sendBtn.disabled  = true;
  sendBtn.style.opacity = '0.5';
  showTyping();
  showProgress(true);

  const history = state.messages.slice(-6)
    .filter(m => ['user','assistant'].includes(m.role))
    .map(m => ({ role:m.role, content:m.content }));

  let data = {}, status = 500;
  try {
    const r = await apiPost('/chat', {
      question:     q,
      file_name:    state.selectedFile,
      top_k:        state.topK,
      chat_history: history,
    });
    data   = r.data;
    status = r.status;
  } catch(err) {
    data = { error: err.message };
  } finally {
    hideTyping();
    showProgress(false);
    state.isTyping   = false;
    sendBtn.disabled = false;
    sendBtn.style.opacity = '';
  }

  const tsAns = new Date().toLocaleTimeString([], { hour:'2-digit', minute:'2-digit' });

  if (status === 200) {
    const answer = data.answer || data.response || data.result || data.message || data.text || 'No answer returned.';
    addMessage({
      role:'assistant', content:answer, ts:tsAns,
      model:      data.model || data.model_used || '',
      sources:    data.sources    || [],
      chunks:     data.chunks     || [],
      sql:        data.sql        || null,
      table_name: data.table_name || null,
      row_count:  data.row_count  ?? null,
      file_type:  data.file_type  || null,
    });
  } else {
    addMessage({
      role:'assistant',
      content: '❌ ' + (data.detail || data.message || data.error || `Server error (${status})`),
      ts: tsAns,
    });
  }
}

/* ─────────────────────────────────────────────────────────────
   UPLOAD
───────────────────────────────────────────────────────────── */
const uploadInput   = document.getElementById('uploadInput');
const uploadZone    = document.getElementById('uploadZone');
const uploadPending = document.getElementById('uploadPending');
const uploadName    = document.getElementById('uploadPendingName');
const ingestBtn     = document.getElementById('ingestBtn');

uploadZone.addEventListener('dragover',  e => { e.preventDefault(); uploadZone.classList.add('drag-over'); });
uploadZone.addEventListener('dragleave', () => uploadZone.classList.remove('drag-over'));
uploadZone.addEventListener('drop', e => {
  e.preventDefault();
  uploadZone.classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (file) setFile(file);
});
uploadInput.addEventListener('change', () => {
  if (uploadInput.files[0]) setFile(uploadInput.files[0]);
});

function setFile(file) {
  state.pendingFile = file;
  uploadName.textContent = `${file.name} · ${fmtSize(file.size)}`;
  uploadPending.classList.remove('hidden');
}

ingestBtn.addEventListener('click', async () => {
  if (!state.pendingFile) return;
  const formData = new FormData();
  formData.append('file', state.pendingFile);
  showProgress(true);
  ingestBtn.disabled = true;
  const { data, status } = await apiPost('/upload', formData, true);
  showProgress(false);
  ingestBtn.disabled = false;
  if (status === 200) {
    const chunks = data.chunks ?? 0;
    toast(`✅ ${state.pendingFile.name} ingested · ${chunks} chunks`, 'success');
    state.pendingFile = null;
    uploadPending.classList.add('hidden');
    uploadInput.value = '';
    await loadFiles();
    await loadHealthStats();
  } else if (status === 400 && (data.detail||'').toLowerCase().includes('duplicate')) {
    toast('File already uploaded — skipped.', 'warning');
  } else {
    toast('Upload failed: ' + (data.detail || data.error || 'Unknown error'), 'error');
  }
});

/* ─────────────────────────────────────────────────────────────
   REMOVE DOCUMENT
───────────────────────────────────────────────────────────── */
/* ── Confirm delete modal ── */
let pendingDeleteName = null;

function showConfirmDelete(name) {
  pendingDeleteName = name;
  document.getElementById('confirmFileName').textContent = name;
  document.getElementById('confirmOverlay').classList.add('show');
}
function hideConfirmDelete() {
  pendingDeleteName = null;
  document.getElementById('confirmOverlay').classList.remove('show');
}

document.getElementById('confirmCancel').addEventListener('click', hideConfirmDelete);
document.getElementById('confirmOverlay').addEventListener('click', (e) => {
  if (e.target === document.getElementById('confirmOverlay')) hideConfirmDelete();
});

document.getElementById('confirmDelete').addEventListener('click', async () => {
  if (!pendingDeleteName) return;
  const name = pendingDeleteName;
  hideConfirmDelete();
  showProgress(true);
  const { status, data } = await apiDelete(`/files/${encodeURIComponent(name)}`);
  showProgress(false);
  if (status === 200) {
    if (state.selectedFile === name) {
      state.selectedFile = null;
      updateFilterBanner();
      clearChat();
    }
    toast(`Removed "${name}"`, 'warning');
    await loadFiles();
    await loadHealthStats();
  } else {
    toast('Remove failed: ' + (data.detail || 'Unknown error'), 'error');
  }
});

/* ─────────────────────────────────────────────────────────────
   SETTINGS
───────────────────────────────────────────────────────────── */
const topKSlider  = document.getElementById('topKSlider');
const topKDisplay = document.getElementById('topKDisplay');
const chunksToggle = document.getElementById('chunksToggle');

topKSlider.addEventListener('input', () => {
  state.topK = parseInt(topKSlider.value);
  topKDisplay.textContent = state.topK;
});

chunksToggle.addEventListener('click', () => {
  state.showChunks = !state.showChunks;
  chunksToggle.classList.toggle('on', state.showChunks);
});

/* ─────────────────────────────────────────────────────────────
   SIDEBAR ACTIONS
───────────────────────────────────────────────────────────── */
document.getElementById('clearChatBtn').addEventListener('click', () => {
  clearChat();
  toast('Chat cleared', 'info');
});
document.getElementById('topClearBtn').addEventListener('click', () => {
  clearChat();
  toast('Chat cleared', 'info');
});
document.getElementById('dbtBtn').addEventListener('click', async () => {
  showProgress(true);
  const { data } = await apiPost('/dbt/run', {});
  showProgress(false);
  toast('dbt: ' + (data.dbt_status || 'done'), 'info');
});

/* ─────────────────────────────────────────────────────────────
   FILTER BANNER CLEAR
───────────────────────────────────────────────────────────── */
document.getElementById('filterClear').addEventListener('click', () => {
  state.selectedFile = null;
  updateFilterBanner();
  clearChat();
  loadFiles().then(() => loadHistory(null));
});

/* ─────────────────────────────────────────────────────────────
   SUGGESTION CHIPS
───────────────────────────────────────────────────────────── */
document.querySelectorAll('.chip').forEach(chip => {
  chip.addEventListener('click', () => {
    chatInput.value = chip.textContent.trim();
    chatInput.dispatchEvent(new Event('input'));
    chatInput.focus();
  });
});

/* ─────────────────────────────────────────────────────────────
   PROFILE DROPDOWN
───────────────────────────────────────────────────────────── */
document.getElementById('profileAvatar').addEventListener('click', e => {
  e.stopPropagation();
  document.getElementById('profileDropdown').classList.contains('open') ? closeProfile() : openProfile();
});
document.getElementById('profileOverlay').addEventListener('click', closeProfile);

document.getElementById('pdDarkBtn').addEventListener('click', () => { setTheme('dark'); });
document.getElementById('pdLightBtn').addEventListener('click', () => { setTheme('light'); });

document.getElementById('pdLogout').addEventListener('click', () => {
  closeProfile();
  forceLogout('Signed out successfully.');
  toast('Signed out', 'success');
});

/* ─────────────────────────────────────────────────────────────
   AUTH — LOGIN MODAL
───────────────────────────────────────────────────────────── */
// Nav / hero buttons
document.getElementById('navSignIn').addEventListener('click',  () => openModal('login'));
document.getElementById('navSignUp').addEventListener('click',  () => openModal('register'));
document.getElementById('heroSignIn').addEventListener('click', () => openModal('login'));
document.getElementById('heroSignUp').addEventListener('click', () => openModal('register'));

// Close
document.getElementById('loginModalClose').addEventListener('click',    () => closeModal('login'));
document.getElementById('registerModalClose').addEventListener('click', () => closeModal('register'));

// Switch
document.getElementById('switchToRegister').addEventListener('click', () => { closeModal('login');    openModal('register'); });
document.getElementById('switchToLogin').addEventListener('click',    () => { closeModal('register'); openModal('login');    });

// Click outside
['loginModal','registerModal'].forEach(id => {
  document.getElementById(id).addEventListener('click', e => {
    if (e.target === document.getElementById(id)) closeModal(id.replace('Modal',''));
  });
});

// Escape key
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closeAllModals();
});

// Login submit
document.getElementById('loginForm').addEventListener('submit', async e => {
  e.preventDefault();
  const email   = document.getElementById('loginEmail').value.trim();
  const pass    = document.getElementById('loginPassword').value;
  const errEl   = document.getElementById('loginError');
  const progress = document.getElementById('loginProgress');
  errEl.classList.remove('show');
  if (!email || !pass) {
    errEl.textContent = 'Please fill in all fields.';
    errEl.classList.add('show'); return;
  }
  progress.classList.add('loading');
  showProgress(true);
  const { data, status } = await apiPost('/auth/login-json', { email, password: pass });
  progress.classList.remove('loading');
  showProgress(false);

  if (status === 200) {
    const token = data.access_token || data.token || data.jwt || data.accessToken;
    if (!token) {
      errEl.textContent = 'Login succeeded but no token received.';
      errEl.classList.add('show'); return;
    }
    state.token = token;
    state.user  = data.user || data.user_info || { email, name: email.split('@')[0] };
    await enterApp();
  } else {
    errEl.textContent = data.detail || data.message || data.error || 'Login failed.';
    errEl.classList.add('show');
  }
});

// Register submit
document.getElementById('registerForm').addEventListener('submit', async e => {
  e.preventDefault();
  const name    = document.getElementById('regName').value.trim();
  const email   = document.getElementById('regEmail').value.trim();
  const pass    = document.getElementById('regPassword').value;
  const errEl   = document.getElementById('registerError');
  const progress = document.getElementById('registerProgress');
  errEl.classList.remove('show');
  if (!name || !email || !pass) {
    errEl.textContent = 'Please fill in all fields.';
    errEl.classList.add('show'); return;
  }
  if (pass.length < 8) {
    errEl.textContent = 'Password must be at least 8 characters.';
    errEl.classList.add('show'); return;
  }
  progress.classList.add('loading');
  showProgress(true);
  const { data, status } = await apiPost('/auth/register', { name, email, password: pass });
  progress.classList.remove('loading');
  showProgress(false);

  if (status === 200 || status === 201) {
    const token = data.access_token || data.token || data.jwt || data.accessToken;
    if (!token) {
      errEl.textContent = 'Account created. Please sign in.';
      errEl.classList.add('show');
      setTimeout(() => { closeModal('register'); openModal('login'); }, 1500);
      return;
    }
    state.token = token;
    state.user  = data.user || data.user_info || { email, name };
    await enterApp();
  } else {
    errEl.textContent = data.detail || data.message || data.error || 'Registration failed.';
    errEl.classList.add('show');
  }
});

/* ─────────────────────────────────────────────────────────────
   RESTORE SESSION ON PAGE LOAD
───────────────────────────────────────────────────────────── */
(async function restoreSession() {
  const saved = loadSession();
  if (saved) {
    state.token = saved.token;
    state.user  = saved.user;
    if (saved.theme) setTheme(saved.theme);
    await enterApp();
  }
})();

/* ─────────────────────────────────────────────────────────────
   Premium UX enhancements — no feature removal
───────────────────────────────────────────────────────────── */
(function enhanceDocChatUI(){
  function ready(fn){
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', fn);
    else fn();
  }
  ready(() => {
    document.querySelectorAll('.suggestion-chips .chip').forEach(chip => {
      chip.setAttribute('role', 'button');
      chip.setAttribute('tabindex', '0');
      const useChip = () => {
        const input = document.getElementById('chatInput');
        const count = document.getElementById('charCount');
        if (!input) return;
        input.value = chip.textContent.replace(/^\S+\s*/, '').trim();
        input.focus();
        input.dispatchEvent(new Event('input', { bubbles: true }));
        if (count) count.textContent = input.value.length;
      };
      chip.addEventListener('click', useChip);
      chip.addEventListener('keydown', e => {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); useChip(); }
      });
    });

    document.querySelectorAll('button, .file-card, .chip, .profile-avatar').forEach(el => {
      el.addEventListener('pointerdown', () => el.classList.add('pressing'));
      ['pointerup','pointerleave','blur'].forEach(ev => el.addEventListener(ev, () => el.classList.remove('pressing')));
    });
  });
})();
