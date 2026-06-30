/* ═══════════════════════════════════════════════════════════════
   DocChat — app.js
   API: FastAPI on localhost:8000
   Endpoints used:
     POST /auth/login-json  → { access_token, user }
     POST /auth/register    → { access_token, user }
     GET  /health           → { postgres, Pgvector }
     GET  /stats            → { pg_files, pg_raw_chunks, total_vectors }
     GET  /files            → { files: [{name, file_type, size, uploaded_at, chunks}] }
     POST /upload           → multipart/form-data
     DELETE /files/:name
     GET  /history?file_name=
     POST /chat             → { question, file_name, top_k, chat_history }
     POST /dbt/run          → { dbt_status }
     GET  /structured/status/{file_name} → { status, ready, message, row_count }
═══════════════════════════════════════════════════════════════ */

// const API_URL = window.__API_URL__ || '/api';
const API_URL =
  location.hostname === "localhost" || location.hostname === "127.0.0.1"
    ? "http://localhost:8000"
    : "/api";
/* ─────────────────────────────────────────────────────────────
   STATE
───────────────────────────────────────────────────────────── */
let state = {
  token:           null,
  user:            null,
  messages:        [],
  selectedFile:    null,
  selectedFiles:   [],
  showChunks:      false,
  topK:            5,
  theme:           'dark',
  pendingFile:     null,
  pendingFiles:    [],
  isTyping:        false,
  fileStatuses:    {},   // { fileName: { status, ready, message, row_count } }
  statusPollers:   {},   // { fileName: intervalId }
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
   FILE STATUS POLLING
   Uses GET /structured/status/{file_name}
   Polls every 8s for structured files until status = 'ready'
───────────────────────────────────────────────────────────── */

const STATUS_ICONS = {
  upload_saved:           { icon: '📤', label: 'Uploaded',          cls: 'status-pending'  },
  sqs_queued:             { icon: '📨', label: 'Queued',            cls: 'status-pending'  },
  step_function_started:  { icon: '🔁', label: 'Workflow Started',  cls: 'status-running'  },
  processing:             { icon: '⚙️', label: 'Processing',        cls: 'status-running'  },
  glue_job_pending:       { icon: '⏳', label: 'Pending',           cls: 'status-pending'  },
  glue_job_started:       { icon: '⚙️', label: 'Processing',        cls: 'status-running'  },
  ready:                  { icon: '✅', label: 'Ready',             cls: 'status-ready'    },
  error:                  { icon: '❌', label: 'Failed',            cls: 'status-error'    },
  sqs_failed:             { icon: '❌', label: 'Queue Failed',      cls: 'status-error'    },
  not_found:              { icon: '❓', label: 'Unknown',           cls: 'status-unknown'  },
};

async function fetchFileStatus(fileName) {
  const { data } = await apiGet(`/structured/status/${encodeURIComponent(fileName)}`);
  return data;
}

function renderFileStatusBadge(fileName) {
  const info = state.fileStatuses[fileName];
  if (!info) return '';
  const s = STATUS_ICONS[info.status] || STATUS_ICONS['not_found'];
  const rowInfo = info.row_count ? ` · ${info.row_count.toLocaleString()} rows` : '';
  return `<span class="file-status-badge ${s.cls}" title="${escapeHtml(info.message || '')}">${s.icon} ${s.label}${rowInfo}</span>`;
}

function startStatusPolling(fileName) {
  // Only poll structured files not yet ready
  if (state.statusPollers[fileName]) return; // already polling

  async function poll() {
    const data = await fetchFileStatus(fileName);
    if (!data || !data.status) return;

    state.fileStatuses[fileName] = data;

    // Re-render file list to show updated status badge
    const card = document.querySelector(`.file-card[data-name="${CSS.escape(fileName)}"]`);
    if (card) {
      const badge = card.querySelector('.file-status-badge');
      const newBadgeHtml = renderFileStatusBadge(fileName);
      if (badge) {
        badge.outerHTML = newBadgeHtml;
      } else {
        // Insert after the fc-row
        const fcRow = card.querySelector('.fc-row');
        if (fcRow && newBadgeHtml) {
          const tmp = document.createElement('div');
          tmp.innerHTML = newBadgeHtml;
          fcRow.after(tmp.firstChild);
        }
      }
    }

    // Show toast when ready
    if (data.status === 'ready' || data.status === 'error') {
      stopStatusPolling(fileName);
      if (data.status === 'ready') {
        toast(`✅ ${fileName} is ready! You can now ask questions.`, 'success');
        // If this file is currently selected, update filter banner
        if (state.selectedFile === fileName) {
          updateFilterBanner();
        }
        // Reload file list to refresh chunk count
        loadFiles();
      } else {
        toast(`❌ ${fileName} processing failed. Try re-uploading.`, 'error');
      }
    }
  }

  poll(); // immediate first check
  state.statusPollers[fileName] = setInterval(poll, 8000); // every 8s
}

function stopStatusPolling(fileName) {
  if (state.statusPollers[fileName]) {
    clearInterval(state.statusPollers[fileName]);
    delete state.statusPollers[fileName];
  }
}

function stopAllPolling() {
  Object.keys(state.statusPollers).forEach(stopStatusPolling);
}

// Start polling for all pending structured files on load
async function initStatusPolling(files) {
  for (const f of files) {
    const data = await fetchFileStatus(f.name);
    if (!data) continue;

    state.fileStatuses[f.name] = data;

    if (!data.ready && data.status !== 'error' && data.status !== 'not_found') {
      startStatusPolling(f.name);
    }
  }
}

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



function getApiErrorMessage(data, fallback = 'Request failed') {
  if (!data) return fallback;

  if (typeof data.detail === 'string') {
    return data.detail;
  }

  if (data.detail && typeof data.detail.message === 'string') {
    return data.detail.message;
  }

  if (typeof data.error === 'string') {
    return data.error;
  }

  if (typeof data.message === 'string') {
    return data.message;
  }

  return fallback;
}

function normalizeSqlList(sql) {
  if (!sql) return [];

  if (Array.isArray(sql)) {
    return sql;
  }

  if (typeof sql === 'string') {
    try {
      const parsed = JSON.parse(sql);
      if (Array.isArray(parsed)) {
        return parsed;
      }
      if (parsed && typeof parsed === 'object') {
        return [parsed];
      }
    } catch(e) {
      return [{ sql }];
    }

    return [{ sql }];
  }

  if (typeof sql === 'object') {
    return [sql];
  }

  return [];
}
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
  stopAllPolling();
  state = { token:null, user:null, messages:[], selectedFile:null,
            selectedFiles: [],
            pendingFile:null, pendingFiles:[], isTyping:false, showChunks: state.showChunks,
            topK: state.topK, theme: state.theme,
            fileStatuses: {}, statusPollers: {} };
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
  // Start polling for structured files not yet ready
  await initStatusPolling(files);
  renderFiles(files); // re-render with status badges
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
    const active = (state.selectedFiles || []).includes(f.name) ? 'active' : '';
    const badgeCls = f.file_type === 'structured'   ? 'badge-structured' :
                     f.file_type === 'unstructured' ? 'badge-unstructured' : 'badge-unknown';
    const badgeTxt = f.file_type === 'structured'   ? '📊 structured' :
                     f.file_type === 'unstructured' ? '📄 unstructured' : '❔ unknown';

    // Status badge for structured files
    // Status badge — structured files poll Glue; unstructured show Ready immediately
    const rawStatus =
      f.processing_status ||
      f.status ||
      (f.file_type === 'structured'
        ? state.fileStatuses[f.name]?.status
        : 'upload_saved');

    state.fileStatuses[f.name] = {
      ...(state.fileStatuses[f.name] || {}),
      status: rawStatus,
      ready: rawStatus === 'ready',
      message: f.processing_error || '',
      row_count: f.row_count || null,
    };

    const statusBadge = renderFileStatusBadge(f.name);
    // If structured and not ready, grey out the card
    const statusInfo = state.fileStatuses[f.name];
    const notReady = statusInfo && !statusInfo.ready;
    const cardCls = `file-card ${active} ${notReady ? 'file-not-ready' : ''}`;

    return `
      <div class="${cardCls}" data-name="${escapeHtml(f.name)}" data-type="${f.file_type}">
        <div class="fc-row">
          <input type="checkbox" class="fc-select" data-select="${escapeHtml(f.name)}" ${(state.selectedFiles || []).includes(f.name) ? 'checked' : ''} title="Select for multi-file chat">
          <span class="fc-icon">${extIcon(f.name)}</span>
          <span class="fc-name" title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</span>
          <span class="fc-badge ${badgeCls}">${badgeTxt}</span>
          ${['error', 'sqs_failed'].includes(rawStatus)
            ? `<button class="fc-retry" data-retry="${f.document_id}" title="Retry processing">↻</button>`
            : ''
          }
          <button class="fc-delete" data-del="${escapeHtml(f.name)}" title="Remove file">🗑</button>
        </div>
        ${statusBadge}
        <div class="fc-meta">
          <span>${f.chunks ?? 0} chunks</span>
          <span>${fmtSize(f.size)}</span>
          <span>${fmtDt(f.uploaded_at)}</span>
        </div>
      </div>`;
  }).join('');

  // File card click → single select
  // Checkbox click → multi-select
  container.querySelectorAll('.file-card').forEach(card => {
    card.addEventListener('click', (e) => {
      if (e.target.closest('.fc-delete') || e.target.closest('.fc-retry')) return;

      const name = card.dataset.name;

      const statusInfo = state.fileStatuses[name];
      if (statusInfo && !statusInfo.ready) {
        toast(`⏳ ${name} is still processing. ${statusInfo.message || 'Please wait.'}`, 'warning');
        e.preventDefault();
        return;
      }

      // Multi-file checkbox selection
      if (e.target.closest('.fc-select')) {
        const checked = e.target.checked;

        if (checked) {
          if (!state.selectedFiles.includes(name)) {
            state.selectedFiles.push(name);
          }
        } else {
          state.selectedFiles = state.selectedFiles.filter(f => f !== name);
        }

        state.selectedFile = state.selectedFiles.length === 1 ? state.selectedFiles[0] : null;

        updateFilterBanner();
        renderFiles(files);

        if (state.selectedFiles.length === 1) {
          loadHistoryForFile(state.selectedFiles[0]);
        } else {
          clearChat();
          loadHistory(null);
        }

        return;
      }

      // Normal card click = single-file selection
      const alreadySingle =
        state.selectedFiles.length === 1 &&
        state.selectedFiles[0] === name;

      if (alreadySingle) {
        state.selectedFiles = [];
        state.selectedFile = null;
        clearChat();
        loadHistory(null);
      } else {
        state.selectedFiles = [name];
        state.selectedFile = name;
        loadHistoryForFile(name);
      }

      updateFilterBanner();
      renderFiles(files);
    });
  });

  // Delete icon click → confirm modal
  container.querySelectorAll('.fc-delete').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      showConfirmDelete(btn.dataset.del);
    });
  });
  container.querySelectorAll('.fc-retry').forEach(btn => {
  btn.addEventListener('click', async (e) => {
    e.stopPropagation();

    const documentId = btn.dataset.retry;

    if (!documentId) {
      toast('Cannot retry: document id missing', 'error');
      return;
    }

    showProgress(true);

    const { data, status } = await apiPost(`/upload/${documentId}/retry-queue`, {});

    showProgress(false);

    if (status === 200) {
      toast('File sent for retry successfully.', 'success');
      await loadFiles();
    } else {
      toast('Retry failed: ' + getApiErrorMessage(data, 'Unknown error'), 'error');
    }
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

    if (item.question) {
      state.messages.push({
        role: 'user',
        content: item.question,
        ts
      });
    }

    if (item.answer) {
      const rows = item.rows || [];
      const columns = item.columns || [];

      const hasStructuredTable =
        Array.isArray(rows) &&
        rows.length > 0 &&
        Array.isArray(columns) &&
        columns.length > 0;

      const cleanAnswer = hasStructuredTable
        ? `Found ${rows.length} result${rows.length > 1 ? 's' : ''}. See the table below.`
        : item.answer;

      state.messages.push({
        role: 'assistant',
        content: cleanAnswer,
        ts,

        sources: item.file_name ? [item.file_name] : (item.file_names || []),
        chunks: [],
        model: item.model || '',

        sql: item.sql || item.generated_sql || null,
        table_name: item.table_name || null,
        row_count: item.row_count ?? rows.length ?? null,
        file_type: item.file_type || null,

        rows,
        columns,
        multi_results: item.multi_results || item.result_rows || [],
      });
    }
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
const chatThread = document.getElementById('chatThread') || chatArea;
const emptyState = document.getElementById('emptyState');

function clearChat() {
  state.messages = [];
  renderMessages();
}

function renderMessages() {
  chatThread.innerHTML = '';
  if (!state.messages.length) {
    chatThread.appendChild(emptyState);
    return;
  }
  if (emptyState.parentNode) emptyState.parentNode.removeChild(emptyState);
  state.messages.forEach(msg => chatThread.appendChild(buildMsgEl(msg)));
  scrollBottom();
}
function formatKey(k) {
  return String(k || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

function formatVal(v) {
  if (v === null || v === undefined || v === '') return '—';
  return escapeHtml(String(v));
}

function isStructuredMsg(msg) {
  return msg.role === 'assistant' && (
    msg.file_type === 'structured' ||
    msg.file_type === 'multi' ||
    msg.sql ||
    msg.rows?.length ||
    msg.multi_results?.length
  );
}

function buildStructuredResult(msg) {
  if (!msg.rows || !msg.rows.length) return '';

  const rows = msg.rows;
  const columns = msg.columns || Object.keys(rows[0] || {});

  if (rows.length === 1) {
    return `
      <div class="data-card">
        <div class="data-card-title">Result Details</div>
        <div class="data-card-grid">
          ${columns.map(c => `
            <div class="data-kv">
              <div class="data-key">${escapeHtml(formatKey(c))}</div>
              <div class="data-val">${formatVal(rows[0][c])}</div>
            </div>
          `).join('')}
        </div>
      </div>
    `;
  }

  return `
    <div class="data-table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            ${columns.map(c => `<th>${escapeHtml(formatKey(c))}</th>`).join('')}
          </tr>
        </thead>
        <tbody>
          ${rows.map(r => `
            <tr>
              ${columns.map(c => `<td>${formatVal(r[c])}</td>`).join('')}
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function buildMultiFileResults(msg) {
  if (!Array.isArray(msg.multi_results) || !msg.multi_results.length) {
    return '';
  }

  return msg.multi_results.map(item => {
    const fileName = item.file_name || 'Unknown file';
    const rows = item.rows || [];

    if (!Array.isArray(rows) || !rows.length) return '';

    const columns = Object.keys(rows[0] || {});

    return `
      <div class="data-card">
        <div class="data-card-title">📄 ${escapeHtml(fileName)} Results (${rows.length} rows)</div>
        <div class="data-table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                ${columns.map(c => `<th>${escapeHtml(formatKey(c))}</th>`).join('')}
              </tr>
            </thead>
            <tbody>
              ${rows.map(r => `
                <tr>
                  ${columns.map(c => `<td>${formatVal(r[c])}</td>`).join('')}
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }).join('');
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
  if (msg.role === 'assistant' && isStructuredMsg(msg)) {
    bubble.innerHTML = `
      <div class="answer-text">${escapeHtml(msg.content || '').replace(/\n/g, '<br>')}</div>
      ${buildStructuredResult(msg)}
      ${buildMultiFileResults(msg)}
    `;
  } else {
    bubble.innerHTML = escapeHtml(msg.content || '').replace(/\n/g, '<br>');
  }

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

  // Structured query details (single SQL or multi-file SQL list)
  if (msg.sql && msg.role === 'assistant') {
    const sqlList = normalizeSqlList(msg.sql);

    if (sqlList.length) {
      const sqlToggle = document.createElement('div');
      sqlToggle.className = 'chunks-toggle';
      sqlToggle.style.borderColor = 'rgba(34,211,238,0.3)';
      sqlToggle.innerHTML = sqlList.length > 1
        ? `📊 Structured Query Details (${sqlList.length} files)`
        : '📊 Structured Query Details';
      body.appendChild(sqlToggle);

      const sqlExpander = document.createElement('div');
      sqlExpander.className = 'chunks-expander hidden sql-expander';

      sqlExpander.innerHTML = sqlList.map((item, index) => {
        const fileName = item.file_name || item.file || `Query ${index + 1}`;
        const sqlText = item.sql || '';
        const tableName = item.table_name || item.table || '';

        return `
          <div class="sql-label">File: ${escapeHtml(fileName)}</div>
          ${tableName ? `<div class="sql-meta">Table: <span class="sql-val">${escapeHtml(tableName)}</span></div>` : ''}
          <pre class="sql-code">${escapeHtml(sqlText)}</pre>
        `;
      }).join('<hr>');

      if (msg.row_count != null) {
        sqlExpander.innerHTML += `
          <div class="sql-meta">Total rows returned: <span class="sql-val">${escapeHtml(String(msg.row_count))}</span></div>
        `;
      }

      sqlToggle.addEventListener('click', () => sqlExpander.classList.toggle('hidden'));
      body.appendChild(sqlExpander);
    }
  }

  row.appendChild(av);
  row.appendChild(body);
  return row;
}

function addMessage(msg) {
  state.messages.push(msg);
  if (emptyState.parentNode) emptyState.parentNode.removeChild(emptyState);
  chatThread.appendChild(buildMsgEl(msg));
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
  chatThread.appendChild(typingEl);
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

  const selected = state.selectedFiles || [];

  if (selected.length === 1) {
    fileEl.textContent = selected[0];
    banner.classList.remove('hidden');
  } else if (selected.length > 1) {
    fileEl.textContent = `${selected.length} files selected: ${selected.join(', ')}`;
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

  // Block chat if any selected file is not ready yet
  const selectedForChat = state.selectedFiles || [];

  for (const fname of selectedForChat) {
    const statusInfo = state.fileStatuses[fname];
    if (statusInfo && !statusInfo.ready && statusInfo.status !== 'not_found') {
      toast(`⏳ ${fname} is still processing. ${statusInfo.message || 'Please wait.'}`, 'warning');
      return;
    }
  }

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
    const selectedForChat = state.selectedFiles || [];

    const r = await apiPost('/chat', {
      question:     q,
      file_name:    selectedForChat.length === 1 ? selectedForChat[0] : null,
      file_names:   selectedForChat.length > 1 ? selectedForChat : null,
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
    let answer = data.answer || data.response || data.result || data.message || data.text || 'No answer returned.';

    const hasStructuredTable =
      Array.isArray(data.rows) &&
      data.rows.length > 0 &&
      Array.isArray(data.columns) &&
      data.columns.length > 0;

    if (hasStructuredTable) {
      answer = `Found ${data.rows.length} result${data.rows.length > 1 ? 's' : ''}. See the table below.`;
    }

    addMessage({
      role:'assistant',
      content: answer,
      ts: tsAns,
      model:      data.model || data.model_used || '',
      sources:    data.sources    || [],
      chunks:     data.chunks     || [],
      sql:        data.sql        || data.generated_sql || null,
      table_name: data.table_name || null,
      row_count:  data.row_count  ?? null,
      file_type:  data.file_type  || null,
      rows:          data.rows || [],
      columns:       data.columns || [],
      multi_results: data.multi_results || [],
    });
  } else {
    addMessage({
      role:'assistant',
      content: '❌ ' + getApiErrorMessage(data, `Server error (${status})`),
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
  const files = Array.from(e.dataTransfer.files || []);
  if (files.length) setFiles(files);
});
uploadInput.addEventListener('change', () => {
  const files = Array.from(uploadInput.files || []);
  if (files.length) setFiles(files);
});

function setFiles(files) {
  state.pendingFiles = files;
  state.pendingFile = files.length === 1 ? files[0] : null;

  if (files.length === 1) {
    uploadName.textContent = `${files[0].name} · ${fmtSize(files[0].size)}`;
  } else {
    const totalSize = files.reduce((sum, f) => sum + (f.size || 0), 0);
    uploadName.textContent = `${files.length} files selected · ${fmtSize(totalSize)}`;
  }

  uploadPending.classList.remove('hidden');
}

function setFile(file) {
  setFiles([file]);
}

ingestBtn.addEventListener('click', async () => {
  const files = state.pendingFiles || [];
  if (!files.length) return;

  const formData = new FormData();
  const isBatch = files.length > 1;

  if (isBatch) {
    files.forEach(file => formData.append('files', file));
  } else {
    formData.append('file', files[0]);
  }

  showProgress(true);
  ingestBtn.disabled = true;

  const { data, status } = await apiPost(isBatch ? '/upload/batch' : '/upload', formData, true);

  showProgress(false);
  ingestBtn.disabled = false;

  if (status === 200) {
    const results = isBatch ? (data.results || []) : [data];

    let successCount = 0;
    let duplicateCount = 0;
    let errorCount = 0;

    results.forEach(result => {
      const resultStatus = result.status;
      const fileName = result.file || 'Unknown file';

      if (resultStatus === 'success') {
        successCount += 1;
        const isStruct = result.file_type === 'structured';

        state.fileStatuses[fileName] = {
          status: 'sqs_queued',
          ready: false,
          message: isStruct
            ? 'Structured file queued for Glue processing…'
            : 'File queued for ECS background processing…',
          row_count: null,
        };

        startStatusPolling(fileName);
      } else if (resultStatus === 'duplicate') {
        duplicateCount += 1;
      } else if (resultStatus === 'retry_required') {
        errorCount += 1;
      } else {
        errorCount += 1;
      }
    });

    if (isBatch) {
      toast(`📤 Batch upload finished: ${successCount} queued, ${duplicateCount} duplicate, ${errorCount} failed/retry.`, errorCount ? 'warning' : 'success');
    } else {
      const result = results[0] || {};
      const fileName = result.file || files[0].name;

      if (result.status === 'success') {
        toast(`📤 ${fileName} uploaded and queued for background processing…`, 'info');
      } else if (result.status === 'duplicate') {
        toast(result.message || 'Duplicate file already uploaded by this user', 'warning');
      } else {
        toast(result.message || 'Upload failed', 'error');
      }
    }

    state.pendingFile = null;
    state.pendingFiles = [];
    uploadPending.classList.add('hidden');
    uploadInput.value = '';

    await loadFiles();
    await loadHealthStats();
  } else {
    const message = getApiErrorMessage(data, 'Upload failed');

    if (message.toLowerCase().includes('duplicate')) {
      toast(message, 'warning');
    } else {
      toast('Upload failed: ' + message, 'error');
    }
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
    if ((state.selectedFiles || []).includes(name)) {
      state.selectedFiles = state.selectedFiles.filter(f => f !== name);
      state.selectedFile = state.selectedFiles.length === 1 ? state.selectedFiles[0] : null;
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
  state.selectedFiles = [];
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
    errEl.textContent = getApiErrorMessage(data, 'Login failed.');
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
    errEl.textContent = getApiErrorMessage(data, 'Registration failed.');
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