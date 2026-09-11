/* ═══════════════════════════════════════════════════════════
   app.js — CSV Graph Explorer frontend logic
   Upload → Status polling → Schema display → Chat interface
════════════════════════════════════════════════════════════ */

const API = '/api';
let currentJobId   = null;
let pollInterval   = null;

// ─────────────────────────────────────────────────────────────
// Health badge — checks every 10 seconds
// ─────────────────────────────────────────────────────────────
async function checkHealth() {
  const badge = document.getElementById('health-badge');
  try {
    const res  = await fetch(`${API}/health`);
    const data = await res.json();
    if (data.status === 'ok') {
      badge.textContent = '🟢 Online';
      badge.className   = 'badge badge-ok';
    } else {
      badge.textContent = '🟡 Degraded';
      badge.className   = 'badge badge-degraded';
    }
  } catch {
    badge.textContent = '🔴 Offline';
    badge.className   = 'badge badge-degraded';
  }
}

checkHealth();
setInterval(checkHealth, 10000);


// ─────────────────────────────────────────────────────────────
// Drag-and-drop zone
// ─────────────────────────────────────────────────────────────
const dropZone  = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');

dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('keydown', e => { if (e.key === 'Enter') fileInput.click(); });

dropZone.addEventListener('dragover', e => {
  e.preventDefault();
  dropZone.classList.add('dragover');
});
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
dropZone.addEventListener('drop', e => {
  e.preventDefault();
  dropZone.classList.remove('dragover');
  const file = e.dataTransfer.files[0];
  if (file) handleUpload(file);
});

fileInput.addEventListener('change', e => {
  if (e.target.files[0]) handleUpload(e.target.files[0]);
});


// ─────────────────────────────────────────────────────────────
// Upload handler
// ─────────────────────────────────────────────────────────────
async function handleUpload(file) {
  clearError();

  // Client-side validation
  if (!file.name.toLowerCase().endsWith('.csv')) {
    showError('Please upload a CSV file (.csv)');
    return;
  }
  if (file.size === 0) {
    showError('File is empty');
    return;
  }

  const form = new FormData();
  form.append('file', file);

  try {
    const res  = await fetch(`${API}/ingest`, { method: 'POST', body: form });
    const data = await res.json();

    if (!res.ok) {
      showError(data.detail || 'Upload failed');
      return;
    }

    currentJobId = data.job_id;

    // Show progress
    document.getElementById('progress-section').hidden = false;
    document.getElementById('progress-filename').textContent = file.name;
    document.getElementById('progress-status').textContent  = 'Queued';

    // Load schema
    await loadSchema(currentJobId);

    // Show chat
    document.getElementById('chat-card').hidden = false;
    buildSuggestions();

    // Start polling
    if (pollInterval) clearInterval(pollInterval);
    pollInterval = setInterval(() => pollStatus(currentJobId), 500);

  } catch (err) {
    showError('Network error — is the API running?');
  }
}


// ─────────────────────────────────────────────────────────────
// Status polling
// ─────────────────────────────────────────────────────────────
async function pollStatus(jobId) {
  try {
    const res  = await fetch(`${API}/status?job_id=${jobId}`);
    const data = await res.json();

    const pct = data.rows_total > 0
      ? Math.round((data.rows_loaded / data.rows_total) * 100)
      : 0;

    document.getElementById('progress-bar').style.width   = `${pct}%`;
    document.getElementById('progress-label').textContent =
      `${data.rows_loaded.toLocaleString()} / ${data.rows_total.toLocaleString()} rows`;
    document.getElementById('progress-status').textContent =
      data.status.charAt(0).toUpperCase() + data.status.slice(1);

    const failedEl = document.getElementById('progress-failed');
    failedEl.textContent = data.rows_failed > 0
      ? `${data.rows_failed} failed`
      : '';

    if (data.status === 'complete' || data.status === 'failed') {
      clearInterval(pollInterval);
    }
  } catch { /* non-fatal */ }
}


// ─────────────────────────────────────────────────────────────
// Schema display
// ─────────────────────────────────────────────────────────────
async function loadSchema(jobId) {
  try {
    const res  = await fetch(`${API}/schema?job_id=${jobId}`);
    const data = await res.json();

    const schemaCard = document.getElementById('schema-card');
    schemaCard.hidden = false;

    const chipList = document.getElementById('schema-columns');
    chipList.innerHTML = data.columns
      .map(c => `<span class="chip">${c}</span>`)
      .join('');

    // Store columns for suggestion generation
    window._csvColumns = data.columns;
  } catch { /* non-fatal */ }
}


// ─────────────────────────────────────────────────────────────
// Chat suggestion chips
// ─────────────────────────────────────────────────────────────
function buildSuggestions() {
  const cols = window._csvColumns || [];
  const suggestionsEl = document.getElementById('suggestions');

  const base = ['How many rows?', 'What columns are available?', 'Show me some rows'];
  const colBased = cols.slice(0, 3).map(c => `List all ${c}`);
  const all = [...base, ...colBased];

  suggestionsEl.innerHTML = all.map(q =>
    `<button class="suggestion-chip" onclick="sendQuestion('${q.replace(/'/g, "\\'")}')">${q}</button>`
  ).join('');
}


// ─────────────────────────────────────────────────────────────
// Chat
// ─────────────────────────────────────────────────────────────
document.getElementById('send-btn').addEventListener('click', () => {
  const q = document.getElementById('chat-input').value.trim();
  if (q) sendQuestion(q);
});

document.getElementById('chat-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') {
    const q = document.getElementById('chat-input').value.trim();
    if (q) sendQuestion(q);
  }
});

async function sendQuestion(question) {
  document.getElementById('chat-input').value = '';
  document.getElementById('send-btn').disabled = true;

  addMessage('user', question);
  const thinkingId = addThinking();

  try {
    const res  = await fetch(`${API}/chat`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ question, job_id: currentJobId }),
    });
    const data = await res.json();

    removeThinking(thinkingId);
    addBotMessage(data);
  } catch (err) {
    removeThinking(thinkingId);
    addBotMessage({
      answer:   'Connection error — please try again.',
      cypher:   null,
      result:   [],
      grounded: false,
    });
  } finally {
    document.getElementById('send-btn').disabled = false;
    document.getElementById('chat-input').focus();
  }
}

function addMessage(role, text) {
  const messages = document.getElementById('chat-messages');
  const div = document.createElement('div');
  div.className = `msg ${role}`;
  div.innerHTML = `<div class="msg-bubble">${escHtml(text)}</div>`;
  messages.appendChild(div);
  div.scrollIntoView({ behavior: 'smooth', block: 'end' });
}

function addBotMessage(data) {
  const messages = document.getElementById('chat-messages');
  const div = document.createElement('div');
  div.className = 'msg bot';

  const groundedBadge = data.grounded
    ? '<span class="badge badge-grounded">✅ Grounded</span>'
    : '<span class="badge badge-ungrounded">⚠️ Not grounded</span>';

  let cypherSection = '';
  if (data.cypher) {
    const resultJson = JSON.stringify(data.result, null, 2);
    cypherSection = `
      <details>
        <summary>View Cypher &amp; Result</summary>
        <div class="cypher-block">
          <div class="cypher-label">Cypher Query</div>
          <pre>${escHtml(data.cypher)}</pre>
        </div>
        <div class="cypher-block result-block">
          <div class="cypher-label">Raw Result</div>
          <pre>${escHtml(resultJson)}</pre>
        </div>
      </details>`;
  }

  div.innerHTML = `
    <div class="msg-bubble">${escHtml(data.answer)}</div>
    <div class="msg-meta">${groundedBadge}</div>
    ${cypherSection}
  `;

  messages.appendChild(div);
  div.scrollIntoView({ behavior: 'smooth', block: 'end' });
}

function addThinking() {
  const messages = document.getElementById('chat-messages');
  const id  = 'thinking-' + Date.now();
  const div = document.createElement('div');
  div.className = 'msg bot';
  div.id        = id;
  div.innerHTML = `
    <div class="thinking">
      <span></span><span></span><span></span>
    </div>`;
  messages.appendChild(div);
  div.scrollIntoView({ behavior: 'smooth', block: 'end' });
  return id;
}

function removeThinking(id) {
  document.getElementById(id)?.remove();
}


// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────
function showError(msg) {
  const el = document.getElementById('upload-error');
  el.textContent = `⚠️ ${msg}`;
  el.hidden = false;
}

function clearError() {
  const el = document.getElementById('upload-error');
  el.hidden = true;
  el.textContent = '';
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
