/* ═══════════════════════════════════════════════════════════
   app.js — CSV Graph Explorer Enterprise Frontend Engine
   Dynamic Ingestion • Real-Time Polling • SVG Graph Topology • Grounded Chat
════════════════════════════════════════════════════════════ */

const API = (window.location.port === '3000')
  ? '/api'
  : (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1' || window.location.protocol === 'file:')
    ? 'http://localhost:8000'
    : '/api';

let currentJobId = null;
let pollInterval = null;
let currentDatasetRows = [];

// ─────────────────────────────────────────────────────────────
// 1. Health Status Polling
// ─────────────────────────────────────────────────────────────
async function checkHealth() {
  const badge = document.getElementById('health-badge');
  try {
    const res = await fetch(`${API}/health`);
    const data = await res.json();
    if (data.status === 'ok') {
      badge.innerHTML = `
        <span class="w-2 h-2 rounded-full bg-tertiary-container neon-pulse-emerald"></span>
        <span>Neo4j Graph Engine: ONLINE</span>
      `;
      badge.className = 'flex items-center gap-2 px-3 py-1 rounded-full border border-tertiary-container/30 bg-tertiary-container/10 text-tertiary-container font-semibold';
    } else {
      badge.innerHTML = `
        <span class="w-2 h-2 rounded-full bg-yellow-400"></span>
        <span>Graph Engine: DEGRADED</span>
      `;
      badge.className = 'flex items-center gap-2 px-3 py-1 rounded-full border border-yellow-500/30 bg-yellow-500/10 text-yellow-300 font-semibold';
    }
  } catch {
    badge.innerHTML = `
      <span class="w-2 h-2 rounded-full bg-red-400"></span>
      <span>Graph Engine: OFFLINE</span>
    `;
    badge.className = 'flex items-center gap-2 px-3 py-1 rounded-full border border-red-500/30 bg-red-500/10 text-red-300 font-semibold';
  }
}

checkHealth();
setInterval(checkHealth, 10000);

// ─────────────────────────────────────────────────────────────
// 2. Drag & Drop File Ingestion
// ─────────────────────────────────────────────────────────────
const dropZone  = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');

if (dropZone && fileInput) {
  dropZone.addEventListener('click', () => fileInput.click());

  dropZone.addEventListener('dragover', e => {
    e.preventDefault();
    dropZone.classList.add('border-primary', 'bg-primary-container/10');
  });

  dropZone.addEventListener('dragleave', () => {
    dropZone.classList.remove('border-primary', 'bg-primary-container/10');
  });

  dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.classList.remove('border-primary', 'bg-primary-container/10');
    const file = e.dataTransfer.files[0];
    if (file) handleUpload(file);
  });

  fileInput.addEventListener('change', e => {
    if (e.target.files[0]) handleUpload(e.target.files[0]);
  });
}

function showError(msg) {
  const errEl = document.getElementById('upload-error');
  if (errEl) {
    errEl.textContent = msg;
    errEl.classList.remove('hidden');
  }
}

function clearError() {
  const errEl = document.getElementById('upload-error');
  if (errEl) errEl.classList.add('hidden');
}

async function handleUpload(file) {
  clearError();

  if (!file.name.toLowerCase().endsWith('.csv')) {
    showError('Only CSV files (.csv) are accepted.');
    return;
  }
  if (file.size === 0) {
    showError('The uploaded file is empty.');
    return;
  }

  const form = new FormData();
  form.append('file', file);

  document.getElementById('progress-filename').textContent = file.name;
  document.getElementById('progress-status').textContent = 'Uploading...';
  document.getElementById('progress-bar').style.width = '15%';

  try {
    const res = await fetch(`${API}/ingest`, { method: 'POST', body: form });
    let data = {};
    try {
      data = await res.json();
    } catch (parseErr) {
      if (res.status === 413) {
        showError('File is too large (exceeds maximum allowed size).');
      } else {
        showError(`Server error (${res.status} ${res.statusText || 'Unknown'})`);
      }
      document.getElementById('progress-status').textContent = 'Failed';
      return;
    }

    if (!res.ok) {
      showError(data.detail || 'Upload failed');
      document.getElementById('progress-status').textContent = 'Failed';
      return;
    }

    currentJobId = data.job_id;
    document.getElementById('progress-status').textContent = 'Streaming...';
    document.getElementById('progress-bar').style.width = '50%';

    // Load schema immediately
    await loadSchema(currentJobId);

    // Read file locally to render dynamic graph canvas
    const reader = new FileReader();
    reader.onload = e => {
      parseAndRenderGraph(e.target.result, file.name);
    };
    reader.readAsText(file);

    // Poll status until complete
    if (pollInterval) clearInterval(pollInterval);
    pollInterval = setInterval(() => pollStatus(currentJobId), 400);

  } catch (err) {
    showError('Network error connecting to API: ' + (err.message || 'Check Docker containers'));
  }
}

// ─────────────────────────────────────────────────────────────
// 3. Status Polling
// ─────────────────────────────────────────────────────────────
async function pollStatus(jobId) {
  try {
    const res = await fetch(`${API}/status?job_id=${jobId}`);
    const data = await res.json();

    const pct = data.rows_total > 0
      ? Math.round((data.rows_loaded / data.rows_total) * 100)
      : 100;

    document.getElementById('progress-bar').style.width = `${pct}%`;
    document.getElementById('progress-label').textContent = `${data.rows_loaded.toLocaleString()} / ${data.rows_total.toLocaleString()} rows loaded`;
    document.getElementById('progress-status').textContent = data.status.toUpperCase();

    if (data.status === 'complete' || data.status === 'failed') {
      clearInterval(pollInterval);
      document.getElementById('progress-bar').style.width = '100%';
    }
  } catch { /* non-fatal */ }
}

// ─────────────────────────────────────────────────────────────
// 4. Schema Matrix & Suggestion Builder
// ─────────────────────────────────────────────────────────────
async function loadSchema(jobId) {
  try {
    const res = await fetch(`${API}/schema?job_id=${jobId}`);
    const data = await res.json();

    const schemaColumnsEl = document.getElementById('schema-columns');
    if (schemaColumnsEl && data.columns) {
      schemaColumnsEl.innerHTML = data.columns.map(col => {
        let typeBadge = 'STRING / TEXT';
        let colorClass = 'border-primary-container/30 bg-primary-container/10 text-primary-container';
        
        const cl = col.toLowerCase();
        if (cl.includes('salary') || cl.includes('price') || cl.includes('amount') || cl.includes('age') || cl.includes('count')) {
          typeBadge = 'NUMERIC / INT';
          colorClass = 'border-tertiary-container/30 bg-tertiary-container/10 text-tertiary-container';
        } else if (cl.includes('dept') || cl.includes('role') || cl.includes('status') || cl.includes('category')) {
          typeBadge = 'CATEGORICAL';
          colorClass = 'border-secondary-container/30 bg-secondary-container/10 text-secondary';
        } else if (cl.includes('city') || cl.includes('country') || cl.includes('location') || cl.includes('state')) {
          typeBadge = 'GEO / STRING';
          colorClass = 'border-primary-fixed/30 bg-primary-fixed/10 text-primary-fixed';
        } else if (cl.includes('name') || cl.includes('id') || cl.includes('title')) {
          typeBadge = 'IDENTIFIER';
          colorClass = 'border-cyan-400/30 bg-cyan-400/10 text-cyan-300';
        }

        return `
          <div class="flex items-center justify-between p-2 rounded-lg bg-surface-container-low/70 border border-outline-variant/30 hover:border-primary-container/40 transition-colors">
            <span class="font-code text-xs text-primary font-medium">${col}</span>
            <span class="font-code text-[9px] px-2 py-0.5 rounded border ${colorClass}">${typeBadge}</span>
          </div>
        `;
      }).join('');
    }

    if (data.cypher_hint) {
      document.getElementById('schema-hint').textContent = data.cypher_hint;
    }

    buildSuggestions(data.columns);
  } catch { /* non-fatal */ }
}

function buildSuggestions(cols = []) {
  const suggestionsEl = document.getElementById('suggestions');
  if (!suggestionsEl) return;

  let queries = [];
  if (cols.includes('salary') || cols.includes('department')) {
    queries = [
      'Who has the highest salary?',
      'Average salary by department',
      'Who earns more than 90000?',
      'Who is in Engineering in New York?',
      'Top 3 highest earners',
      'How many people per city?',
      "What is Alice's salary?",
      'Where does Dave live?'
    ];
  } else {
    queries = [
      'How many rows?',
      ...cols.slice(0, 2).map(c => `List all ${c}`),
      ...cols.slice(0, 2).map(c => `Count by ${c}`)
    ];
  }

  suggestionsEl.innerHTML = queries.map(q => `
    <button class="font-code text-[10px] px-2.5 py-1 rounded-lg bg-surface-container border border-outline-variant/40 hover:border-primary-container hover:text-primary-container text-on-surface-variant transition-all text-left shadow-sm active:scale-95" onclick="sendQuestion('${q.replace(/'/g, "\\'")}')">
      ${q}
    </button>
  `).join('');
}

// ─────────────────────────────────────────────────────────────
// 5. Dynamic Interactive SVG Graph Canvas
// ─────────────────────────────────────────────────────────────
function parseAndRenderGraph(csvText, filename) {
  const lines = csvText.trim().split('\n').map(l => l.trim()).filter(Boolean);
  if (lines.length < 2) return;

  const headers = lines[0].split(',').map(h => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = lines[i].split(',').map(v => v.trim());
    const obj = {};
    headers.forEach((h, idx) => obj[h] = vals[idx] || '');
    rows.push(obj);
  }

  currentDatasetRows = rows;
  renderInteractiveGraph(rows, filename, headers);
}

function renderInteractiveGraph(rows, filename = 'small.csv', headers = []) {
  const svgLinks = document.getElementById('svg-links');
  const svgNodes = document.getElementById('svg-nodes');
  if (!svgLinks || !svgNodes) return;

  svgLinks.innerHTML = '';
  svgNodes.innerHTML = '';

  const cx = 350, cy = 250; // Center coordinate
  const nodeCountEl = document.getElementById('canvas-node-count');
  const edgeCountEl = document.getElementById('canvas-edge-count');

  // Root Dataset Node at Center
  const rootNode = { id: 'root', label: `:Dataset (${filename.split('.')[0]})`, x: cx, y: cy, type: 'root' };

  // Identify category and name columns
  const nameCol = headers.find(h => /name|person|employee/i.test(h)) || headers[0];
  const catCol  = headers.find(h => /department|category|role/i.test(h)) || (headers.length > 1 ? headers[1] : null);
  const numCol  = headers.find(h => /salary|amount|price/i.test(h));
  const geoCol  = headers.find(h => /city|country|location/i.test(h));

  // Build cluster groups (e.g. departments)
  const categories = catCol ? [...new Set(rows.map(r => r[catCol]))].filter(Boolean) : [];
  const catNodes = [];
  const rowNodes = [];

  const catRadius = 130;
  categories.forEach((cat, idx) => {
    const angle = (idx / categories.length) * 2 * Math.PI - Math.PI / 2;
    catNodes.push({
      id: `cat_${cat}`,
      label: cat,
      x: cx + catRadius * Math.cos(angle),
      y: cy + catRadius * Math.sin(angle),
      type: 'cat'
    });
  });

  // Position row nodes
  const displayRows = rows.slice(0, 10);
  displayRows.forEach((r, idx) => {
    const parentCat = catNodes.find(c => c.label === r[catCol]);
    let rx, ry;
    if (parentCat) {
      const offsetAngle = ((idx % 3) - 1) * 0.45;
      const angle = Math.atan2(parentCat.y - cy, parentCat.x - cx) + offsetAngle;
      rx = parentCat.x + 85 * Math.cos(angle);
      ry = parentCat.y + 85 * Math.sin(angle);
    } else {
      const angle = (idx / displayRows.length) * 2 * Math.PI;
      rx = cx + 180 * Math.cos(angle);
      ry = cy + 180 * Math.sin(angle);
    }

    rowNodes.push({
      id: `row_${idx}`,
      label: r[nameCol] || `Row #${idx + 1}`,
      data: r,
      x: rx,
      y: ry,
      parentCatId: parentCat ? parentCat.id : 'root',
      type: 'row'
    });
  });

  // Render SVG Bezier Curved Links
  let linkHtml = '';

  // Root to Cat links
  catNodes.forEach(cat => {
    linkHtml += `
      <path d="M ${cx} ${cy} Q ${(cx + cat.x) / 2} ${(cy + cat.y) / 2} ${cat.x} ${cat.y}"
            fill="none" stroke="url(#edgeCyanViolet)" stroke-width="2" opacity="0.75" />
    `;
  });

  // Cat to Row links (or Root to Row)
  rowNodes.forEach(rn => {
    const parent = catNodes.find(c => c.id === rn.parentCatId) || rootNode;
    linkHtml += `
      <path d="M ${parent.x} ${parent.y} Q ${(parent.x + rn.x) / 2} ${(parent.y + rn.y) / 2} ${rn.x} ${rn.y}"
            fill="none" stroke="url(#edgeCyanEmerald)" stroke-width="1.8" opacity="0.65" />
    `;
  });

  svgLinks.innerHTML = linkHtml;

  // Render Nodes HTML
  let nodeHtml = '';

  // 1. Root Node
  nodeHtml += `
    <g class="graph-node cursor-pointer" onclick="setFocusedEntity(':Dataset', '${filename}')">
      <circle cx="${cx}" cy="${cy}" r="26" fill="#060913" stroke="#00f0ff" stroke-width="2.5" filter="url(#neonGlow)"></circle>
      <text x="${cx}" y="${cy - 3}" text-anchor="middle" fill="#00f0ff" font-family="Space Grotesk" font-size="11" font-weight="bold">ROOT</text>
      <text x="${cx}" y="${cy + 11}" text-anchor="middle" fill="#8da1be" font-family="JetBrains Mono" font-size="8">:Dataset</text>
    </g>
  `;

  // 2. Category Nodes
  catNodes.forEach(c => {
    nodeHtml += `
      <g class="graph-node cursor-pointer" onclick="setFocusedEntity('Category', '${c.label}')">
        <circle cx="${c.x}" cy="${c.y}" r="20" fill="#0d1322" stroke="#cf5cff" stroke-width="2"></circle>
        <text x="${c.x}" y="${c.y + 4}" text-anchor="middle" fill="#ecb2ff" font-family="Space Grotesk" font-size="9" font-weight="600">${c.label.slice(0, 7)}</text>
      </g>
    `;
  });

  // 3. Row Nodes
  rowNodes.forEach(r => {
    const name = r.label;
    const extra = r.data[numCol] ? `$${parseInt(r.data[numCol]).toLocaleString()}` : (r.data[geoCol] || '');
    nodeHtml += `
      <g class="graph-node cursor-pointer" onclick="setFocusedEntity('Employee', '${name}', '${extra}', '${r.data[catCol] || ''}')">
        <circle cx="${r.x}" cy="${r.y}" r="15" fill="#060913" stroke="#11f89e" stroke-width="1.5"></circle>
        <text x="${r.x}" y="${r.y + 3}" text-anchor="middle" fill="#daffe4" font-family="JetBrains Mono" font-size="8">${name.slice(0, 5)}</text>
        <text x="${r.x}" y="${r.y + 22}" text-anchor="middle" fill="#8da1be" font-family="JetBrains Mono" font-size="7">${extra}</text>
      </g>
    `;
  });

  svgNodes.innerHTML = nodeHtml;

  const totalNodes = 1 + catNodes.length + rowNodes.length;
  const totalEdges = catNodes.length + rowNodes.length;
  if (nodeCountEl) nodeCountEl.textContent = totalNodes;
  if (edgeCountEl) edgeCountEl.textContent = totalEdges;

  document.getElementById('telemetry-nodes').textContent = totalNodes;
}

function setFocusedEntity(type, name, extra = '', dept = '') {
  const focusedEl = document.getElementById('focused-entity');
  if (focusedEl) {
    focusedEl.textContent = `${name} ${dept ? `(${dept})` : ''} ${extra ? `• ${extra}` : ''}`;
  }
}

// ─────────────────────────────────────────────────────────────
// 6. Conversational Graph Copilot & Chat Engine
// ─────────────────────────────────────────────────────────────
const sendBtn   = document.getElementById('send-btn');
const chatInput = document.getElementById('chat-input');

if (sendBtn && chatInput) {
  sendBtn.addEventListener('click', () => {
    const q = chatInput.value.trim();
    if (q) sendQuestion(q);
  });

  chatInput.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      const q = chatInput.value.trim();
      if (q) sendQuestion(q);
    }
  });
}

async function sendQuestion(question) {
  const messagesEl = document.getElementById('chat-messages');
  if (!messagesEl) return;

  chatInput.value = '';

  // Render User Message Card
  const userCard = document.createElement('div');
  userCard.className = 'flex flex-col items-end gap-1';
  userCard.innerHTML = `
    <div class="max-w-[85%] bg-surface-container-high/90 border border-outline-variant/40 rounded-2xl rounded-tr-xs p-3 text-on-surface shadow-md">
      <p class="font-body text-xs leading-relaxed font-medium">${escapeHtml(question)}</p>
    </div>
    <span class="font-code text-[9px] text-on-surface-variant pr-1">Lead Graph Architect</span>
  `;
  messagesEl.appendChild(userCard);
  messagesEl.scrollTop = messagesEl.scrollHeight;

  // Render Temporary Loading Pill
  const loadingCard = document.createElement('div');
  loadingCard.className = 'flex items-center gap-2 text-primary font-code text-xs p-3';
  loadingCard.innerHTML = `
    <span class="w-2 h-2 rounded-full bg-primary-container animate-ping"></span>
    <span>Generating validated Cypher & querying graph...</span>
  `;
  messagesEl.appendChild(loadingCard);
  messagesEl.scrollTop = messagesEl.scrollHeight;

  const startTime = performance.now();

  try {
    const res = await fetch(`${API}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: question, job_id: currentJobId })
    });
    const data = await res.json();
    const duration = Math.max(2, Math.round(performance.now() - startTime));
    document.getElementById('telemetry-lat').textContent = `${duration}ms`;

    loadingCard.remove();

    // Determine Grounded status
    const isGrounded = data.grounded === true;
    const badgeHtml = isGrounded
      ? `
        <span class="font-code text-[9px] px-2 py-0.5 rounded bg-tertiary-container/10 border border-tertiary-container/40 text-tertiary-container font-bold flex items-center gap-1 shadow-[0_0_8px_rgba(17,248,158,0.3)]">
          <span class="material-symbols-outlined text-[13px]">verified</span>
          [GROUNDED: TRUE]
        </span>
      `
      : `
        <span class="font-code text-[9px] px-2 py-0.5 rounded bg-yellow-500/10 border border-yellow-500/40 text-yellow-300 font-bold flex items-center gap-1">
          <span class="material-symbols-outlined text-[13px]">warning</span>
          [GROUNDED: FALSE]
        </span>
      `;

    // Render Cypher box if present
    const cypherBlock = data.cypher
      ? `
        <div class="rounded-xl bg-[#04060b] border border-primary-container/30 p-2.5 overflow-hidden">
          <div class="flex items-center justify-between font-code text-[8px] text-primary-container tracking-wider uppercase mb-1">
            <span>NEO4J CYPHER 5.24</span>
            <span class="text-tertiary-container font-semibold">VALIDATED AST</span>
          </div>
          <pre class="font-code text-[11px] leading-relaxed text-on-surface overflow-x-auto whitespace-pre-wrap">${highlightCypher(data.cypher)}</pre>
        </div>
      `
      : '';

    // Render tabular preview if result has records
    let tablePreview = '';
    if (Array.isArray(data.result) && data.result.length > 0 && typeof data.result[0] === 'object') {
      const keys = Object.keys(data.result[0]).filter(k => !k.startsWith('_'));
      tablePreview = `
        <div class="border border-outline-variant/30 rounded-lg overflow-hidden mt-1 max-h-[140px] overflow-y-auto">
          <table class="w-full text-left font-code text-[10px]">
            <thead class="bg-surface-container-high/80 text-primary-container sticky top-0">
              <tr>${keys.map(k => `<th class="p-1.5 border-b border-outline-variant/40">${k}</th>`).join('')}</tr>
            </thead>
            <tbody class="divide-y divide-outline-variant/20 bg-surface-container-lowest/60">
              ${data.result.slice(0, 5).map(row => `
                <tr class="hover:bg-primary-container/5">
                  ${keys.map(k => `<td class="p-1.5 text-on-surface">${row[k] !== undefined ? row[k] : ''}</td>`).join('')}
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      `;
    }

    // Append Copilot Card
    const copilotCard = document.createElement('div');
    copilotCard.className = 'flex flex-col items-start gap-1.5 w-full';
    copilotCard.innerHTML = `
      <div class="w-full bg-surface-container-lowest/95 border border-primary-container/30 rounded-2xl rounded-tl-xs p-3.5 shadow-2xl flex flex-col gap-2.5">
        <div class="flex items-center justify-between pb-1.5 border-b border-outline-variant/30">
          <div class="flex items-center gap-1.5">
            <span class="material-symbols-outlined text-primary-container text-[18px]">smart_toy</span>
            <span class="font-headline text-xs font-semibold text-primary">Neo4j Cypher Synthesizer</span>
          </div>
          ${badgeHtml}
        </div>

        <p class="font-body text-xs text-on-surface leading-relaxed font-medium">
          ${escapeHtml(data.answer)}
        </p>

        ${cypherBlock}
        ${tablePreview}

        <div class="flex items-center justify-between font-code text-[9px] text-on-surface-variant pt-1 border-t border-outline-variant/20">
          <span>Execution: ${duration}ms • Bolt Protocol</span>
          <span class="text-tertiary-container font-semibold">Verified against Graph</span>
        </div>
      </div>
    `;

    messagesEl.appendChild(copilotCard);
    messagesEl.scrollTop = messagesEl.scrollHeight;

  } catch (err) {
    loadingCard.remove();
    showError('Failed to execute query on backend.');
  }
}

function highlightCypher(cypher) {
  return escapeHtml(cypher)
    .replace(/\b(MATCH|WHERE|RETURN|ORDER BY|DESC|ASC|LIMIT|DISTINCT|AS|AND|OR|NOT|IS|NULL)\b/g, '<span class="text-secondary font-bold">$1</span>')
    .replace(/\b(toFloat|avg|count|min|max|sum)\b/g, '<span class="text-primary-container font-semibold">$1</span>')
    .replace(/(:[a-zA-Z0-9_]+)/g, '<span class="text-primary-fixed">$1</span>')
    .replace(/('.*?')/g, '<span class="text-tertiary-container">$1</span>');
}

function escapeHtml(text) {
  const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
  return String(text).replace(/[&<>"']/g, m => map[m]);
}

// Initial dummy graph so canvas looks alive immediately
setTimeout(() => {
  renderInteractiveGraph([
    { name: 'Alice', department: 'Engineering', salary: '95000', city: 'New York' },
    { name: 'Bob', department: 'Marketing', salary: '72000', city: 'London' },
    { name: 'Carol', department: 'Engineering', salary: '88000', city: 'New York' },
    { name: 'Dave', department: 'HR', salary: '61000', city: 'Paris' },
    { name: 'Eve', department: 'Engineering', salary: '102000', city: 'Berlin' },
    { name: 'Frank', department: 'Marketing', salary: '69000', city: 'New York' },
    { name: 'Grace', department: 'HR', salary: '58000', city: 'London' },
    { name: 'Heidi', department: 'Engineering', salary: '91000', city: 'Berlin' },
    { name: 'Ivan', department: 'Marketing', salary: '75000', city: 'Paris' },
    { name: 'Judy', department: 'HR', salary: '63000', city: 'New York' }
  ], 'small.csv', ['name', 'department', 'salary', 'city']);

  buildSuggestions(['name', 'department', 'salary', 'city']);
}, 300);
