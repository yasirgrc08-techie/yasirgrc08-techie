import { ResumableUpload, TransferError, STORAGE_PREFIX, fileFingerprint } from './uploader-core.js';

const $ = (sel, root = document) => root.querySelector(sel);

// ---------- browser adapters for the uploader ----------

function xhrRequest(method, url, { headers = {}, body, timeoutMs = 30_000, stallTimeoutMs, onProgress, signal } = {}) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new TransferError('Aborted', { kind: 'aborted' }));
    const xhr = new XMLHttpRequest();
    xhr.open(method, url);
    for (const [k, v] of Object.entries(headers)) xhr.setRequestHeader(k, v);
    xhr.timeout = timeoutMs;

    // Detect a connection that is open but no longer moving any bytes.
    let stalled = false;
    let stallTimer = null;
    const armStall = () => {
      if (!stallTimeoutMs) return;
      clearTimeout(stallTimer);
      stallTimer = setTimeout(() => {
        stalled = true;
        xhr.abort();
      }, stallTimeoutMs);
    };
    const cleanup = () => {
      clearTimeout(stallTimer);
      signal?.removeEventListener('abort', onAbort);
    };
    const onAbort = () => xhr.abort();
    signal?.addEventListener('abort', onAbort, { once: true });

    xhr.upload.onprogress = (e) => {
      armStall();
      onProgress?.(e.loaded);
    };
    xhr.upload.onload = armStall;
    xhr.onload = () => {
      cleanup();
      let parsed = null;
      try {
        parsed = xhr.responseText ? JSON.parse(xhr.responseText) : null;
      } catch {
        parsed = { error: xhr.responseText };
      }
      // Status 0 here means the connection died without an HTTP response.
      if (xhr.status === 0) return reject(new TransferError('Network error', { kind: 'network' }));
      resolve({ status: xhr.status, body: parsed, retryAfter: xhr.getResponseHeader('Retry-After') });
    };
    xhr.onerror = () => {
      cleanup();
      reject(new TransferError('Network error', { kind: 'network' }));
    };
    xhr.ontimeout = () => {
      cleanup();
      reject(new TransferError(`No response within ${Math.round(timeoutMs / 1000)}s`, { kind: 'timeout' }));
    };
    xhr.onabort = () => {
      cleanup();
      if (stalled) reject(new TransferError(`Connection stalled for ${Math.round(stallTimeoutMs / 1000)}s`, { kind: 'timeout' }));
      else reject(new TransferError('Aborted', { kind: 'aborted' }));
    };
    armStall();
    xhr.send(body ?? null);
  });
}

const sha256 = globalThis.crypto?.subtle
  ? async (bytes) => {
      const digest = await crypto.subtle.digest('SHA-256', bytes);
      return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
    }
  : null; // crypto.subtle needs HTTPS (or localhost); without it the server still hashes chunks.

const storage = {
  get(key) {
    try {
      return JSON.parse(localStorage.getItem(key));
    } catch {
      return null;
    }
  },
  set(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch {
      // Storage full or blocked: uploads still work, they just can't resume after a reload.
    }
  },
  remove(key) {
    try {
      localStorage.removeItem(key);
    } catch {
      // ignore
    }
  },
  keys() {
    try {
      return Object.keys(localStorage).filter((k) => k.startsWith(STORAGE_PREFIX));
    } catch {
      return [];
    }
  },
};

const onlineWaiters = new Set();
function waitForOnline(signal) {
  return new Promise((resolve) => {
    if (navigator.onLine) return resolve();
    const done = () => {
      onlineWaiters.delete(done);
      resolve();
    };
    onlineWaiters.add(done);
    signal?.addEventListener('abort', done, { once: true });
  });
}

function fileSource(file) {
  return {
    name: file.name,
    size: file.size,
    lastModified: file.lastModified,
    type: file.type,
    async read(start, end) {
      return new Uint8Array(await file.slice(start, end).arrayBuffer());
    },
  };
}

// ---------- formatting ----------

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let u = -1;
  do {
    n /= 1024;
    u++;
  } while (n >= 1024 && u < units.length - 1);
  return `${n.toFixed(n < 10 ? 2 : 1)} ${units[u]}`;
}

function fmtDuration(sec) {
  if (!Number.isFinite(sec) || sec < 0) return '';
  if (sec < 60) return `${Math.ceil(sec)}s`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ${Math.round(sec % 60)}s`;
  return `${Math.floor(sec / 3600)}h ${Math.round((sec % 3600) / 60)}m`;
}

const absoluteUrl = (url) => new URL(url, location.href).href;

async function copy(text, button) {
  try {
    await navigator.clipboard.writeText(text);
    const old = button.textContent;
    button.textContent = 'Copied';
    setTimeout(() => (button.textContent = old), 1200);
  } catch {
    prompt('Copy this link:', text);
  }
}

// ---------- uploads ----------

const active = new Map(); // fingerprint -> { upload, el }
let maxFileSize = Infinity;

function startUpload(file) {
  if (file.size > maxFileSize) {
    alert(`${file.name} is ${fmtBytes(file.size)}; the server limit is ${fmtBytes(maxFileSize)}.`);
    return;
  }
  const fp = fileFingerprint(file);
  const existing = active.get(fp);
  if (existing) {
    if (existing.upload.state !== 'complete') existing.upload.start().catch(() => {});
    return;
  }

  const el = $('#uploadTpl').content.firstElementChild.cloneNode(true);
  $('.name', el).textContent = file.name;
  $('#uploads').prepend(el);
  $('#uploadSection').hidden = false;

  const view = { el, lastBytes: null, lastTime: 0, speed: 0, frame: 0, snap: null };
  const upload = new ResumableUpload({
    source: fileSource(file),
    endpoint: '',
    http: { request: xhrRequest },
    sha256,
    storage,
    chunkSize: Number($('#chunkSize').value),
    concurrency: Number($('#concurrency').value),
    isOnline: () => navigator.onLine,
    waitForOnline,
    onUpdate: (snap) => {
      view.snap = snap;
      if (!view.frame) view.frame = requestAnimationFrame(() => render(view));
    },
  });
  active.set(fp, { upload, el });

  $('.pause', el).onclick = () => upload.pause();
  $('.resume', el).onclick = () => run();
  $('.cancel', el).onclick = async () => {
    if (upload.state !== 'complete' && !confirm(`Cancel uploading ${file.name}?`)) return;
    await upload.cancel();
    el.remove();
    active.delete(fp);
    if (!active.size) $('#uploadSection').hidden = true;
    renderResumeList();
  };
  $('.copy', el).onclick = (e) => copy($('.link', el).value, e.target);

  const run = () =>
    upload
      .start()
      .then((result) => {
        if (result) loadFiles();
      })
      .catch(() => {})
      .finally(renderResumeList);
  run();
  renderResumeList();
}

const STATE_TEXT = {
  idle: 'Waiting',
  preparing: 'Connecting…',
  uploading: 'Uploading',
  retrying: 'Retrying',
  offline: 'Offline. Will continue automatically when the network is back',
  finalizing: 'Finalizing…',
  complete: 'Complete',
  paused: 'Paused',
  failed: 'Failed',
  cancelled: 'Cancelled',
};

function render(view) {
  view.frame = 0;
  const s = view.snap;
  const { el } = view;
  const now = performance.now();

  if (s.state === 'uploading' && view.lastBytes !== null && now - view.lastTime > 500) {
    const inst = ((s.uploadedBytes - view.lastBytes) / (now - view.lastTime)) * 1000;
    view.speed = view.speed ? view.speed * 0.7 + Math.max(0, inst) * 0.3 : Math.max(0, inst);
    view.lastBytes = s.uploadedBytes;
    view.lastTime = now;
  } else if (view.lastBytes === null || s.state !== 'uploading') {
    view.lastBytes = s.uploadedBytes;
    view.lastTime = now;
    if (s.state !== 'uploading') view.speed = 0;
  }

  const pct = s.totalBytes ? (s.uploadedBytes / s.totalBytes) * 100 : s.state === 'complete' ? 100 : 0;
  $('.fill', el).style.width = `${pct.toFixed(2)}%`;
  el.className = `card ${s.state}`;

  let text = STATE_TEXT[s.state] || s.state;
  let tone = '';
  if (s.state === 'retrying') {
    const secs = Math.max(0, Math.ceil((s.retryAt - Date.now()) / 1000));
    text = `Retrying in ${secs}s (${s.lastError})`;
    tone = 'warn';
    // Keep the countdown ticking.
    setTimeout(() => view.snap === s && !view.frame && (view.frame = requestAnimationFrame(() => render(view))), 500);
  } else if (s.state === 'offline') tone = 'warn';
  else if (s.state === 'failed') {
    text = `Failed: ${s.lastError}`;
    tone = 'bad';
  } else if (s.state === 'complete') tone = 'ok';
  const stateEl = $('.state', el);
  stateEl.textContent = text;
  stateEl.className = `state ${tone}`;

  let numbers = `${fmtBytes(s.uploadedBytes)} / ${fmtBytes(s.totalBytes)} · ${pct.toFixed(1)}%`;
  if (s.state === 'uploading' && view.speed > 0) {
    numbers += ` · ${fmtBytes(view.speed)}/s · ${fmtDuration((s.totalBytes - s.uploadedBytes) / view.speed)} left`;
  }
  if (s.totalChunks) numbers += ` · chunk ${s.confirmedChunks}/${s.totalChunks}`;
  $('.numbers', el).textContent = numbers;

  const running = ['preparing', 'uploading', 'retrying', 'offline', 'finalizing'].includes(s.state);
  $('.pause', el).hidden = !running;
  $('.resume', el).hidden = !(s.state === 'paused' || s.state === 'failed');
  $('.cancel', el).textContent = s.state === 'complete' ? 'Dismiss' : 'Cancel';
  if (s.state === 'complete' && s.result) {
    $('.share', el).hidden = false;
    $('.link', el).value = absoluteUrl(s.result.url);
  }
}

// Uploads interrupted by a reload or closed tab: the browser can't reopen a
// file by itself, so ask the user to pick it again.
function renderResumeList() {
  const list = $('#resumeList');
  list.replaceChildren();
  for (const key of storage.keys()) {
    const fp = key.slice(STORAGE_PREFIX.length);
    if (active.has(fp)) continue;
    const saved = storage.get(key);
    if (!saved) continue;
    const li = document.createElement('li');
    li.className = 'card row';
    const name = document.createElement('div');
    name.className = 'name';
    name.textContent = `${saved.fileName} (${fmtBytes(saved.fileSize)})`;
    const actions = document.createElement('div');
    actions.className = 'actions';
    const pick = document.createElement('button');
    pick.type = 'button';
    pick.textContent = 'Select file to resume';
    pick.onclick = () => $('#picker').click();
    const forget = document.createElement('button');
    forget.type = 'button';
    forget.className = 'ghost danger';
    forget.textContent = 'Discard';
    forget.onclick = async () => {
      storage.remove(key);
      if (saved.uploadId) fetch(`/api/uploads/${saved.uploadId}`, { method: 'DELETE' }).catch(() => {});
      renderResumeList();
    };
    actions.append(pick, forget);
    li.append(name, actions);
    list.append(li);
  }
  $('#resumeSection').hidden = !list.children.length;
}

// ---------- shared files ----------

async function loadFiles() {
  let files;
  try {
    const res = await fetch('/api/files', { cache: 'no-store' });
    ({ files } = await res.json());
  } catch {
    return;
  }
  const list = $('#files');
  list.replaceChildren();
  for (const f of files) {
    const li = document.createElement('li');
    li.className = 'card file';
    const row = document.createElement('div');
    row.className = 'row';
    const info = document.createElement('div');
    info.className = 'name';
    info.textContent = f.fileName;
    const sub = document.createElement('div');
    sub.className = 'sub';
    sub.textContent = `${fmtBytes(f.fileSize)} · ${new Date(f.uploadedAt).toLocaleString()}`;
    info.append(sub);

    const actions = document.createElement('div');
    actions.className = 'actions';
    const dl = document.createElement('a');
    dl.href = f.url;
    dl.className = 'btn ghost';
    dl.textContent = 'Download';
    const cp = document.createElement('button');
    cp.type = 'button';
    cp.textContent = 'Copy link';
    cp.onclick = () => copy(absoluteUrl(f.url), cp);
    const del = document.createElement('button');
    del.type = 'button';
    del.className = 'ghost danger';
    del.textContent = 'Delete';
    del.onclick = async () => {
      if (!confirm(`Delete ${f.fileName}? The share link will stop working.`)) return;
      await fetch(`/api/files/${f.id}`, { method: 'DELETE' }).catch(() => {});
      loadFiles();
    };
    actions.append(dl, cp, del);
    row.append(info, actions);
    li.append(row);
    list.append(li);
  }
  $('#noFiles').hidden = files.length > 0;
}

// ---------- connectivity indicators ----------

function setBadge(el, ok, text) {
  el.textContent = text;
  el.className = `badge ${ok ? 'ok' : 'bad'}`;
}

function updateNetworkBadge() {
  setBadge($('#net'), navigator.onLine, navigator.onLine ? 'Online' : 'Offline');
}

let serverWasDown = false;
async function pingServer() {
  try {
    const res = await fetch('/api/health', { cache: 'no-store', signal: AbortSignal.timeout(5000) });
    const body = await res.json();
    setBadge($('#srv'), true, 'Server reachable');
    $('#chaos').hidden = !body.faultInjection;
    if (serverWasDown) {
      // Server is back (e.g. after a restart): skip any remaining back-off.
      for (const { upload } of active.values()) upload.nudge();
    }
    serverWasDown = false;
  } catch {
    setBadge($('#srv'), false, 'Server unreachable');
    serverWasDown = true;
  }
}

window.addEventListener('online', () => {
  updateNetworkBadge();
  for (const wake of [...onlineWaiters]) wake();
  for (const { upload } of active.values()) upload.nudge();
  pingServer();
});
window.addEventListener('offline', updateNetworkBadge);

window.addEventListener('beforeunload', (e) => {
  const busy = [...active.values()].some(({ upload }) => !['complete', 'cancelled', 'failed', 'paused'].includes(upload.state));
  if (busy) {
    e.preventDefault();
    e.returnValue = '';
  }
});

// ---------- file selection ----------

const drop = $('#drop');
$('#picker').addEventListener('change', (e) => {
  for (const f of e.target.files) startUpload(f);
  e.target.value = '';
});
drop.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' || e.key === ' ') {
    e.preventDefault();
    $('#picker').click();
  }
});
drop.addEventListener('dragover', (e) => {
  e.preventDefault();
  drop.classList.add('over');
});
drop.addEventListener('dragleave', () => drop.classList.remove('over'));
drop.addEventListener('drop', (e) => {
  e.preventDefault();
  drop.classList.remove('over');
  for (const f of e.dataTransfer.files) startUpload(f);
});
$('#refresh').onclick = loadFiles;

// ---------- boot ----------

updateNetworkBadge();
pingServer();
setInterval(pingServer, 5000);
loadFiles();
renderResumeList();
fetch('/api/config')
  .then((r) => r.json())
  .then((c) => {
    maxFileSize = c.maxFileSize;
    $('#limit').textContent = fmtBytes(c.maxFileSize);
  })
  .catch(() => {});
