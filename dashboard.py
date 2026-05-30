#!/usr/bin/env python3
"""
YouTube-grab pipeline dashboard.

Usage:
    python dashboard.py CONTAINER_NAME       # stream docker logs
    python dashboard.py CONTAINER --port 9000
    docker logs -f CONTAINER | python dashboard.py -   # from stdin pipe
"""

import sys, os, re, json, time, threading, subprocess, webbrowser, argparse
import urllib.request as _ureq
from collections import deque, OrderedDict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import urlparse as _urlparse

PORT = 8080

STAGES = [
    'CheckIP', 'GetItemFromTracker', 'PrepareDirectories', 'SetCookies',
    'WgetDownload', 'SetBadUrls', 'PrepareStatsForTracker', 'MoveFiles',
    'UploadWithTracker', 'SendDoneToTracker',
]
STAGE_IDX = {s: i for i, s in enumerate(STAGES)}
_UPLOAD_STAGE_IDX = STAGE_IDX['PrepareStatsForTracker']  # stages >= this are upload phase

# ── Settings ───────────────────────────────────────────────────────────────────
_SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              'dashboard_config.json')
_settings = {'concurrent': 2, 'downloader': '', 'rsync_jobs': 2}


def _load_settings():
    try:
        with open(_SETTINGS_FILE) as f:
            _settings.update(json.load(f))
    except Exception:
        pass
    _settings.setdefault('concurrent', int(os.environ.get('CONCURRENT_ITEMS', 2)))
    _settings.setdefault('downloader', os.environ.get('DOWNLOADER', ''))
    _settings.setdefault('rsync_jobs', 2)


def _save_settings(data):
    """Save settings to file; try to push rsync_jobs to seesaw live."""
    for k in ('concurrent', 'downloader', 'rsync_jobs'):
        if k in data:
            _settings[k] = data[k]
    try:
        with open(_SETTINGS_FILE, 'w') as f:
            json.dump(_settings, f, indent=2)
    except Exception as e:
        return {'ok': False, 'error': str(e), 'live': False}

    live = False
    if 'rsync_jobs' in data:
        try:
            req = _ureq.Request(
                'http://localhost:8001/api/config/shared:rsync_threads',
                data=f'value={data["rsync_jobs"]}'.encode(),
                method='POST',
            )
            _ureq.urlopen(req, timeout=2)
            live = True
        except Exception:
            pass
    return {'ok': True, 'live': live}


# ── Shared state ───────────────────────────────────────────────────────────────
state = {
    'items'       : OrderedDict(),   # key: item_name.lower()
    'completed'   : 0,
    'failed'      : 0,
    'upload_speed': None,
    'dl_speed'    : None,
    'logs'        : deque(maxlen=400),
    'started_at'  : time.time(),
    '_cur_key'    : None,
    '_color_ctr'  : 0,
}
lock = threading.Lock()
sse_clients = []
sse_lock = threading.Lock()


def ts():
    return datetime.now().strftime('%H:%M:%S')


def broadcast(data):
    msg = 'data: ' + json.dumps(data) + '\n\n'
    with sse_lock:
        dead = []
        for q in sse_clients:
            try:
                q.append(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            try:
                sse_clients.remove(q)
            except ValueError:
                pass


# ── Log parser ─────────────────────────────────────────────────────────────────

_DOCKER_TS = re.compile(r'^\d{4}-\d{2}-\d{2}T[\d:.]+Z ')
_ITEM_RE   = re.compile(r'\b(v\d*:[0-9a-zA-Z_\-]{6,12})\b')

# Any speed token — stage context decides upload vs download attribution
_SPEED_RE  = re.compile(r'([\d.]+\s*[KMGkm]i?B/s)', re.I)

# Completion signals — broaden this list if your seesaw version differs:
#   "Item v:ID done."        seesaw pipeline runner
#   "Sending done to tracker" SendDoneToTracker pre-request
#   "item done"              shorter seesaw form
#   "total size is 1,234"   rsync final summary → WARC upload complete
_DONE_RE = re.compile(
    r'Item\s+\S+\s+done\b'
    r'|\bitem\s+done\b'
    r'|Sending done.*?tracker'
    r'|total size is\s+[\d,]',
    re.I,
)

# Failure signals:
#   "Item v:abc is aborted."  SetBadUrls (lowercases the name)
#   "Download failed"         WgetDownload non-zero exit
_FAIL_RE = re.compile(r'\bis aborted\b|Download failed', re.I)


def _detect_substage(line):
    if re.search(r'getting more comments', line, re.I): return 'paginating comments'
    if re.search(r'getting comments',      line, re.I): return 'getting comments'
    if re.search(r'getting replies from',  line, re.I): return 'getting replies'
    if re.search(r'Using cached player js',line, re.I): return 'player JS (cached ✓)'
    if re.search(r'Using player js url',   line, re.I): return 'fetching player JS'
    if re.search(r'Decrypted n\b',         line):       return 'decrypting n-param'
    if re.search(r'Decrypted sig\b',       line):       return 'decrypting signature'
    if re.search(r'found encrypted sig',   line, re.I): return 'sig decrypt…'
    if re.search(r'Video is not playable', line, re.I): return 'not playable'
    if re.search(r'comments turned off',   line, re.I): return 'comments off'
    return None


def _oldest_active():
    for item in state['items'].values():
        if item['status'] == 'active':
            return item
    return None


def parse_line(raw: str):
    line = raw.rstrip('\n\r')
    if not line:
        return
    line = _DOCKER_TS.sub('', line)
    now = ts()
    changed = False

    with lock:
        # Item detection — lowercase key for SetBadUrls compatibility
        m = _ITEM_RE.search(line)
        if m:
            raw_name = m.group(1)
            key = raw_name.lower()
            if key not in state['items']:
                ci = state['_color_ctr']
                state['_color_ctr'] += 1
                state['items'][key] = {
                    'name'     : raw_name,
                    'key'      : key,
                    'stage'    : '',
                    'stage_idx': -1,
                    'substage' : '',
                    'started'  : now,
                    'status'   : 'active',
                    'ci'       : ci,
                }
                changed = True
            state['_cur_key'] = key

        cur = state['items'].get(state['_cur_key']) if state['_cur_key'] else None

        log_entry = {
            't'   : now,
            'l'   : line,
            'item': state['_cur_key'],
            'name': cur['name'] if cur else None,
            'ci'  : cur['ci'] if cur else None,
        }
        state['logs'].append(log_entry)

        # Pipeline stage
        for stage in STAGES:
            if re.search(r'\b' + re.escape(stage) + r'\b', line):
                idx = STAGE_IDX[stage]
                if cur and idx > cur['stage_idx']:
                    cur['stage']     = stage
                    cur['stage_idx'] = idx
                    changed = True
                break

        # Substage
        sub = _detect_substage(line)
        if sub and cur and cur['status'] == 'active' and cur['substage'] != sub:
            cur['substage'] = sub
            changed = True

        # Speed — attribute to upload or download based on item's current stage
        sm = _SPEED_RE.search(line)
        if sm:
            speed = sm.group(1)
            in_upload = cur and cur['stage_idx'] >= _UPLOAD_STAGE_IDX
            if in_upload:
                state['upload_speed'] = speed
            else:
                state['dl_speed'] = speed
            changed = True

        # Completion ("Skipping SendDoneToTracker" excluded — that's the abort path)
        if _DONE_RE.search(line):
            target = (cur if cur and cur['status'] == 'active' else None) \
                     or _oldest_active()
            if target:
                target['status'] = 'done'
                state['completed'] += 1
                state['upload_speed'] = None
                changed = True

        # Failure
        elif _FAIL_RE.search(line):
            target = (cur if cur and cur['status'] == 'active' else None) \
                     or _oldest_active()
            if target and target['status'] == 'active':
                target['status'] = 'failed'
                state['failed'] += 1
                changed = True

    # Always broadcast log line; only broadcast state on structural changes
    broadcast({'log': log_entry})
    if changed:
        broadcast({'state': _snapshot()})


def _snapshot(include_logs=False):
    with lock:
        active = [v for v in state['items'].values() if v['status'] == 'active']
        s = {
            'items'       : active[-6:],
            'completed'   : state['completed'],
            'failed'      : state['failed'],
            'upload_speed': state['upload_speed'],
            'dl_speed'    : state['dl_speed'],
            'stages'      : STAGES,
            'uptime'      : int(time.time() - state['started_at']),
        }
        if include_logs:
            s['logs'] = list(state['logs'])[-100:]
        return s


# ── HTML ───────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>youtube-grab dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d1117;--surface:#161b22;--border:#21262d;
  --text:#c9d1d9;--muted:#6e7681;--green:#3fb950;--red:#f85149;
  --blue:#58a6ff;--purple:#bc8cff;--yellow:#e3b341;--orange:#ffa657;
}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,sans-serif;font-size:13px;height:100vh;display:flex;flex-direction:column;overflow:hidden}

/* Header */
.hdr{background:var(--surface);border-bottom:1px solid var(--border);padding:10px 20px;display:flex;align-items:center;gap:20px;flex-shrink:0}
.hdr-title{font-size:15px;font-weight:700;color:#fff;display:flex;align-items:center;gap:8px}
.live-dot{width:8px;height:8px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.stat{display:flex;flex-direction:column;gap:1px}
.stat-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em}
.stat-val{font-size:22px;font-weight:700;line-height:1}
.c-green{color:var(--green)}.c-red{color:var(--red)}.c-blue{color:var(--blue)}.c-orange{color:var(--orange)}
.hdr-spacer{flex:1}
.uptime{font-size:11px;color:var(--muted)}
.btn-icon{background:none;border:1px solid var(--border);color:var(--muted);border-radius:6px;padding:5px 8px;cursor:pointer;font-size:14px;transition:color .15s,border-color .15s}
.btn-icon:hover{color:var(--text);border-color:var(--text)}

/* Body */
.body{display:grid;grid-template-columns:400px 1fr;gap:12px;padding:12px;flex:1;overflow:hidden;min-height:0}

/* Panel */
.panel{background:var(--surface);border:1px solid var(--border);border-radius:8px;display:flex;flex-direction:column;overflow:hidden}
.panel-hdr{padding:8px 14px;border-bottom:1px solid var(--border);font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;flex-shrink:0;display:flex;align-items:center;gap:8px}
.panel-body{flex:1;overflow-y:auto;padding:10px}

/* Filter badge */
.filter-badge{background:rgba(88,166,255,.15);color:var(--blue);border-radius:4px;padding:1px 6px;font-size:10px;display:flex;align-items:center;gap:4px}
.filter-badge button{background:none;border:none;color:var(--blue);cursor:pointer;font-size:12px;line-height:1;padding:0}

/* Item cards */
.item-card{background:var(--bg);border:1px solid var(--border);border-radius:7px;padding:10px 13px;margin-bottom:8px;border-left-width:3px;cursor:pointer;transition:box-shadow .15s}
.item-card:hover{box-shadow:0 0 0 1px rgba(255,255,255,.08)}
.item-card.card-selected{box-shadow:0 0 0 2px var(--blue)}
.item-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:7px}
.item-name{font-family:'SF Mono',Consolas,monospace;font-size:13px;font-weight:700}
.badge{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:700;text-transform:uppercase}
.badge-active{background:rgba(88,166,255,.15);color:var(--blue)}
.item-track{display:flex;align-items:center;gap:3px;margin-bottom:5px}
.dot{width:9px;height:9px;border-radius:50%;background:var(--border);flex-shrink:0;transition:background .2s}
.dot.past{background:var(--green);opacity:.7}
.dot.cur{background:var(--blue);box-shadow:0 0 5px var(--blue);width:11px;height:11px}
.track-label{font-size:11px;color:var(--blue);font-weight:600;margin-left:6px;white-space:nowrap}
.item-sub{font-size:11px;color:var(--purple);margin-bottom:3px}
.item-foot{font-size:10px;color:var(--muted)}
.empty{color:var(--muted);text-align:center;padding:32px 16px;font-size:12px}

/* Log */
.log-line{display:flex;gap:6px;padding:1px 0;border-bottom:1px solid rgba(33,38,45,.5);font-family:'SF Mono',Consolas,monospace;font-size:11px;line-height:1.6;align-items:baseline}
.log-ts{color:var(--muted);flex-shrink:0;user-select:none}
.item-tag{font-size:10px;font-weight:700;padding:0 4px;border-radius:3px;flex-shrink:0}
.log-msg{color:#6e7681;word-break:break-all;flex:1}
.log-msg.hl    {color:var(--text)}
.log-msg.hl-gr {color:var(--green)}
.log-msg.hl-rd {color:var(--red)}
.log-msg.hl-bl {color:var(--blue)}
.log-msg.hl-yl {color:var(--yellow)}

/* Settings modal */
.modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.6);display:flex;align-items:center;justify-content:center;z-index:100}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:10px;width:380px;padding:0;overflow:hidden}
.modal-hdr{padding:14px 18px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
.modal-hdr h2{font-size:14px;font-weight:700;color:var(--text)}
.modal-hdr button{background:none;border:none;color:var(--muted);cursor:pointer;font-size:18px;line-height:1}
.modal-hdr button:hover{color:var(--text)}
.modal-body{padding:18px}
.setting-row{margin-bottom:18px}
.setting-row label{display:block;font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px}
.setting-row input{width:100%;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:7px 10px;color:var(--text);font-size:13px;outline:none;transition:border-color .15s}
.setting-row input:focus{border-color:var(--blue)}
.setting-note{font-size:10px;margin-top:4px}
.note-restart{color:var(--yellow)}
.note-live{color:var(--green)}
.modal-footer{padding:14px 18px;border-top:1px solid var(--border);display:flex;justify-content:flex-end;gap:8px}
.btn{padding:7px 16px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:none}
.btn-cancel{background:var(--border);color:var(--text)}
.btn-save{background:var(--blue);color:#fff}
.btn-cancel:hover{background:#2d3748}.btn-save:hover{background:#4393e4}
.save-status{font-size:11px;margin-right:auto;padding-top:2px}
.save-status.ok{color:var(--green)}.save-status.err{color:var(--red)}

/* Scrollbar */
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
</style>
</head>
<body>

<div class="hdr">
  <div class="hdr-title"><span class="live-dot"></span>youtube-grab</div>
  <div class="stat">
    <span class="stat-lbl">Completed</span>
    <span class="stat-val c-green" id="n-done">0</span>
  </div>
  <div class="stat">
    <span class="stat-lbl">Failed</span>
    <span class="stat-val c-red" id="n-fail">0</span>
  </div>
  <div class="stat">
    <span class="stat-lbl">Download</span>
    <span class="stat-val c-blue" id="dl-speed">—</span>
  </div>
  <div class="stat">
    <span class="stat-lbl">Upload</span>
    <span class="stat-val c-orange" id="ul-speed">—</span>
  </div>
  <div class="hdr-spacer"></div>
  <span class="uptime" id="uptime"></span>
  <button class="btn-icon" title="Settings" onclick="openSettings()">⚙</button>
</div>

<div class="body">
  <div class="panel">
    <div class="panel-hdr">Active Items <span style="color:var(--muted);font-weight:400">(click to filter log)</span></div>
    <div class="panel-body" id="items"><div class="empty">Waiting for items…</div></div>
  </div>
  <div class="panel">
    <div class="panel-hdr">
      <span>Live log</span>
      <span class="filter-badge" id="filter-badge" style="display:none">
        <span id="filter-label"></span>
        <button title="Clear filter" onclick="setFilter(null)">✕</button>
      </span>
    </div>
    <div class="panel-body" id="log"></div>
  </div>
</div>

<!-- Settings modal (hidden by default) -->
<div class="modal-backdrop" id="settings-modal" style="display:none" onclick="onBackdropClick(event)">
  <div class="modal">
    <div class="modal-hdr">
      <h2>⚙ Settings</h2>
      <button onclick="closeSettings()">✕</button>
    </div>
    <div class="modal-body">
      <div class="setting-row">
        <label>Concurrent Jobs</label>
        <input type="number" id="s-concurrent" min="1" max="20" value="2">
        <div class="setting-note note-restart">⚠ Requires container restart to take effect</div>
      </div>
      <div class="setting-row">
        <label>Downloader Name</label>
        <input type="text" id="s-downloader" placeholder="e.g. Ikata">
        <div class="setting-note note-restart">⚠ Requires container restart to take effect</div>
      </div>
      <div class="setting-row">
        <label>Rsync Upload Threads</label>
        <input type="number" id="s-rsync" min="1" max="20" value="2">
        <div class="setting-note note-live">✓ Applied immediately via seesaw API if available</div>
      </div>
    </div>
    <div class="modal-footer">
      <span class="save-status" id="save-status"></span>
      <button class="btn btn-cancel" onclick="closeSettings()">Cancel</button>
      <button class="btn btn-save" onclick="saveSettings()">Save</button>
    </div>
  </div>
</div>

<script>
// ── Palette ────────────────────────────────────────────────────────────────────
const PALETTE = ['#58a6ff','#3fb950','#e3b341','#bc8cff','#f78166','#79c0ff','#56d364','#ffa657'];
function itemColor(ci){ return ci != null ? PALETTE[ci % PALETTE.length] : null; }

// ── Utilities ──────────────────────────────────────────────────────────────────
function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function fmtUptime(s){
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=s%60;
  return h?`${h}h ${m}m`:(m?`${m}m ${ss}s`:`${ss}s`);
}
function logCls(l){
  if(/getting (more )?comments|getting replies|Decrypted|player js/i.test(l)) return 'hl-bl';
  if(/not playable|aborted|error|failed/i.test(l))  return 'hl-rd';
  if(/done|completed|Sending done/i.test(l))         return 'hl-gr';
  if(/Starting|CheckIP|GetItem|PrepareDir|SetCookie|WgetDownload|MoveFiles|Upload|Tracker/i.test(l)) return 'hl-yl';
  if(l.trim()) return 'hl';
  return '';
}
function tagHtml(e){
  if(e.ci==null||!e.item) return '';
  const c=itemColor(e.ci), s=e.item.replace(/^v\d*:/,'').substring(0,8);
  return `<span class="item-tag" style="color:${c};background:${c}22">${esc(s)}</span>`;
}
function lineHtml(e){
  return `<span class="log-ts">${esc(e.t)}</span>${tagHtml(e)}<span class="log-msg ${logCls(e.l)}">${esc(e.l)}</span>`;
}

// ── Log buffer + filter ────────────────────────────────────────────────────────
// LOG_BUFFER is the ground truth. setFilter re-renders from it so the filter
// always reflects the actual state of the buffer — no stale DOM style issues.
const LOG_BUFFER = [];
const logEl  = document.getElementById('log');
const itemEl = document.getElementById('items');
let selectedKey = null;
let _stages = [];

function applyFilter(){
  const entries = selectedKey ? LOG_BUFFER.filter(e=>e.item===selectedKey) : LOG_BUFFER;
  const atBottom = logEl.scrollTop+logEl.clientHeight >= logEl.scrollHeight-30;
  logEl.innerHTML = entries.map(e=>
    `<div class="log-line" data-item="${esc(e.item)}">${lineHtml(e)}</div>`
  ).join('');
  if(atBottom||selectedKey) logEl.scrollTop = logEl.scrollHeight;
}

function setFilter(key){
  selectedKey = (key===selectedKey) ? null : key;
  applyFilter();

  const badge = document.getElementById('filter-badge');
  const lbl   = document.getElementById('filter-label');
  badge.style.display = selectedKey ? '' : 'none';
  if(selectedKey) lbl.textContent = selectedKey;

  document.querySelectorAll('.item-card').forEach(c=>{
    c.classList.toggle('card-selected', c.dataset.key===selectedKey);
  });
}

// Clicks on items panel: card → filter that item; empty area → clear filter
itemEl.addEventListener('click', e=>{
  const card = e.target.closest('.item-card[data-key]');
  setFilter(card ? card.dataset.key : null);
});
// Clicking the log panel clears the filter
logEl.addEventListener('click', ()=>{ if(selectedKey) setFilter(null); });

function appendLog(entry){
  LOG_BUFFER.push(entry);
  if(LOG_BUFFER.length>400) LOG_BUFFER.shift();
  // Only add to DOM if visible under current filter
  if(selectedKey && entry.item!==selectedKey) return;
  const atBottom = logEl.scrollTop+logEl.clientHeight >= logEl.scrollHeight-30;
  const div = document.createElement('div');
  div.className = 'log-line';
  div.dataset.item = entry.item||'';
  div.innerHTML = lineHtml(entry);
  logEl.appendChild(div);
  while(logEl.children.length>400) logEl.removeChild(logEl.firstChild);
  if(atBottom) logEl.scrollTop = logEl.scrollHeight;
}

// ── Stats + items rendering ────────────────────────────────────────────────────
function updateStats(s){
  document.getElementById('n-done').textContent   = s.completed;
  document.getElementById('n-fail').textContent   = s.failed;
  document.getElementById('dl-speed').textContent = s.dl_speed     || '—';
  document.getElementById('ul-speed').textContent = s.upload_speed || '—';
  document.getElementById('uptime').textContent   = 'up '+fmtUptime(s.uptime);
}

function cardHtml(it){
  const color = itemColor(it.ci)||'#58a6ff';
  const dots  = (_stages||[]).map((s,i)=>{
    const cls = i<it.stage_idx?'past':i===it.stage_idx?'cur':'';
    return `<span class="dot ${cls}" title="${esc(s)}"></span>`;
  }).join('');
  return `<div class="item-card${it.key===selectedKey?' card-selected':''}"
               data-key="${esc(it.key)}" style="border-left-color:${color}">
    <div class="item-top">
      <span class="item-name" style="color:${color}">${esc(it.name)}</span>
      <span class="badge badge-active">active</span>
    </div>
    <div class="item-track">${dots}<span class="track-label">${esc(it.stage||'—')}</span></div>
    ${it.substage?`<div class="item-sub">↳ ${esc(it.substage)}</div>`:''}
    <div class="item-foot">started ${esc(it.started)}</div>
  </div>`;
}

function updateItems(s){
  _stages = s.stages||_stages;
  const items = (s.items||[]).slice().reverse();
  itemEl.innerHTML = items.length ? items.map(cardHtml).join('') : '<div class="empty">Waiting for items…</div>';
}

function renderFull(s){
  updateStats(s);
  updateItems(s);
  if(s.logs){
    LOG_BUFFER.length = 0;
    s.logs.forEach(e=>LOG_BUFFER.push(e));
    applyFilter();
  }
}

// ── SSE ───────────────────────────────────────────────────────────────────────
function connect(){
  const es = new EventSource('/events');
  es.onmessage = e=>{
    const msg = JSON.parse(e.data);
    if(msg.state) renderFull(msg.state);
    else if(msg.log) appendLog(msg.log);
  };
  es.onerror = ()=>{ es.close(); setTimeout(connect,2000); };
}
connect();

// ── Settings modal ─────────────────────────────────────────────────────────────
async function openSettings(){
  const modal = document.getElementById('settings-modal');
  document.getElementById('save-status').textContent = '';
  try{
    const r = await fetch('/api/settings');
    const s = await r.json();
    document.getElementById('s-concurrent').value = s.concurrent||2;
    document.getElementById('s-downloader').value = s.downloader||'';
    document.getElementById('s-rsync').value = s.rsync_jobs||2;
  }catch(e){}
  modal.style.display = 'flex';
}
function closeSettings(){
  document.getElementById('settings-modal').style.display = 'none';
}
function onBackdropClick(e){
  if(e.target===document.getElementById('settings-modal')) closeSettings();
}
async function saveSettings(){
  const statusEl = document.getElementById('save-status');
  statusEl.textContent = 'Saving…'; statusEl.className = 'save-status';
  const data = {
    concurrent: parseInt(document.getElementById('s-concurrent').value)||2,
    downloader: document.getElementById('s-downloader').value.trim(),
    rsync_jobs: parseInt(document.getElementById('s-rsync').value)||2,
  };
  try{
    const r   = await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    const res = await r.json();
    if(res.ok){
      statusEl.className = 'save-status ok';
      statusEl.textContent = res.live ? '✓ Saved — rsync threads updated live' : '✓ Saved — restart container for concurrent/downloader changes';
    } else {
      statusEl.className = 'save-status err';
      statusEl.textContent = '✗ '+(res.error||'Unknown error');
    }
  }catch(e){
    statusEl.className = 'save-status err';
    statusEl.textContent = '✗ Request failed';
  }
}
</script>
</body>
</html>
"""


# ── HTTP handler ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        path = _urlparse(self.path).path
        if path == '/':
            body = HTML.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif path == '/api/settings':
            self._json(_settings)

        elif path == '/state':
            self._json(_snapshot(include_logs=True))

        elif path == '/events':
            self._sse()

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        path = _urlparse(self.path).path
        if path == '/api/settings':
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
                result = _save_settings(data)
            except Exception as e:
                result = {'ok': False, 'error': str(e), 'live': False}
            self._json(result)
        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sse(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'keep-alive')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        q = deque()
        with sse_lock:
            sse_clients.append(q)

        try:
            init = 'data: ' + json.dumps({'state': _snapshot(include_logs=True)}) + '\n\n'
            self.wfile.write(init.encode())
            self.wfile.flush()

            while True:
                if q:
                    self.wfile.write(q.popleft().encode())
                    self.wfile.flush()
                else:
                    self.wfile.write(b': ping\n\n')
                    self.wfile.flush()
                    time.sleep(0.25)
        except Exception:
            pass
        finally:
            with sse_lock:
                try:
                    sse_clients.remove(q)
                except ValueError:
                    pass


class _ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


# ── Entry point ────────────────────────────────────────────────────────────────

def _read_source(source):
    for line in source:
        if isinstance(line, bytes):
            line = line.decode('utf-8', errors='replace')
        parse_line(line)


def _tail_file(path):
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        while True:
            line = f.readline()
            if line:
                parse_line(line)
            else:
                time.sleep(0.05)


def main():
    _load_settings()

    ap = argparse.ArgumentParser(description='YouTube-grab pipeline dashboard')
    ap.add_argument('source', nargs='?', default='-',
                    help='Log file path, docker container name, or - for stdin')
    ap.add_argument('--port', type=int, default=PORT)
    ap.add_argument('--no-browser', action='store_true')
    args = ap.parse_args()

    if args.source == '-':
        t = threading.Thread(target=_read_source, args=(sys.stdin,), daemon=True)
    elif os.path.exists(args.source):
        t = threading.Thread(target=_tail_file, args=(args.source,), daemon=True)
    else:
        proc = subprocess.Popen(
            ['docker', 'logs', '-f', args.source],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        t = threading.Thread(target=_read_source, args=(proc.stdout,), daemon=True)

    t.start()

    url = f'http://localhost:{args.port}'
    print(f'Dashboard → {url}')
    if not args.no_browser:
        webbrowser.open(url)

    server = _ThreadedHTTPServer(('', args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nStopped.')


if __name__ == '__main__':
    main()
