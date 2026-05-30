#!/usr/bin/env python3
"""
YouTube-grab pipeline dashboard.

Usage:
    python dashboard.py CONTAINER_NAME       # stream docker logs
    python dashboard.py CONTAINER --port 9000
    docker logs -f CONTAINER | python dashboard.py -   # from stdin pipe
"""

import sys, os, re, json, time, threading, subprocess, webbrowser, argparse
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

# Stages at which a speed line belongs to the upload path rather than download
_UPLOAD_STAGE_IDX = STAGE_IDX['PrepareStatsForTracker']

# ── Shared state ───────────────────────────────────────────────────────────────
# items keyed by item_name.lower() so SetBadUrls' lowercase logs resolve correctly.
# item dict: name (original case), stage, stage_idx, substage, started, status, ci
# log entry: t, l, item (lower-case key or None), ci
state = {
    'items'       : OrderedDict(),
    'completed'   : 0,
    'failed'      : 0,
    'upload_speed': None,
    'dl_speed'    : None,
    'logs'        : deque(maxlen=400),
    'started_at'  : time.time(),
    '_cur_key'    : None,   # lower-case key of most recently seen item
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

# Item names: v:ID or v1:ID / v2:ID — match case-insensitively
_ITEM_RE = re.compile(r'\b(v\d*:[0-9a-zA-Z_\-]{6,12})\b')

# rsync progress:  "1,234,567  45%   5.12MB/s   0:00:01"
# (leading byte count + percentage required — uniquely identifies rsync output)
_RSYNC_SPEED_RE = re.compile(r'[\d,]+\s+\d+%\s+([\d.]+\s*[KMGkm]i?B/s)', re.I)

# Download speed — any speed token not already caught by _RSYNC_SPEED_RE.
# Covers wget verbose "(1.23 MB/s)" and bare "200KB/s" style output.
_DL_SPEED_RE = re.compile(r'([\d.]+\s*[KMGkm]i?B/s)', re.I)

# Completion signals (broaden if your seesaw version logs differently):
#   "Item v:ID done."          — seesaw pipeline runner standard log
#   "Sending done to tracker"  — SendDoneToTracker pre-request log
#   "item done"                — shorter seesaw variant
_DONE_RE = re.compile(
    r'Item\s+\S+\s+done\b'
    r'|\bitem\s+done\b'
    r'|Sending done.*?tracker',
    re.I
)

# Failure signals:
#   "Item v:abc123 is aborted." — SetBadUrls (note: logs item in lower-case)
#   "Download failed"           — WgetDownload on bad exit code
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
    log_entry = None

    with lock:
        # ── Item detection ────────────────────────────────────────────────────
        # Use lower-case key so SetBadUrls' lowercase logs match the stored entry.
        m = _ITEM_RE.search(line)
        if m:
            raw_name = m.group(1)
            key = raw_name.lower()
            if key not in state['items']:
                ci = state['_color_ctr']
                state['_color_ctr'] += 1
                state['items'][key] = {
                    'name'     : raw_name,   # original case for display
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
            'item': state['_cur_key'],          # lower-case key for JS matching
            'name': cur['name'] if cur else None,  # display name
            'ci'  : cur['ci'] if cur else None,
        }
        state['logs'].append(log_entry)

        # ── Pipeline stage → current item only ───────────────────────────────
        for stage in STAGES:
            if re.search(r'\b' + re.escape(stage) + r'\b', line):
                idx = STAGE_IDX[stage]
                if cur and idx > cur['stage_idx']:
                    cur['stage']     = stage
                    cur['stage_idx'] = idx
                    changed = True
                break

        # ── Substage ─────────────────────────────────────────────────────────
        sub = _detect_substage(line)
        if sub and cur and cur['status'] == 'active' and cur['substage'] != sub:
            cur['substage'] = sub
            changed = True

        # ── Speed ─────────────────────────────────────────────────────────────
        # rsync progress lines have a leading byte count before the percentage
        # and are always the upload phase.  Anything else with a speed token
        # gets attributed by stage: upload stages → upload, otherwise → download.
        rm = _RSYNC_SPEED_RE.search(line)
        if rm:
            state['upload_speed'] = rm.group(1)
            changed = True
        else:
            dm = _DL_SPEED_RE.search(line)
            if dm:
                speed = dm.group(1)
                in_upload = cur and cur['stage_idx'] >= _UPLOAD_STAGE_IDX
                if in_upload:
                    state['upload_speed'] = speed
                else:
                    state['dl_speed'] = speed
                changed = True

        # ── Completion ────────────────────────────────────────────────────────
        # "Skipping SendDoneToTracker" is intentionally excluded — that's the
        # abort path; the failure is already counted by _FAIL_RE via SetBadUrls.
        if _DONE_RE.search(line):
            target = (cur if cur and cur['status'] == 'active' else None) \
                     or _oldest_active()
            if target:
                target['status'] = 'done'
                state['completed'] += 1
                state['upload_speed'] = None
                changed = True

        # ── Failure ──────────────────────────────────────────────────────────
        elif _FAIL_RE.search(line):
            target = (cur if cur and cur['status'] == 'active' else None) \
                     or _oldest_active()
            if target and target['status'] == 'active':
                target['status'] = 'failed'
                state['failed'] += 1
                changed = True

    broadcast({'log': log_entry})
    if changed:
        broadcast({'state': _snapshot()})


def _snapshot(include_logs=False):
    with lock:
        # Only expose active items (max 6) — done/failed items are tracked
        # internally for colour assignment and log tagging but not shown.
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


# ── HTML dashboard ─────────────────────────────────────────────────────────────

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

/* ── Header ── */
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

/* ── Body ── */
.body{display:grid;grid-template-columns:400px 1fr;gap:12px;padding:12px;flex:1;overflow:hidden;min-height:0}

/* ── Panel ── */
.panel{background:var(--surface);border:1px solid var(--border);border-radius:8px;display:flex;flex-direction:column;overflow:hidden}
.panel-hdr{padding:8px 14px;border-bottom:1px solid var(--border);font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;flex-shrink:0;display:flex;align-items:center;gap:8px}
.panel-body{flex:1;overflow-y:auto;padding:10px}

/* filter badge in log panel header */
.filter-badge{background:rgba(88,166,255,.15);color:var(--blue);border-radius:4px;padding:1px 6px;font-size:10px;display:flex;align-items:center;gap:4px}
.filter-badge button{background:none;border:none;color:var(--blue);cursor:pointer;font-size:11px;line-height:1;padding:0}

/* ── Item cards ── */
.item-card{background:var(--bg);border:1px solid var(--border);border-radius:7px;padding:10px 13px;margin-bottom:8px;border-left-width:3px;cursor:pointer;transition:opacity .2s,box-shadow .2s}
.item-card:hover{box-shadow:0 0 0 1px rgba(255,255,255,.08)}
.item-card.card-selected{box-shadow:0 0 0 2px var(--blue)}
.item-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:7px}
.item-name{font-family:'SF Mono',Consolas,monospace;font-size:13px;font-weight:700}
.badge{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
.badge-active{background:rgba(88,166,255,.15);color:var(--blue)}

/* stage dot-track */
.item-track{display:flex;align-items:center;gap:3px;margin-bottom:5px}
.dot{width:9px;height:9px;border-radius:50%;background:var(--border);flex-shrink:0;cursor:default;transition:background .2s}
.dot.past{background:var(--green);opacity:.7}
.dot.cur{background:var(--blue);box-shadow:0 0 5px var(--blue);width:11px;height:11px}
.track-label{font-size:11px;color:var(--blue);font-weight:600;margin-left:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.item-sub{font-size:11px;color:var(--purple);margin-bottom:3px}
.item-foot{font-size:10px;color:var(--muted)}
.empty{color:var(--muted);text-align:center;padding:32px 16px;font-size:12px}

/* ── Log pane ── */
.log-line{display:flex;gap:6px;padding:1px 0;border-bottom:1px solid rgba(33,38,45,.5);font-family:'SF Mono',Consolas,monospace;font-size:11px;line-height:1.6;align-items:baseline}
.log-ts{color:var(--muted);flex-shrink:0;user-select:none}
.item-tag{font-size:10px;font-weight:700;padding:0 4px;border-radius:3px;flex-shrink:0;letter-spacing:.02em}
.log-msg{color:#6e7681;word-break:break-all;flex:1}
.log-msg.hl    {color:var(--text)}
.log-msg.hl-gr {color:var(--green)}
.log-msg.hl-rd {color:var(--red)}
.log-msg.hl-bl {color:var(--blue)}
.log-msg.hl-yl {color:var(--yellow)}

/* scrollbar */
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
</div>

<div class="body">
  <div class="panel">
    <div class="panel-hdr">Active Items</div>
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

<script>
const PALETTE = [
  '#58a6ff','#3fb950','#e3b341','#bc8cff',
  '#f78166','#79c0ff','#56d364','#ffa657'
];
function itemColor(ci){ return ci != null ? PALETTE[ci % PALETTE.length] : null; }

function esc(s){
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function fmtUptime(s){
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=s%60;
  return h?`${h}h ${m}m`:(m?`${m}m ${ss}s`:`${ss}s`);
}
function logCls(line){
  if(/getting (more )?comments|getting replies|Decrypted|player js/i.test(line)) return 'hl-bl';
  if(/not playable|aborted|error|failed/i.test(line))  return 'hl-rd';
  if(/done|completed|Sending done/i.test(line))         return 'hl-gr';
  if(/Starting|CheckIP|GetItem|PrepareDir|SetCookie|WgetDownload|MoveFiles|Upload|Tracker/i.test(line)) return 'hl-yl';
  if(line.trim()) return 'hl';
  return '';
}

// ── Filter state ──────────────────────────────────────────────────────────────
let selectedKey = null;   // lower-case item key, or null for no filter

// Apply current filter to every log line via direct style (no CSS class games).
function applyFilter() {
  const kids = logEl.children;
  for (let i = 0; i < kids.length; i++) {
    const el = kids[i];
    el.style.display = (!selectedKey || el.dataset.item === selectedKey) ? '' : 'none';
  }
}

function setFilter(key) {
  selectedKey = (key === selectedKey) ? null : key;  // toggle on re-click

  applyFilter();

  const badge = document.getElementById('filter-badge');
  const lbl   = document.getElementById('filter-label');
  badge.style.display = selectedKey ? '' : 'none';
  if (selectedKey) lbl.textContent = selectedKey;

  document.querySelectorAll('.item-card').forEach(c => {
    c.classList.toggle('card-selected', c.dataset.key === selectedKey);
  });
}

// Delegate clicks on the items panel — card or empty space both handled.
itemEl.addEventListener('click', e => {
  const card = e.target.closest('.item-card[data-key]');
  // Clicking empty space in items panel clears filter.
  setFilter(card ? card.dataset.key : null);
});
// Clicking the log panel clears the filter.
logEl.addEventListener('click', () => { if (selectedKey) setFilter(null); });

// ── Log helpers ───────────────────────────────────────────────────────────────
function itemTagHtml(entry) {
  if (entry.ci == null || !entry.item) return '';
  const color = itemColor(entry.ci);
  const short = entry.item.replace(/^v\d*:/,'').substring(0, 8);
  return `<span class="item-tag" style="color:${color};background:${color}22">${esc(short)}</span>`;
}

function logLineHtml(entry) {
  return `<span class="log-ts">${esc(entry.t)}</span>`
    + itemTagHtml(entry)
    + `<span class="log-msg ${logCls(entry.l)}">${esc(entry.l)}</span>`;
}

const logEl  = document.getElementById('log');
const itemEl = document.getElementById('items');
let   _stages = [];

function appendLog(entry) {
  const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 30;
  const div = document.createElement('div');
  div.className = 'log-line';
  div.dataset.item = entry.item || '';
  const visible = !selectedKey || entry.item === selectedKey;
  if (!visible) div.style.display = 'none';
  div.innerHTML = logLineHtml(entry);
  logEl.appendChild(div);
  while (logEl.children.length > 400) logEl.removeChild(logEl.firstChild);
  if (atBottom && visible) logEl.scrollTop = logEl.scrollHeight;
}

// ── Item card renderer ────────────────────────────────────────────────────────
function itemCardHtml(it) {
  const color = itemColor(it.ci) || '#58a6ff';
  const dots  = (_stages||[]).map((s, i) => {
    const cls = i < it.stage_idx ? 'past' : i === it.stage_idx ? 'cur' : '';
    return `<span class="dot ${cls}" title="${esc(s)}"></span>`;
  }).join('');
  const stageLbl = esc(it.stage || '—');
  return `<div class="item-card${it.key === selectedKey ? ' card-selected' : ''}"
               data-key="${esc(it.key)}"
               style="border-left-color:${color}">
    <div class="item-top">
      <span class="item-name" style="color:${color}">${esc(it.name)}</span>
      <span class="badge badge-active">active</span>
    </div>
    <div class="item-track">${dots}<span class="track-label">${stageLbl}</span></div>
    ${it.substage ? `<div class="item-sub">↳ ${esc(it.substage)}</div>` : ''}
    <div class="item-foot">started ${esc(it.started)}</div>
  </div>`;
}

// ── State rendering ───────────────────────────────────────────────────────────
function updateStats(s) {
  document.getElementById('n-done').textContent   = s.completed;
  document.getElementById('n-fail').textContent   = s.failed;
  document.getElementById('dl-speed').textContent = s.dl_speed     || '—';
  document.getElementById('ul-speed').textContent = s.upload_speed || '—';
  document.getElementById('uptime').textContent   = 'up ' + fmtUptime(s.uptime);
}

function updateItems(s) {
  _stages = s.stages || _stages;
  const items = (s.items || []).slice().reverse();
  itemEl.innerHTML = items.length
    ? items.map(itemCardHtml).join('')
    : '<div class="empty">Waiting for items…</div>';
}

function renderFull(s) {
  updateStats(s);
  updateItems(s);
  if (s.logs) {
    const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 30;
    logEl.innerHTML = s.logs.map(e => {
      const show = !selectedKey || e.item === selectedKey;
      return `<div class="log-line" data-item="${esc(e.item||'')}"${show ? '' : ' style="display:none"'}>`
        + logLineHtml(e) + '</div>';
    }).join('');
    if (atBottom) logEl.scrollTop = logEl.scrollHeight;
  }
}

// ── SSE with auto-reconnect ───────────────────────────────────────────────────
function connect() {
  const es = new EventSource('/events');
  es.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.state) renderFull(msg.state);
    else if (msg.log) appendLog(msg.log);
  };
  es.onerror = () => { es.close(); setTimeout(connect, 2000); };
}
connect();
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

        elif path == '/state':
            body = json.dumps(_snapshot(include_logs=True)).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(body)

        elif path == '/events':
            self._sse()

        else:
            self.send_response(404)
            self.end_headers()

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
    """Read a log file from the beginning, then block waiting for new lines."""
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        while True:
            line = f.readline()
            if line:
                parse_line(line)
            else:
                time.sleep(0.05)


def main():
    ap = argparse.ArgumentParser(description='YouTube-grab pipeline dashboard')
    ap.add_argument('source', nargs='?', default='-',
                    help='Log file path, docker container name, or - for stdin')
    ap.add_argument('--port', type=int, default=PORT,
                    help=f'HTTP port (default {PORT})')
    ap.add_argument('--no-browser', action='store_true',
                    help='Do not open browser automatically')
    args = ap.parse_args()

    if args.source == '-':
        reader_thread = threading.Thread(
            target=_read_source, args=(sys.stdin,), daemon=True)
    elif os.path.exists(args.source):
        reader_thread = threading.Thread(
            target=_tail_file, args=(args.source,), daemon=True)
    else:
        proc = subprocess.Popen(
            ['docker', 'logs', '-f', args.source],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        reader_thread = threading.Thread(
            target=_read_source, args=(proc.stdout,), daemon=True)

    reader_thread.start()

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
