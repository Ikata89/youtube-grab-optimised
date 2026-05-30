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

# ── Shared state ───────────────────────────────────────────────────────────────
# Each item dict:
#   name, stage, stage_idx, substage, started, status ('active'|'done'|'failed'), ci (color index)
# Each log entry:
#   t (timestamp), l (line text), item (item name or None), ci (color index or None)
state = {
    'items'      : OrderedDict(),
    'completed'  : 0,
    'failed'     : 0,
    'upload_speed': None,
    'dl_speed'   : None,
    'logs'       : deque(maxlen=400),
    'started_at' : time.time(),
    '_cur_item'  : None,   # most recently referenced item name
    '_color_ctr' : 0,      # incremented per new item for palette assignment
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
# rsync upload speed:  "1,234,567  45%   5.12MB/s"
_UP_RE     = re.compile(r'[\d,]+\s+\d+%\s+([\d.]+\s*[KMGkm]?B/s)')
# wget download speed: "(1.23 MB/s)" at end of transfer line
_DL_RE     = re.compile(r'\(([\d.]+\s*[KMGi]+B/s)\)')
# Completion: SendDoneToTracker sends a POST and logs the response.
# "Sending done to tracker" is the most common pattern; broaden if needed.
_DONE_RE   = re.compile(r'Sending done.*?tracker', re.I)
# Failure: SetBadUrls logs "Item X is aborted."
_FAIL_RE   = re.compile(r'\bis aborted\b|failed to download', re.I)


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
        # ── Item name detection → updates _cur_item ─────────────────────────
        m = _ITEM_RE.search(line)
        item_name = m.group(1) if m else None

        if item_name:
            if item_name not in state['items']:
                ci = state['_color_ctr']
                state['_color_ctr'] += 1
                state['items'][item_name] = {
                    'name'     : item_name,
                    'stage'    : '',
                    'stage_idx': -1,
                    'substage' : '',
                    'started'  : now,
                    'status'   : 'active',
                    'ci'       : ci,
                }
                changed = True
            state['_cur_item'] = item_name

        cur = state['items'].get(state['_cur_item']) if state['_cur_item'] else None

        log_entry = {
            't'   : now,
            'l'   : line,
            'item': state['_cur_item'],
            'ci'  : cur['ci'] if cur else None,
        }
        state['logs'].append(log_entry)

        # ── Pipeline stage → update current item only ────────────────────────
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
        if sub and cur and cur['status'] == 'active':
            if cur['substage'] != sub:
                cur['substage'] = sub
                changed = True

        # ── Upload speed (rsync) ─────────────────────────────────────────────
        sm = _UP_RE.search(line)
        if sm:
            state['upload_speed'] = sm.group(1)
            changed = True

        # ── Download speed (wget verbose) ────────────────────────────────────
        dm = _DL_RE.search(line)
        if dm:
            state['dl_speed'] = dm.group(1)
            changed = True

        # ── Completion ───────────────────────────────────────────────────────
        # Use _cur_item if it's active, otherwise fall back to oldest active.
        # "Skipping SendDoneToTracker" is NOT counted — that's an abort path.
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

    # Broadcast the log line immediately (cheap delta).
    # Broadcast a state snapshot only when something structural changed.
    broadcast({'log': log_entry})
    if changed:
        broadcast({'state': _snapshot()})


def _snapshot(include_logs=False):
    with lock:
        s = {
            'items'       : list(state['items'].values())[-20:],
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
.green{color:var(--green)}.red{color:var(--red)}.blue{color:var(--blue)}.orange{color:var(--orange)}
.hdr-spacer{flex:1}
.uptime{font-size:11px;color:var(--muted)}

/* ── Body ── */
.body{display:grid;grid-template-columns:400px 1fr;gap:12px;padding:12px;flex:1;overflow:hidden;min-height:0}

/* ── Panel ── */
.panel{background:var(--surface);border:1px solid var(--border);border-radius:8px;display:flex;flex-direction:column;overflow:hidden}
.panel-hdr{padding:8px 14px;border-bottom:1px solid var(--border);font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;flex-shrink:0}
.panel-body{flex:1;overflow-y:auto;padding:10px}

/* ── Item cards ── */
.item-card{background:var(--bg);border:1px solid var(--border);border-radius:7px;padding:10px 13px;margin-bottom:8px;border-left-width:3px;transition:opacity .3s}
.item-card.done{opacity:.55}
.item-card.failed{opacity:.7}
.item-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:7px}
.item-name{font-family:'SF Mono',Consolas,monospace;font-size:13px;font-weight:700}

.badge{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
.badge-active{background:rgba(88,166,255,.15);color:var(--blue)}
.badge-done  {background:rgba(63,185,80,.15);color:var(--green)}
.badge-failed{background:rgba(248,81,73,.15);color:var(--red)}

/* stage dot-track */
.item-track{display:flex;align-items:center;gap:3px;margin-bottom:5px}
.dot{width:9px;height:9px;border-radius:50%;background:var(--border);flex-shrink:0;cursor:default;transition:background .2s}
.dot.past{background:var(--green);opacity:.7}
.dot.cur {background:var(--blue);box-shadow:0 0 5px var(--blue);width:11px;height:11px}
.track-label{font-size:11px;color:var(--blue);font-weight:600;margin-left:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.track-label.done-lbl{color:var(--green)}
.track-label.fail-lbl{color:var(--red)}

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
    <span class="stat-val green" id="n-done">0</span>
  </div>
  <div class="stat">
    <span class="stat-lbl">Failed</span>
    <span class="stat-val red" id="n-fail">0</span>
  </div>
  <div class="stat">
    <span class="stat-lbl">Download</span>
    <span class="stat-val blue" id="dl-speed">—</span>
  </div>
  <div class="stat">
    <span class="stat-lbl">Upload</span>
    <span class="stat-val orange" id="ul-speed">—</span>
  </div>
  <div class="hdr-spacer"></div>
  <span class="uptime" id="uptime"></span>
</div>

<div class="body">
  <div class="panel">
    <div class="panel-hdr">Items</div>
    <div class="panel-body" id="items"><div class="empty">Waiting for items…</div></div>
  </div>
  <div class="panel">
    <div class="panel-hdr">Live log</div>
    <div class="panel-body" id="log"></div>
  </div>
</div>

<script>
// 8-colour palette, one per item (cycles)
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

// Colour-code log lines by content
function logCls(line){
  if(/getting (more )?comments|getting replies|Decrypted|player js/i.test(line)) return 'hl-bl';
  if(/not playable|aborted|error|failed/i.test(line))  return 'hl-rd';
  if(/done|completed|Sending done/i.test(line))         return 'hl-gr';
  if(/Starting|CheckIP|GetItem|PrepareDir|SetCookie|WgetDownload|MoveFiles|Upload|Tracker/i.test(line)) return 'hl-yl';
  if(line.trim()) return 'hl';
  return '';
}

// Build a coloured [ITEMID] tag for a log entry
function itemTagHtml(entry){
  if(entry.ci == null) return '';
  const color = itemColor(entry.ci);
  const short = (entry.item||'').replace(/^v\d*:/,'').substring(0,8);
  const bg = color + '22';  // 13% opacity hex alpha
  return `<span class="item-tag" style="color:${color};background:${bg}">${esc(short)}</span>`;
}

// Build item card HTML
function itemCardHtml(it, stages){
  const color     = itemColor(it.ci) || '#58a6ff';
  const stageIdx  = it.stage_idx;
  const statusCls = it.status;

  const dots = stages.map((s,i) => {
    const cls = i < stageIdx ? 'past' : i === stageIdx ? 'cur' : '';
    return `<span class="dot ${cls}" title="${esc(s)}"></span>`;
  }).join('');

  let labelCls = '', labelTxt = esc(it.stage || '—');
  if(it.status === 'done')   { labelCls = 'done-lbl'; labelTxt = '✓ done'; }
  if(it.status === 'failed') { labelCls = 'fail-lbl'; labelTxt = '✗ failed'; }

  const bc = `badge-${it.status}`;

  return `<div class="item-card ${statusCls}" style="border-left-color:${color}">
    <div class="item-top">
      <span class="item-name" style="color:${color}">${esc(it.name)}</span>
      <span class="badge ${bc}">${esc(it.status)}</span>
    </div>
    <div class="item-track">
      ${dots}
      <span class="track-label ${labelCls}">${labelTxt}</span>
    </div>
    ${it.substage ? `<div class="item-sub">↳ ${esc(it.substage)}</div>` : ''}
    <div class="item-foot">started ${esc(it.started)}</div>
  </div>`;
}

const logEl  = document.getElementById('log');
const itemEl = document.getElementById('items');
let   _stages = [];

function appendLog(entry){
  const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 30;
  const div = document.createElement('div');
  div.className = 'log-line';
  div.innerHTML =
    `<span class="log-ts">${esc(entry.t)}</span>`
    + itemTagHtml(entry)
    + `<span class="log-msg ${logCls(entry.l)}">${esc(entry.l)}</span>`;
  logEl.appendChild(div);
  while(logEl.children.length > 400) logEl.removeChild(logEl.firstChild);
  if(atBottom) logEl.scrollTop = logEl.scrollHeight;
}

function updateStats(state){
  document.getElementById('n-done').textContent  = state.completed;
  document.getElementById('n-fail').textContent  = state.failed;
  document.getElementById('dl-speed').textContent = state.dl_speed    || '—';
  document.getElementById('ul-speed').textContent = state.upload_speed || '—';
  document.getElementById('uptime').textContent   = 'up ' + fmtUptime(state.uptime);
}

function updateItems(state){
  _stages = state.stages || _stages;
  const items = (state.items || []).slice().reverse();
  if(!items.length){
    itemEl.innerHTML = '<div class="empty">Waiting for items…</div>';
  } else {
    itemEl.innerHTML = items.map(it => itemCardHtml(it, _stages)).join('');
  }
}

function renderFull(state){
  updateStats(state);
  updateItems(state);
  if(state.logs){
    const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 30;
    logEl.innerHTML = state.logs.map(e =>
      `<div class="log-line">`
      + `<span class="log-ts">${esc(e.t)}</span>`
      + itemTagHtml(e)
      + `<span class="log-msg ${logCls(e.l)}">${esc(e.l)}</span>`
      + `</div>`
    ).join('');
    if(atBottom) logEl.scrollTop = logEl.scrollHeight;
  }
}

// SSE with auto-reconnect
function connect(){
  const es = new EventSource('/events');
  es.onmessage = e => {
    const msg = JSON.parse(e.data);
    if(msg.state){
      // Full state snapshot (includes logs on first connect)
      renderFull(msg.state);
    } else if(msg.log){
      // Incremental log line — update log pane only
      appendLog(msg.log);
    }
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
            # Full state + log replay on connect
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
