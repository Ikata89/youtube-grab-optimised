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
    'CheckIP',
    'GetItemFromTracker',
    'PrepareDirectories',
    'SetCookies',
    'WgetDownload',
    'SetBadUrls',
    'PrepareStatsForTracker',
    'MoveFiles',
    'UploadWithTracker',
    'SendDoneToTracker',
]
STAGE_IDX = {s: i for i, s in enumerate(STAGES)}

# ── Shared state ───────────────────────────────────────────────────────────────
state = {
    'items'       : OrderedDict(),   # item_name → item_dict
    'completed'   : 0,
    'failed'      : 0,
    'upload_speed': None,
    'stage'       : None,
    'stage_idx'   : -1,
    'logs'        : deque(maxlen=400),
    'started_at'  : time.time(),
}
lock = threading.Lock()

# SSE clients: each is a deque they pop from
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

# Docker timestamps: 2026-05-30T10:23:45.123456789Z<space>
_DOCKER_TS = re.compile(r'^\d{4}-\d{2}-\d{2}T[\d:.]+Z ')

# Video item names embedded in log lines
_ITEM_RE = re.compile(r'\b(v\d*:[0-9a-zA-Z_\-]{6,12})\b')

# Rsync / wget upload speed
_SPEED_RE = re.compile(r'[\d,]+\s+\d+%\s+([\d.]+\s*[KMGkm]?B/s)')


def parse_line(raw: str):
    line = raw.rstrip('\n\r')
    if not line:
        return

    # Strip docker timestamp prefix so it doesn't pollute display
    line = _DOCKER_TS.sub('', line)

    now = ts()
    changed = False

    with lock:
        state['logs'].append({'t': now, 'l': line})

        # ── Pipeline stage ──────────────────────────────────────────────────
        for stage in STAGES:
            if re.search(r'\b' + re.escape(stage) + r'\b', line):
                if state['stage'] != stage:
                    state['stage'] = stage
                    state['stage_idx'] = STAGE_IDX[stage]
                    changed = True
                break

        # ── Item name detection ─────────────────────────────────────────────
        m = _ITEM_RE.search(line)
        item_name = m.group(1) if m else None

        if item_name and item_name not in state['items']:
            state['items'][item_name] = {
                'name'    : item_name,
                'stage'   : state['stage'] or '—',
                'substage': '',
                'started' : now,
                'status'  : 'active',
            }
            changed = True

        # Advance stage for all active items (coarse: they share one stage counter)
        if state['stage']:
            cur_idx = state['stage_idx']
            for item in state['items'].values():
                if item['status'] == 'active':
                    if STAGE_IDX.get(item['stage'], -1) < cur_idx:
                        item['stage'] = state['stage']
                        changed = True

        # ── Lua substage ────────────────────────────────────────────────────
        substage = None
        if   re.search(r'getting more comments', line, re.I):  substage = 'paginating comments'
        elif re.search(r'getting comments',      line, re.I):  substage = 'getting comments'
        elif re.search(r'getting replies from',  line, re.I):  substage = 'getting replies'
        elif re.search(r'Using cached player js',line, re.I):  substage = 'player JS (cached ✓)'
        elif re.search(r'Using player js url',   line, re.I):  substage = 'fetching player JS'
        elif re.search(r'Decrypted n\b',         line):        substage = 'decrypting n-param'
        elif re.search(r'Decrypted sig\b',       line):        substage = 'decrypting signature'
        elif re.search(r'found encrypted sig',   line, re.I):  substage = 'sig decrypt…'
        elif re.search(r'Video is not playable', line, re.I):  substage = 'not playable'
        elif re.search(r'comments turned off',   line, re.I):  substage = 'comments off'
        elif re.search(r'Checking IP',           line, re.I):  substage = 'IP check'

        if substage:
            for item in state['items'].values():
                if item['status'] == 'active':
                    item['substage'] = substage
            changed = True

        # ── Upload speed (rsync) ────────────────────────────────────────────
        sm = _SPEED_RE.search(line)
        if sm:
            state['upload_speed'] = sm.group(1)
            changed = True

        # ── Completion / failure ────────────────────────────────────────────
        if re.search(r'Sending done to tracker|item.*done|MaybeSendDoneToTracker.*complet', line, re.I):
            state['completed'] += 1
            _advance_oldest('done')
            state['upload_speed'] = None
            changed = True

        elif re.search(r'\bis aborted\b|failed to download', line, re.I):
            state['failed'] += 1
            _advance_oldest('failed')
            changed = True

    if changed:
        broadcast({'state': _snapshot()})
    else:
        # Always push log line updates (less aggressively)
        broadcast({'log': {'t': now, 'l': line}})


def _advance_oldest(status):
    """Mark the oldest active item with the given status."""
    for item in state['items'].values():
        if item['status'] == 'active':
            item['status'] = status
            break


def _snapshot():
    with lock:
        items_list = list(state['items'].values())
        return {
            'items'       : items_list[-20:],   # last 20
            'completed'   : state['completed'],
            'failed'      : state['failed'],
            'upload_speed': state['upload_speed'],
            'stage'       : state['stage'],
            'stage_idx'   : state['stage_idx'],
            'stages'      : STAGES,
            'logs'        : list(state['logs'])[-100:],
            'uptime'      : int(time.time() - state['started_at']),
        }


# ── HTTP server ────────────────────────────────────────────────────────────────

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
  --blue:#58a6ff;--purple:#bc8cff;--yellow:#e3b341;
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
.green{color:var(--green)}.red{color:var(--red)}.blue{color:var(--blue)}
.hdr-spacer{flex:1}
.uptime{font-size:11px;color:var(--muted)}

/* ── Pipeline bar ── */
.pipe{background:var(--surface);border-bottom:1px solid var(--border);padding:8px 20px;overflow-x:auto;flex-shrink:0}
.pipe-inner{display:flex;align-items:center;gap:0;min-width:max-content}
.ps{padding:5px 10px;border-radius:4px;font-size:11px;color:var(--muted);white-space:nowrap;transition:.2s}
.ps.active{background:#1c2d3a;color:var(--blue);font-weight:700}
.ps.past{color:var(--green)}
.ps-arrow{color:var(--border);margin:0 1px;font-size:10px;user-select:none}

/* ── Body grid ── */
.body{display:grid;grid-template-columns:340px 1fr;gap:12px;padding:12px;flex:1;overflow:hidden;min-height:0}

/* ── Panel ── */
.panel{background:var(--surface);border:1px solid var(--border);border-radius:8px;display:flex;flex-direction:column;overflow:hidden}
.panel-hdr{padding:8px 14px;border-bottom:1px solid var(--border);font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;flex-shrink:0}
.panel-body{flex:1;overflow-y:auto;padding:10px}

/* ── Item cards ── */
.item-card{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:9px 12px;margin-bottom:7px;transition:border-color .2s}
.item-card.active{border-color:#1c2d3a}
.item-card.done{border-color:#1a3a24;opacity:.65}
.item-card.failed{border-color:#3a1a1a}
.item-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.item-name{font-family:'SF Mono',Consolas,monospace;font-size:13px;font-weight:700;color:#fff}
.badge{font-size:10px;padding:2px 7px;border-radius:10px;font-weight:700}
.badge-active{background:#1c2d3a;color:var(--blue)}
.badge-done{background:#1a3a24;color:var(--green)}
.badge-failed{background:#3a1a1a;color:var(--red)}
.item-meta{display:flex;flex-wrap:wrap;gap:8px;font-size:11px;color:var(--muted)}
.item-stage{color:var(--blue)}
.item-sub{color:var(--purple)}
.item-time{color:var(--muted)}
.empty{color:var(--muted);text-align:center;padding:32px 16px;font-size:12px}

/* ── Log pane ── */
.log-line{display:flex;gap:8px;padding:1px 0;border-bottom:1px solid rgba(33,38,45,.6);font-family:'SF Mono',Consolas,monospace;font-size:11px;line-height:1.55}
.log-ts{color:var(--muted);flex-shrink:0;user-select:none}
.log-msg{color:#6e7681;word-break:break-all}
.log-msg.hl{color:var(--text)}
.log-msg.hl-green{color:var(--green)}
.log-msg.hl-red{color:var(--red)}
.log-msg.hl-blue{color:var(--blue)}
.log-msg.hl-yellow{color:var(--yellow)}

/* ── Scrollbar ── */
::-webkit-scrollbar{width:6px;height:6px}
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
    <span class="stat-lbl">Upload</span>
    <span class="stat-val blue" id="speed">—</span>
  </div>
  <div class="hdr-spacer"></div>
  <span class="uptime" id="uptime"></span>
</div>

<div class="pipe">
  <div class="pipe-inner" id="pipe"></div>
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
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

function fmtUptime(s){
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=s%60;
  return h?`${h}h ${m}m`:(m?`${m}m ${ss}s`:`${ss}s`);
}

function logClass(line){
  if(/getting (more )?comments|getting replies|Decrypted|player js/i.test(line)) return 'hl-blue';
  if(/not playable|aborted|error|failed/i.test(line))  return 'hl-red';
  if(/done|completed|Sending done/i.test(line))         return 'hl-green';
  if(/Starting|CheckIP|GetItem|PrepareDir|SetCookie|WgetDownload|MoveFiles|Upload|Tracker/i.test(line)) return 'hl-yellow';
  if(line.trim()) return 'hl';
  return '';
}

const logEl  = document.getElementById('log');
const itemEl = document.getElementById('items');
const pipeEl = document.getElementById('pipe');

function appendLog(entry){
  const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 30;
  const div = document.createElement('div');
  div.className = 'log-line';
  div.innerHTML = `<span class="log-ts">${esc(entry.t)}</span><span class="log-msg ${logClass(entry.l)}">${esc(entry.l)}</span>`;
  logEl.appendChild(div);
  // Trim if too many
  while(logEl.children.length > 300) logEl.removeChild(logEl.firstChild);
  if(atBottom) logEl.scrollTop = logEl.scrollHeight;
}

function renderFull(state){
  // Header stats
  document.getElementById('n-done').textContent = state.completed;
  document.getElementById('n-fail').textContent = state.failed;
  document.getElementById('speed').textContent  = state.upload_speed || '—';
  document.getElementById('uptime').textContent = 'up ' + fmtUptime(state.uptime);

  // Pipeline bar
  const stageIdx = state.stage_idx;
  pipeEl.innerHTML = (state.stages||[]).map((s,i)=>{
    const cls = i===stageIdx?'active':(i<stageIdx?'past':'');
    const arrow = i<state.stages.length-1?'<span class="ps-arrow">›</span>':'';
    return `<span class="ps ${cls}">${esc(s)}</span>${arrow}`;
  }).join('');

  // Items
  const items = (state.items||[]).slice().reverse();
  if(!items.length){
    itemEl.innerHTML = '<div class="empty">Waiting for items…</div>';
  } else {
    itemEl.innerHTML = items.map(it=>{
      const bc = it.status==='done'?'badge-done':it.status==='failed'?'badge-failed':'badge-active';
      const cc = it.status==='done'?'done':it.status==='failed'?'failed':'active';
      return `<div class="item-card ${cc}">
        <div class="item-top">
          <span class="item-name">${esc(it.name)}</span>
          <span class="badge ${bc}">${esc(it.status)}</span>
        </div>
        <div class="item-meta">
          <span class="item-time">${esc(it.started)}</span>
          <span class="item-stage">${esc(it.stage||'—')}</span>
          ${it.substage?`<span class="item-sub">↳ ${esc(it.substage)}</span>`:''}
        </div>
      </div>`;
    }).join('');
  }

  // Full log redraw (on reconnect / initial load)
  const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 30;
  logEl.innerHTML = (state.logs||[]).map(e=>
    `<div class="log-line"><span class="log-ts">${esc(e.t)}</span><span class="log-msg ${logClass(e.l)}">${esc(e.l)}</span></div>`
  ).join('');
  if(atBottom) logEl.scrollTop = logEl.scrollHeight;
}

// SSE connection with auto-reconnect
function connect(){
  const es = new EventSource('/events');
  es.onmessage = e => {
    const msg = JSON.parse(e.data);
    if(msg.state) renderFull(msg.state);
    else if(msg.log) appendLog(msg.log);
  };
  es.onerror = () => {
    es.close();
    setTimeout(connect, 2000);
  };
}
connect();
</script>
</body>
</html>
"""


# ── HTTP handler ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass  # suppress per-request noise

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
            body = json.dumps(_snapshot()).encode()
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
            # Send current state immediately
            init = 'data: ' + json.dumps({'state': _snapshot()}) + '\n\n'
            self.wfile.write(init.encode())
            self.wfile.flush()

            while True:
                if q:
                    self.wfile.write(q.popleft().encode())
                    self.wfile.flush()
                else:
                    # Heartbeat comment keeps connection alive
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
