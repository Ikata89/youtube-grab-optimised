# youtube-grab-optimised — Progress

Fork of [ArchiveTeam/youtube-grab](https://github.com/ArchiveTeam/youtube-grab).
Changes live at <https://github.com/Ikata89/youtube-grab-optimised>.

---

## Done

### 1. Player JS fetch and decryption optimisation (`youtube.lua`)
*Commit `7c0decc`*

**Problem:** Every video item fetched the ~500 KB player JS over HTTPS and then ran two expensive `string.gsub` passes over it on every `execute_js` call. Each stream decryption also spawned a separate Node.js process (~50–100 ms startup each).

**Changes:**
- **`player_js_cache`** — module-level table keyed on the player JS URL. After the first fetch the raw body is reused for all subsequent items in the same session. The player JS URL only changes when YouTube deploys a new build (days to weeks), so cache-hit rate is near 100% in normal runs.
- **`player_js_cache.pretransformed`** — the first two `gsub` passes (preamble prepend + `'use strict'` patch) are identical for every call within the same player epoch. The intermediate result is cached so only the third per-call `gsub` (injecting `func`/`args`) runs each time.
- **`execute_js_batch`** — collapses multiple Node.js decryption calls into one process spawn by injecting multiple `console.log(...)` calls before the `})(_yt_player);` anchor and reading back one result per line. Stream processing refactored into three passes: (1) collect all `signatureCipher` calls → one batch spawn, (2) collect unique uncached `n` values → one batch spawn, (3) apply results and build final URLs.

**Impact:** For a 100-video session — ~99 HTTPS fetches eliminated, ~200 O(500 KB) `gsub` scans eliminated, 2–4 Node.js spawns per video reduced to 1–2.

---

### 2. Container startup wrapper (`Dockerfile`, `start.sh`)
*Commits `f27924c`, `c669c26`, `9fa99f1`*

The Dockerfile was a single `FROM` line with no entrypoint. Added:
- `COPY . /grab/` and `EXPOSE 8080`
- `start.sh` as `ENTRYPOINT` — starts the dashboard in the background reading from `/tmp/grab.log`, then runs `run-pipeline3 pipeline.py "$@"` piping all output through `tee` so both `docker logs` and the dashboard receive every line.

Usage unchanged — container arguments pass straight through to `run-pipeline3`:
```
docker run -p 8080:8080 IMAGE --concurrent 6 Ikata
```

---

### 3. Live browser dashboard (`dashboard.py`)
*Commits `f27924c` → `f452671`*

Single-file Python server (stdlib only, no extra deps). Reads from a log file, docker container name, or stdin. Serves an SSE-backed dashboard at `http://HOST:8080`.

**Features:**
| Area | Detail |
|---|---|
| Header | Completed / Failed / Download speed / Upload speed / Uptime |
| Items panel | One card per active item (max 6 shown). Each card has a 10-dot pipeline stage track (past = green, current = blue with glow), current substage label, and start time. Finished/failed items removed from panel automatically. |
| Log panel | Live log tail, colour-coded by content. Each line tagged with a coloured `[ITEMID]` pill. Clicking an item card filters the log to that item only; clicking the log panel or the same card again clears the filter. |
| Settings (⚙) | Modal to change Concurrent Jobs, Downloader Name, and Rsync Upload Threads. Changes saved to `dashboard_config.json`. Rsync threads POSTed to the seesaw API at `localhost:8001` for live effect; the other two require a container restart. |

**Log parsing:** Item names extracted by regex; all items keyed lowercase to handle `SetBadUrls`' lowercase log output. Stage transitions, substages, and completion/failure signals tracked per item. Speed attributed to download or upload based on the item's current pipeline stage.

---

## Known limitations / open issues

- **Completed/Failed counters** — detection relies on matching log strings (`"Item X done"`, `"Sending done to tracker"`, rsync's `"total size is"`, `"is aborted"`). If your seesaw version logs differently the counters may not increment. The patterns are in `_DONE_RE` / `_FAIL_RE` in `dashboard.py` and are easy to extend once you identify what your logs actually say.

- **Download speed** — wget runs with `-nv` which suppresses per-transfer speed output. The `Download` stat only populates if wget-at emits a speed token during the download phase; in many runs it will stay at `—`. No workaround without changing wget's verbosity flags.

- **Concurrent item log attribution** — `_cur_key` (the item a log line is attributed to) is set whenever an item ID appears in a log line and persists until the next item mention. In concurrent runs with interleaved output from multiple items, lines without an explicit item ID are attributed to whichever item was mentioned most recently, which may be wrong. The log tag colours help but the association is approximate.

- **Settings → Concurrent Jobs / Downloader** — these are read from `sys.argv` at pipeline startup and cannot be changed without restarting the container. The settings modal saves them to `dashboard_config.json`; `start.sh` does not yet read that file on startup, so a manual `docker run` with the new values is needed.

---

## What's next (suggestions)

1. **Read `dashboard_config.json` in `start.sh`** — source the saved concurrent and downloader values so the settings modal can fully control the next run without manual CLI editing.

2. **Proper item-to-log-line attribution for concurrent runs** — seesaw's concurrent workers write interleaved output with no item prefix. Options: (a) patch `pipeline.py` to prefix each `item.log_output()` call with the item name, or (b) use a named pipe per worker and route output separately.

3. **WARC size / throughput tracking** — the dashboard could poll the WARC file size on disk and display bytes archived and an estimated download rate, bypassing the `-nv` wget limitation.

4. **Automatic version bump** — increment `VERSION` in `pipeline.py` and update the `WGET_AT` minimum version string when submitting upstream.

5. **Upstream PR** — the player JS cache and batch decryption in `youtube.lua` are backward-compatible improvements that could be contributed back to ArchiveTeam/youtube-grab.
