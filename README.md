# Repomix

Stream GitHub repositories, local folders, or ZIP archives. All content stays
in the browser. The Go backend is a stateless pipe with a 4-field session
binding.

## Bugs fixed in this revision

1. **Analyze never opened a network connection.** `openWS` was resolving on
   `CONNECTING` sockets, and `wsSend` was silently dropping messages. Fixed:
   `openWS` now resolves only after `onopen`, and `wsSend` queues messages
   while the socket is connecting, then flushes on open.
2. **Preview showed `[Not fetched]` for every file.** When the frontend
   couldn't resolve a path to a SHA, it dropped the file from the fetch
   request instead of asking the backend to resolve it. Fixed: added a
   `request-paths` protocol message; the backend now resolves paths to SHAs
   and fetches them.
3. **`pending` stayed 0 after Preview on a new repo.** A race in the tree
   message flow could leave `pathToSha` empty. Fixed: `onTreeReceived` now
   warns if any file arrives without a SHA, and `analyzeGitHub` waits for the
   tree before clearing `busy`.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Detailed health JSON (public) |
| GET | `/healthz` | Plain `ok` for Render (public) |
| GET | `/testapi` | Per-token rate-limit probe (public) |
| GET | `/metrics` | Lifetime counters + health JSON (public) |
| GET | `/ws` | WebSocket upgrade |

## WebSocket protocol

### Client → Server

| Type | Payload | When |
|------|---------|------|
| `set-repo` | `{owner, repo, branch, ignorePatterns}` | On connect or repo switch |
| `fetch` | `{shas: [...]}` | Batched, on demand |
| `request-paths` | `{paths: [...]}` | Fallback when a path has no known SHA |
| `ping` | `{}` | Keepalive |

### Server → Client

| Type | Payload |
|------|---------|
| `ready` | `{repoId, defaultBranch}` |
| `tree` | `{files: [{path, size, sha}]}` |
| `file` | `{sha, path, size, content}` |
| `file-chunk` | `{sha, path, index, total, content}` |
| `file-skipped` | `{sha, path, reason}` |
| `fetch-complete` | `{requested, sent, skipped}` |
| `error` | `{message}` |

## DevTools helpers

| Command | Effect |
|---------|--------|
| `testapi()` | Probe every token's rate limit |
| `repomix.info()` | State snapshot (includes `wsReady`, `wsOutbox`) |
| `repomix.health()` | GET `/health` |
| `repomix.metrics()` | GET `/metrics` |
| `repomix.debug(true)` | Verbose WS logging on |
| `repomix.clearCache()` | Wipe SHA cache + IndexedDB |
| `repomix.reset()` | Reset the UI |

## Debugging WebSocket issues

Open the DevTools console and run `repomix.debug(true)`. Then click Analyze.
You'll see:

```

[repomix] WS → set-repo
[repomix] WS ← ready
[repomix] WS ← tree
[repomix] → Preview
[repomix] WS → fetch 7 shas
[repomix] WS ← file sha=...
[repomix] WS ← fetch-complete req=7 sent=7

```

If `set-repo` never appears, the socket isn't opening — check that the
backend is running and `BACKEND_WS` in `index.html` points at it.

If `fetch` never appears after clicking Preview, `collectMissing` returned
empty — check `repomix.info()` for `shaCache`, `selected`, and `pending`.

## Env variables

| Name | Default | Purpose |
|------|---------|---------|
| `PORT` | `8080` | HTTP listen port |
| `GITHUB_TOKENS` | — | Comma-separated PATs |
| `MAX_CLIENTS` | `8` | Max simultaneous WebSocket connections |
| `MAX_QUEUE` | `512` | Per-session fetch queue depth |
| `MAX_CONCURRENT_FETCHES` | `12` | Global concurrent outbound GitHub requests |
| `MAX_FILE_SIZE` | `8388608` | 8 MB. Files larger are skipped |
| `STREAM_CHUNK_SIZE` | `524288` | 512 KB. WS frame size |
| `TRUNCATE_PREVIEW_BYTES` | `262144` | 256 KB. Preview cutoff |
| `IDLE_TIMEOUT_SECONDS` | `3600` | Close idle WS |
| `PING_INTERVAL_SECONDS` | `45` | WS ping cadence |
| `ALLOWED_ORIGINS` | `*` | Comma-separated CORS allowlist |
| `LOG_LEVEL` | `info` | debug / info / warn / error |

## Local development

```powershell
cd render
go mod tidy
.\run.ps1
```

Or manually:

```
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
    }
}
go run .
```

## Deploy

Push `render/` to a repo. Render → New → Web Service → runtime Docker →
health check path `/healthz` → set env vars. Then update the two constants
at the top of `index.html`'s inline script to your Render URLs.

## License

MIT

