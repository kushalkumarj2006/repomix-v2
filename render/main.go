// Repomix backend — single-file streaming server.
//
// Endpoints:
//   GET /health     detailed health JSON (public)
//   GET /healthz    plain "ok" (Render probe, zero-alloc)
//   GET /testapi    per-token rate-limit probe (public)
//   GET /metrics    Prometheus-style counters + JSON (public)
//   GET /ws         WebSocket upgrade
//
// Session state is 4 fields: owner, repo, branch, ignorePatterns.
// Idle timeout, max clients, and concurrency are env-configurable.

package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ============================================================================
// CONFIG
// ============================================================================

type Config struct {
	Port                 int
	Tokens               []string
	MaxClients           int
	MaxQueue             int
	MaxConcurrentFetches int
	MaxFileSize          int64
	StreamChunkSize      int
	TruncatePreviewBytes int64
	IdleTimeout          time.Duration
	PingInterval         time.Duration
	AllowedOrigins       []string
	LogLevel             slog.Level
}

func loadConfig() Config {
	cfg := Config{
		Port:                 envInt("PORT", 8080),
		MaxClients:           envInt("MAX_CLIENTS", 8),
		MaxQueue:             envInt("MAX_QUEUE", 512),
		MaxConcurrentFetches: envInt("MAX_CONCURRENT_FETCHES", 12),
		MaxFileSize:          int64(envInt("MAX_FILE_SIZE", 8*1024*1024)),
		StreamChunkSize:      envInt("STREAM_CHUNK_SIZE", 512*1024),
		TruncatePreviewBytes: int64(envInt("TRUNCATE_PREVIEW_BYTES", 256*1024)),
		IdleTimeout:          time.Duration(envInt("IDLE_TIMEOUT_SECONDS", 3600)) * time.Second,
		PingInterval:         time.Duration(envInt("PING_INTERVAL_SECONDS", 45)) * time.Second,
		LogLevel:             parseLevel(envStr("LOG_LEVEL", "info")),
	}
	for _, t := range strings.Split(envStr("GITHUB_TOKENS", ""), ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			cfg.Tokens = append(cfg.Tokens, t)
		}
	}
	for _, o := range strings.Split(envStr("ALLOWED_ORIGINS", "*"), ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			cfg.AllowedOrigins = append(cfg.AllowedOrigins, o)
		}
	}
	return cfg
}

func envStr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
func envInt(k string, fallback int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ============================================================================
// CORS
// ============================================================================

// corsMiddleware adds permissive CORS headers to every HTTP response.
// The backend has no auth and is meant to be reached from any frontend
// origin (GitHub Pages, localhost dev servers, file://). "*" is safe here.
// If you ever restrict this, replace the "*" with a specific origin and
// handle the OPTIONS preflight with the same origin.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Auth-Key")
		w.Header().Set("Access-Control-Max-Age", "86400")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ============================================================================
// METRICS
// ============================================================================

type metrics struct {
	startedAt time.Time

	wsConnects    atomic.Uint64
	wsRejected    atomic.Uint64
	wsActive      atomic.Int64
	wsPeak        atomic.Int64
	wsIdleClosed  atomic.Uint64

	repoSets      atomic.Uint64
	treesSent     atomic.Uint64
	treeFilesSent atomic.Uint64
	treeTruncated atomic.Uint64

	fetchBatches      atomic.Uint64
	fetchShasReq      atomic.Uint64
	fetchPathsReq     atomic.Uint64
	fetchFilesSent    atomic.Uint64
	fetchFilesSkipped atomic.Uint64
	fetchBytesSent    atomic.Uint64
	fetchErrors       atomic.Uint64

	tarballFetches atomic.Uint64
	blobFetches    atomic.Uint64

	tokensDisabled atomic.Uint64
	tokensAuthFail atomic.Uint64

	httpTestAPICalls atomic.Uint64
	httpHealthCalls  atomic.Uint64
	httpMetricsCalls atomic.Uint64
}

func newMetrics() *metrics { return &metrics{startedAt: time.Now()} }

func (m *metrics) snapshot() map[string]any {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return map[string]any{
		"uptime":         time.Since(m.startedAt).Round(time.Second).String(),
		"uptimeSeconds":  int64(time.Since(m.startedAt).Seconds()),
		"wsConnects":     m.wsConnects.Load(),
		"wsRejected":     m.wsRejected.Load(),
		"wsActive":       m.wsActive.Load(),
		"wsPeak":         m.wsPeak.Load(),
		"wsIdleClosed":   m.wsIdleClosed.Load(),
		"repoSets":       m.repoSets.Load(),
		"treesSent":      m.treesSent.Load(),
		"treeFilesSent":  m.treeFilesSent.Load(),
		"treeTruncated":  m.treeTruncated.Load(),
		"fetchBatches":   m.fetchBatches.Load(),
		"fetchShasReq":   m.fetchShasReq.Load(),
		"fetchPathsReq":  m.fetchPathsReq.Load(),
		"fetchFilesSent": m.fetchFilesSent.Load(),
		"fetchFilesSkip": m.fetchFilesSkipped.Load(),
		"fetchBytesSent": m.fetchBytesSent.Load(),
		"fetchErrors":    m.fetchErrors.Load(),
		"tarballFetches": m.tarballFetches.Load(),
		"blobFetches":    m.blobFetches.Load(),
		"tokensDisabled": m.tokensDisabled.Load(),
		"tokensAuthFail": m.tokensAuthFail.Load(),
		"httpTestAPI":    m.httpTestAPICalls.Load(),
		"httpHealth":     m.httpHealthCalls.Load(),
		"httpMetrics":    m.httpMetricsCalls.Load(),
		"goroutines":     runtime.NumGoroutine(),
		"heapAlloc":      humanBytes(ms.HeapAlloc),
		"heapSys":        humanBytes(ms.HeapSys),
		"numGC":          ms.NumGC,
	}
}

// ============================================================================
// TOKEN POOL
// ============================================================================

type token struct {
	value     string
	remaining int
	resetAt   time.Time
	authFails int
	disabled  bool
}

type tokenPool struct {
	mu     sync.Mutex
	tokens []*token
	idx    int
	m      *metrics
}

func newTokenPool(values []string, m *metrics) *tokenPool {
	tokens := make([]*token, len(values))
	for i, v := range values {
		tokens[i] = &token{value: v, remaining: -1}
	}
	return &tokenPool{tokens: tokens, m: m}
}

func (p *tokenPool) next() *token {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.tokens) == 0 {
		return nil
	}
	now := time.Now()
	for i := 0; i < len(p.tokens); i++ {
		t := p.tokens[p.idx]
		p.idx = (p.idx + 1) % len(p.tokens)
		if t.disabled {
			continue
		}
		if t.remaining > 0 || t.remaining == -1 || now.After(t.resetAt) {
			if t.remaining == 0 && now.After(t.resetAt) {
				t.remaining = -1
			}
			return t
		}
	}
	for _, t := range p.tokens {
		if !t.disabled {
			return t
		}
	}
	return nil
}

func (p *tokenPool) report(t *token, h http.Header, status int) {
	if t == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if rem := h.Get("X-RateLimit-Remaining"); rem != "" {
		if n, err := strconv.Atoi(rem); err == nil {
			t.remaining = n
		}
	}
	if res := h.Get("X-RateLimit-Reset"); res != "" {
		if n, err := strconv.ParseInt(res, 10, 64); err == nil {
			t.resetAt = time.Unix(n, 0)
		}
	}
	if status == 401 {
		t.authFails++
		p.m.tokensAuthFail.Add(1)
		if t.authFails >= 3 && !t.disabled {
			t.disabled = true
			p.m.tokensDisabled.Add(1)
		}
	} else if status < 500 {
		t.authFails = 0
	}
}

func (p *tokenPool) snapshot() (available int, masked []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.tokens {
		if !t.disabled && t.remaining != 0 {
			available++
		}
		masked = append(masked, maskToken(t.value))
	}
	return
}

func (p *tokenPool) detailedSnapshot() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]any, len(p.tokens))
	for i, t := range p.tokens {
		out[i] = map[string]any{
			"index":     i,
			"masked":    maskToken(t.value),
			"remaining": t.remaining,
			"resetAt":   t.resetAt.UTC().Format(time.RFC3339),
			"authFails": t.authFails,
			"disabled":  t.disabled,
		}
	}
	return out
}

func maskToken(v string) string {
	if len(v) <= 8 {
		return "…"
	}
	if len(v) <= 16 {
		return v[:4] + "…" + v[len(v)-2:]
	}
	return v[:7] + "…" + v[len(v)-4:]
}

// ============================================================================
// SEMAPHORE
// ============================================================================

type semaphore struct {
	ch chan struct{}
}
func newSemaphore(n int) *semaphore {
	if n < 1 {
		n = 1
	}
	return &semaphore{ch: make(chan struct{}, n)}
}
func (s *semaphore) acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *semaphore) release() {
	select {
	case <-s.ch:
	default:
	}
}
func (s *semaphore) inFlight() int { return len(s.ch) }

// ============================================================================
// SESSION
// ============================================================================

type session struct {
	id   string
	conn *websocket.Conn

	mu             sync.RWMutex
	owner          string
	repo           string
	branch         string
	ignorePatterns []string
	createdAt      time.Time
	lastActivity   time.Time

	writeMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	queue   chan []string
	fetches atomic.Int32
}

func (s *session) sendJSON(v any) error {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return s.conn.WriteJSON(v)
}

func (s *session) setRepo(owner, repo, branch string, patterns []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owner = owner
	s.repo = repo
	s.branch = branch
	s.ignorePatterns = patterns
	s.lastActivity = time.Now()
}

func (s *session) repoInfo() (owner, repo, branch string, patterns []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.owner, s.repo, s.branch, s.ignorePatterns
}

func (s *session) idleFor() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Since(s.lastActivity)
}

func (s *session) snapshot() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]any{
		"id":           s.id,
		"owner":        s.owner,
		"repo":         s.repo,
		"branch":       s.branch,
		"queueLen":     len(s.queue),
		"queueCap":     cap(s.queue),
		"fetching":     s.fetches.Load() > 0,
		"fetchCount":   s.fetches.Load(),
		"connectedFor": time.Since(s.createdAt).Round(time.Second).String(),
		"lastActivity": time.Since(s.lastActivity).Round(time.Second).String(),
	}
}

// ============================================================================
// MESSAGES
// ============================================================================

type inMessage struct {
	Type           string   `json:"type"`
	Owner          string   `json:"owner,omitempty"`
	Repo           string   `json:"repo,omitempty"`
	Branch         string   `json:"branch,omitempty"`
	IgnorePatterns []string `json:"ignorePatterns,omitempty"`
	Shas           []string `json:"shas,omitempty"`
	Paths          []string `json:"paths,omitempty"`
}

type fileNode struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	Sha  string `json:"sha"`
}

// ============================================================================
// SERVER
// ============================================================================

type server struct {
	cfg      Config
	log      *slog.Logger
	tokens   *tokenPool
	sem      *semaphore
	client   *http.Client
	upgrader websocket.Upgrader
	metrics  *metrics

	activeMu    sync.Mutex
	activeConns map[string]*session
}

func newServer(cfg Config, log *slog.Logger, m *metrics) *server {
	s := &server{
		cfg:         cfg,
		log:         log,
		metrics:     m,
		tokens:      newTokenPool(cfg.Tokens, m),
		sem:         newSemaphore(cfg.MaxConcurrentFetches),
		activeConns: make(map[string]*session),
		client: &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 40,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
	s.upgrader = websocket.Upgrader{
		ReadBufferSize:    16 * 1024,
		WriteBufferSize:   16 * 1024,
		CheckOrigin:       func(r *http.Request) bool { return true },
		EnableCompression: true,
	}
	return s
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/testapi", s.handleTestAPI)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/", s.handleRoot)
	return corsMiddleware(mux)
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.metrics.httpHealthCalls.Add(1)
	payload := s.healthPayload()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (s *server) healthPayload() map[string]any {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	s.activeMu.Lock()
	sessions := make([]map[string]any, 0, len(s.activeConns))
	for _, sess := range s.activeConns {
		sessions = append(sessions, sess.snapshot())
	}
	active := len(s.activeConns)
	s.activeMu.Unlock()

	available, masked := s.tokens.snapshot()

	return map[string]any{
		"status":        "alive",
		"uptime":        time.Since(s.metrics.startedAt).Round(time.Second).String(),
		"uptimeSeconds": int64(time.Since(s.metrics.startedAt).Seconds()),
		"clients": map[string]any{
			"connected": active,
			"max":       s.cfg.MaxClients,
			"available": max(0, s.cfg.MaxClients-active),
		},
		"sessions": sessions,
		"fetching": map[string]any{
			"active": s.sem.inFlight(),
			"max":    s.cfg.MaxConcurrentFetches,
		},
		"memory": map[string]any{
			"rss":        humanBytes(ms.Sys),
			"heapAlloc":  humanBytes(ms.HeapAlloc),
			"heapSys":    humanBytes(ms.HeapSys),
			"heapInUse":  humanBytes(ms.HeapInuse),
			"numGC":      ms.NumGC,
			"goroutines": runtime.NumGoroutine(),
		},
		"tokens": map[string]any{
			"count":     len(s.cfg.Tokens),
			"available": available,
			"masked":    masked,
		},
		"config": map[string]any{
			"maxClients":           s.cfg.MaxClients,
			"maxConcurrentFetches": s.cfg.MaxConcurrentFetches,
			"maxQueue":             s.cfg.MaxQueue,
			"maxFileSize":          humanBytes(uint64(s.cfg.MaxFileSize)),
			"streamChunkSize":      humanBytes(uint64(s.cfg.StreamChunkSize)),
			"truncatePreviewBytes": humanBytes(uint64(s.cfg.TruncatePreviewBytes)),
			"idleTimeout":          s.cfg.IdleTimeout.String(),
			"pingInterval":         s.cfg.PingInterval.String(),
		},
		"time": time.Now().UTC().Format(time.RFC3339),
	}
}

// ----------------------------------------------------------------------------
// /testapi
// ----------------------------------------------------------------------------

type tokenProbe struct {
	Index     int    `json:"index"`
	Masked    string `json:"masked"`
	OK        bool   `json:"ok"`
	Limit     int    `json:"limit"`
	Remaining int    `json:"remaining"`
	Used      int    `json:"used"`
	ResetMins int    `json:"resetInMins"`
	ResetAt   string `json:"resetAt"`
	Error     string `json:"error,omitempty"`
	LatencyMs int64  `json:"latencyMs"`
	Disabled  bool   `json:"disabled,omitempty"`
}

func (s *server) handleTestAPI(w http.ResponseWriter, r *http.Request) {
	s.metrics.httpTestAPICalls.Add(1)
	start := time.Now()

	anon := s.probeRateLimit(r.Context(), "", "")

	results := make([]tokenProbe, len(s.cfg.Tokens))
	var wg sync.WaitGroup
	for i, raw := range s.cfg.Tokens {
		wg.Add(1)
		go func(idx int, tok string) {
			defer wg.Done()
			results[idx] = s.probeRateLimit(r.Context(), tok, maskToken(tok))
		}(i, raw)
	}
	wg.Wait()

	totalRemaining := 0
	totalLimit := 0
	healthy := 0
	for _, r := range results {
		if r.OK {
			healthy++
			totalRemaining += r.Remaining
			totalLimit += r.Limit
		}
	}

	payload := map[string]any{
		"success": true,
		"anonymous": map[string]any{
			"ok":          anon.OK,
			"limit":       anon.Limit,
			"remaining":   anon.Remaining,
			"resetInMins": anon.ResetMins,
			"error":       anon.Error,
		},
		"tokens":         results,
		"tokenCount":     len(s.cfg.Tokens),
		"healthyTokens":  healthy,
		"totalRemaining": totalRemaining,
		"totalLimit":     totalLimit,
		"aggregate": map[string]any{
			"poolRemaining": totalRemaining,
			"poolLimit":     totalLimit,
			"poolUtilized":  fmt.Sprintf("%.1f%%", 100*float64(totalLimit-totalRemaining)/float64(max(1, totalLimit))),
		},
		"probedInMs": time.Since(start).Milliseconds(),
		"time":       time.Now().UTC().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (s *server) probeRateLimit(ctx context.Context, tok, masked string) tokenProbe {
	probe := tokenProbe{Masked: masked, Index: -1}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.github.com/rate_limit", nil)
	if err != nil {
		probe.Error = err.Error()
		return probe
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "repomix-backend")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := s.client.Do(req)
	probe.LatencyMs = time.Since(start).Milliseconds()
	if err != nil {
		probe.Error = err.Error()
		return probe
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		probe.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return probe
	}
	var data struct {
		Resources struct {
			Core struct {
				Limit     int   `json:"limit"`
				Remaining int   `json:"remaining"`
				Used      int   `json:"used"`
				Reset     int64 `json:"reset"`
			} `json:"core"`
		} `json:"resources"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		probe.Error = "decode: " + err.Error()
		return probe
	}
	probe.OK = true
	probe.Limit = data.Resources.Core.Limit
	probe.Remaining = data.Resources.Core.Remaining
	probe.Used = data.Resources.Core.Used
	resetTime := time.Unix(data.Resources.Core.Reset, 0)
	probe.ResetAt = resetTime.UTC().Format(time.RFC3339)
	mins := int(time.Until(resetTime).Minutes())
	if mins < 0 {
		mins = 0
	}
	if mins > 60 {
		mins = 60
	}
	probe.ResetMins = mins
	return probe
}

// ----------------------------------------------------------------------------
// /metrics
// ----------------------------------------------------------------------------
func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.metrics.httpMetricsCalls.Add(1)

	s.activeMu.Lock()
	active := len(s.activeConns)
	s.activeMu.Unlock()

	available, _ := s.tokens.snapshot()

	payload := map[string]any{
		"status":          "alive",
		"time":            time.Now().UTC().Format(time.RFC3339),
		"health":          s.healthPayload(),
		"counters":        s.metrics.snapshot(),
		"tokensDetail":    s.tokens.detailedSnapshot(),
		"tokensAvailable": available,
		"activeConns":     active,
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "Repomix backend\n\nEndpoints:\n  GET /health    detailed health JSON\n  GET /healthz   plain ok\n  GET /testapi   per-token rate-limit probe\n  GET /metrics   lifetime counters + health\n  GET /ws        WebSocket upgrade\n")
}

// ============================================================================
// WEBSOCKET HANDLER
// ============================================================================

func (s *server) handleWS(w http.ResponseWriter, r *http.Request) {
	if s.activeConnsCount() >= int32(s.cfg.MaxClients) {
		s.metrics.wsRejected.Add(1)
		http.Error(w, "server at capacity", http.StatusServiceUnavailable)
		s.log.Warn("rejecting: at capacity",
			"active", s.activeConnsCount(), "max", s.cfg.MaxClients)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("upgrade failed", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:           randomID(),
		conn:         conn,
		ctx:          ctx,
		cancel:       cancel,
		queue:        make(chan []string, s.cfg.MaxQueue),
		createdAt:    time.Now(),
		lastActivity: time.Now(),
	}

	s.activeMu.Lock()
	s.activeConns[sess.id] = sess
	active := int64(len(s.activeConns))
	s.activeMu.Unlock()

	s.metrics.wsConnects.Add(1)
	s.metrics.wsActive.Add(1)
	for {
		peak := s.metrics.wsPeak.Load()
		if active <= peak || s.metrics.wsPeak.CompareAndSwap(peak, active) {
			break
		}
	}

	s.log.Info("session opened", "id", sess.id, "active", active)

	defer func() {
		cancel()
		s.activeMu.Lock()
		delete(s.activeConns, sess.id)
		s.activeMu.Unlock()
		conn.Close()
		s.metrics.wsActive.Add(-1)
		s.log.Info("session closed", "id", sess.id, "active", s.activeConnsCount()-1)
	}()

	go s.queueWorker(sess)

	conn.SetReadLimit(64 * 1024 * 1024)
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(2 * s.cfg.IdleTimeout))
		sess.mu.Lock()
		sess.lastActivity = time.Now()
		sess.mu.Unlock()
		return nil
	})

	go func() {
		t := time.NewTicker(s.cfg.PingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sess.writeMu.Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				err := conn.WriteMessage(websocket.PingMessage, nil)
				sess.writeMu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if sess.idleFor() > s.cfg.IdleTimeout {
					s.metrics.wsIdleClosed.Add(1)
					s.log.Info("closing idle session",
						"id", sess.id,
						"idle", sess.idleFor().Round(time.Second).String())
					sess.sendJSON(map[string]any{
						"type":    "error",
						"message": "closed due to inactivity",
					})
					cancel()
					_ = conn.Close()
					return
				}
			}
		}
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * s.cfg.IdleTimeout))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway) {
				s.log.Debug("read error", "id", sess.id, "err", err)
			}
			return
		}

		var msg inMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			_ = sess.sendJSON(map[string]any{"type": "error", "message": "invalid json"})
			continue
		}

		s.log.Debug("recv", "id", sess.id, "type", msg.Type,
			"shas", len(msg.Shas), "paths", len(msg.Paths))

		switch msg.Type {
		case "set-repo":
			s.handleSetRepo(sess, msg)
		case "fetch":
			s.handleFetch(sess, msg)
		case "request-paths":
			s.handleRequestPaths(sess, msg)
		case "ping":
			_ = sess.sendJSON(map[string]any{"type": "pong"})
		default:
			_ = sess.sendJSON(map[string]any{"type": "error", "message": "unknown type: " + msg.Type})
		}
	}
}

func (s *server) activeConnsCount() int32 {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	return int32(len(s.activeConns))
}

func (s *server) queueWorker(sess *session) {
	for {
		select {
		case <-sess.ctx.Done():
			return
		case batch, ok := <-sess.queue:
			if !ok {
				return
			}
			sess.fetches.Add(1)
			go func(b []string) {
				defer sess.fetches.Add(-1)
				s.doFetch(sess, b)
			}(batch)
		}
	}
}

// ============================================================================
// HANDLERS
// ============================================================================

func (s *server) handleSetRepo(sess *session, msg inMessage) {
	if msg.Owner == "" || msg.Repo == "" {
		_ = sess.sendJSON(map[string]any{"type": "error", "message": "missing owner/repo"})
		return
	}
	branch := msg.Branch
	if branch == "" {
		branch = "main"
	}
	sess.setRepo(msg.Owner, msg.Repo, branch, msg.IgnorePatterns)
	s.metrics.repoSets.Add(1)
	go s.doSetRepo(sess, msg.Owner, msg.Repo, branch, msg.IgnorePatterns)
}

func (s *server) doSetRepo(sess *session, owner, repo, branch string, patterns []string) {
	ctx := sess.ctx

	meta, err := s.fetchRepoMeta(ctx, owner, repo)
	if err != nil {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{"type": "error", "message": "repo metadata: " + err.Error()})
		return
	}
	if meta.DefaultBranch != "" {
		branch = meta.DefaultBranch
		sess.setRepo(owner, repo, branch, patterns)
	}

	tree, err := s.fetchTree(ctx, owner, repo, branch, patterns)
	if err != nil {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{"type": "error", "message": "tree: " + err.Error()})
		return
	}

	s.metrics.treesSent.Add(1)
	s.metrics.treeFilesSent.Add(uint64(len(tree)))

	_ = sess.sendJSON(map[string]any{
		"type":          "ready",
		"repoId":        meta.ID,
		"defaultBranch": branch,
	})
	_ = sess.sendJSON(map[string]any{
		"type":       "tree",
		"totalCount": len(tree),
		"files":      tree,
	})
}

func (s *server) handleFetch(sess *session, msg inMessage) {
	if len(msg.Shas) == 0 {
		_ = sess.sendJSON(map[string]any{
			"type": "fetch-complete", "requested": 0, "sent": 0, "skipped": 0,
		})
		return
	}

	seen := make(map[string]struct{}, len(msg.Shas))
	unique := make([]string, 0, len(msg.Shas))
	for _, sha := range msg.Shas {
		if sha == "" {
			continue
		}
		if _, ok := seen[sha]; ok {
			continue
		}
		seen[sha] = struct{}{}
		unique = append(unique, sha)
	}

	s.metrics.fetchBatches.Add(1)
	s.metrics.fetchShasReq.Add(uint64(len(unique)))

	select {
	case sess.queue <- unique:
	case <-sess.ctx.Done():
	case <-time.After(30 * time.Second):
		_ = sess.sendJSON(map[string]any{
			"type": "error", "message": "queue full, try a smaller batch",
		})
	}
}

func (s *server) handleRequestPaths(sess *session, msg inMessage) {
	if len(msg.Paths) == 0 {
		_ = sess.sendJSON(map[string]any{"type": "fetch-complete", "requested": 0})
		return
	}
	owner, repo, branch, patterns := sess.repoInfo()
	if owner == "" || repo == "" {
		_ = sess.sendJSON(map[string]any{"type": "error", "message": "set-repo first"})
		return
	}
	s.metrics.fetchPathsReq.Add(uint64(len(msg.Paths)))

	tree, err := s.fetchTree(sess.ctx, owner, repo, branch, patterns)
	if err != nil {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{"type": "error", "message": "tree: " + err.Error()})
		return
	}
	index := make(map[string]string, len(tree))
	for _, n := range tree {
		index[n.Path] = n.Sha
	}
	shas := make([]string, 0, len(msg.Paths))
	for _, p := range msg.Paths {
		if sha, ok := index[p]; ok && sha != "" {
			shas = append(shas, sha)
		}
	}
	if len(shas) == 0 {
		_ = sess.sendJSON(map[string]any{
			"type": "fetch-complete", "requested": len(msg.Paths), "sent": 0, "skipped": len(msg.Paths),
		})
		return
	}
	s.handleFetch(sess, inMessage{Type: "fetch", Shas: shas})
}

func (s *server) doFetch(sess *session, shas []string) {
	owner, repo, branch, _ := sess.repoInfo()
	if owner == "" || repo == "" {
		_ = sess.sendJSON(map[string]any{"type": "error", "message": "set-repo first"})
		return
	}
	requested := len(shas)
	sent := 0
	skipped := 0

	if requested >= 50 {
		s.metrics.tarballFetches.Add(1)
		sent, skipped = s.fetchViaTarball(sess.ctx, sess, owner, repo, branch, shas)
	} else {
		s.metrics.blobFetches.Add(1)
		sent, skipped = s.fetchViaBlobs(sess.ctx, sess, owner, repo, shas)
	}

	s.metrics.fetchFilesSent.Add(uint64(sent))
	s.metrics.fetchFilesSkipped.Add(uint64(skipped))

	_ = sess.sendJSON(map[string]any{
		"type":      "fetch-complete",
		"requested": requested,
		"sent":      sent,
		"skipped":   skipped,
	})
}

// ============================================================================
// GITHUB API
// ============================================================================

type repoMeta struct {
	ID            int64  `json:"id"`
	DefaultBranch string `json:"default_branch"`
}

func (s *server) ghRequest(ctx context.Context, url, accept string) (*http.Response, error) {
	tok := s.tokens.next()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if tok != nil {
		req.Header.Set("Authorization", "Bearer "+tok.value)
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("User-Agent", "repomix-backend")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	s.tokens.report(tok, resp.Header, resp.StatusCode)
	return resp, nil
}

func (s *server) fetchRepoMeta(ctx context.Context, owner, repo string) (*repoMeta, error) {
	if err := s.sem.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.sem.release()
	resp, err := s.ghRequest(ctx, "https://api.github.com/repos/"+owner+"/"+repo,
		"application/vnd.github+json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var meta repoMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (s *server) fetchTree(ctx context.Context, owner, repo, branch string, patterns []string) ([]fileNode, error) {
	if err := s.sem.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.sem.release()
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/git/trees/%s?recursive=1",
		owner, repo, branch)
	resp, err := s.ghRequest(ctx, url, "application/vnd.github+json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	nodes, truncated, err := filterTreeStream(resp.Body, patterns)
	if err != nil {
		return nil, err
	}
	if truncated {
		s.metrics.treeTruncated.Add(1)
	}
	return nodes, nil
}

func filterTreeStream(r io.Reader, patterns []string) ([]fileNode, bool, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	dec := json.NewDecoder(br)

	tok, err := dec.Token()
	if err != nil {
		return nil, false, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, false, errors.New("expected top-level object")
	}

	var out []fileNode
	found := false
	truncated := false

	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, false, err
		}
		key, _ := keyTok.(string)

		if key == "truncated" {
			if err := dec.Decode(&truncated); err != nil {
				return nil, false, err
			}
			continue
		}

		if key != "tree" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, false, err
			}
			continue
		}

		tok, err := dec.Token()
		if err != nil {
			return nil, false, err
		}
		if d, ok := tok.(json.Delim); !ok || d != '[' {
			return nil, false, errors.New("expected array for tree")
		}

		found = true
		for dec.More() {
			var node struct {
				Path string `json:"path"`
				Type string `json:"type"`
				Size int64  `json:"size"`
				Sha  string `json:"sha"`
			}
			if err := dec.Decode(&node); err != nil {
				return nil, false, err
			}
			if node.Type != "blob" {
				continue
			}
			if shouldIgnore(node.Path, patterns) {
				continue
			}
			if isBinaryPath(node.Path) {
				continue
			}
			out = append(out, fileNode{Path: node.Path, Size: node.Size, Sha: node.Sha})
		}

		if _, err := dec.Token(); err != nil {
			return nil, false, err
		}
	}

	if !found {
		return nil, false, errors.New("tree field missing")
	}
	return out, truncated, nil
}

// ============================================================================
// FETCH: BLOB PATH
// ============================================================================

func (s *server) fetchViaBlobs(ctx context.Context, sess *session, owner, repo string, shas []string) (sent, skipped int) {
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, sha := range shas {
		sha := sha
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, skip := s.fetchOneBlob(ctx, sess, owner, repo, sha)
			mu.Lock()
			if ok {
				sent++
			}
			if skip {
				skipped++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return
}

func (s *server) fetchOneBlob(ctx context.Context, sess *session, owner, repo, sha string) (sent, skipped bool) {
	if err := s.sem.acquire(ctx); err != nil {
		return false, false
	}
	defer s.sem.release()

	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/git/blobs/%s", owner, repo, sha)
	resp, err := s.ghRequest(ctx, url, "application/vnd.github.raw+json")
	if err != nil {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{"type": "error", "sha": sha, "message": err.Error()})
		return false, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{
			"type": "error", "sha": sha, "message": fmt.Sprintf("HTTP %d", resp.StatusCode),
		})
		return false, false
	}

	limited := io.LimitReader(resp.Body, s.cfg.MaxFileSize+1)
	buf, err := io.ReadAll(limited)
	if err != nil {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{"type": "error", "sha": sha, "message": err.Error()})
		return false, false
	}
	if int64(len(buf)) > s.cfg.MaxFileSize {
		_ = sess.sendJSON(map[string]any{
			"type": "file-skipped", "sha": sha, "path": sha, "reason": "oversize",
		})
		return false, true
	}
	if looksBinary(buf) {
		_ = sess.sendJSON(map[string]any{
			"type": "file-skipped", "sha": sha, "path": sha, "reason": "binary",
		})
		return false, true
	}
	if err := s.sendFile(sess, sha, "", buf); err != nil {
		return false, false
	}
	return true, false
}

// ============================================================================
// FETCH: TARBALL PATH
// ============================================================================

func (s *server) fetchViaTarball(ctx context.Context, sess *session, owner, repo, branch string, shas []string) (sent, skipped int) {
	wantSet := make(map[string]struct{}, len(shas))
	for _, h := range shas {
		wantSet[h] = struct{}{}
	}

	if err := s.sem.acquire(ctx); err != nil {
		return 0, 0
	}
	defer s.sem.release()

	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/tarball/%s", owner, repo, branch)
	resp, err := s.ghRequest(ctx, url, "application/vnd.github+json")
	if err != nil {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{"type": "error", "message": err.Error()})
		return 0, 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{
			"type": "error", "message": fmt.Sprintf("tarball HTTP %d", resp.StatusCode),
		})
		return 0, 0
	}

	gz, err := gzip.NewReader(bufio.NewReaderSize(resp.Body, 64*1024))
	if err != nil {
		s.metrics.fetchErrors.Add(1)
		_ = sess.sendJSON(map[string]any{"type": "error", "message": "gzip: " + err.Error()})
		return 0, 0
	}
	defer gz.Close()

	return parseTarStream(ctx, sess, gz, wantSet, s.cfg.MaxFileSize)
}

func parseTarStream(ctx context.Context, sess *session, r io.Reader, want map[string]struct{}, maxFile int64) (sent, skipped int) {
	br := bufio.NewReaderSize(r, 64*1024)
	hdr := make([]byte, 512)
	rootPrefix := ""

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if _, err := io.ReadFull(br, hdr); err != nil {
			return
		}
		if isZeroBlock(hdr) {
			return
		}
		name := tarName(hdr)
		if name == "" {
			return
		}
		size := tarSize(hdr)
		typeFlag := hdr[156]

		if rootPrefix == "" {
			if i := strings.IndexByte(name, '/'); i >= 0 {
				rootPrefix = name[:i+1]
			}
		}
		rel := name
		if strings.HasPrefix(rel, rootPrefix) {
			rel = rel[len(rootPrefix):]
		}

		if typeFlag != '0' && typeFlag != 0 {
			skipBytes(br, size)
			continue
		}

		body, err := readFileBytes(br, size, maxFile)
		if err != nil {
			return
		}
		if rel == "" || body == nil {
			continue
		}

		sha := gitBlobSha(body)
		if _, wanted := want[sha]; !wanted {
			continue
		}
		if looksBinary(body) {
			_ = sess.sendJSON(map[string]any{
				"type": "file-skipped", "sha": sha, "path": rel, "reason": "binary",
			})
			skipped++
			continue
		}
		if err := sess.sendJSON(map[string]any{
			"type":    "file",
			"sha":     sha,
			"path":    rel,
			"size":    len(body),
			"content": string(body),
		}); err != nil {
			return
		}
		sent++
	}
}

// ============================================================================
// TAR HELPERS
// ============================================================================

func isZeroBlock(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}
func tarName(hdr []byte) string {
	name := cstring(hdr[0:100])
	prefix := cstring(hdr[345:500])
	if prefix != "" {
		return prefix + "/" + name
	}
	return name
}
func tarSize(hdr []byte) int64 {
	s := strings.TrimSpace(cstring(hdr[124:136]))
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 8, 64)
	return n
}
func cstring(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}
func skipBytes(r *bufio.Reader, n int64) {
	padded := (n + 511) &^ 511
	buf := make([]byte, 32*1024)
	remaining := padded
	for remaining > 0 {
		chunk := int64(len(buf))
		if chunk > remaining {
			chunk = remaining
		}
		if _, err := io.ReadFull(r, buf[:chunk]); err != nil {
			return
		}
		remaining -= chunk
	}
}
func readFileBytes(r *bufio.Reader, n, maxFile int64) ([]byte, error) {
	padded := (n + 511) &^ 511
	if n <= maxFile {
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		pad := padded - n
		if pad > 0 {
			if _, err := io.CopyN(io.Discard, r, pad); err != nil {
				return nil, err
			}
		}
		return body, nil
	}
	if _, err := io.CopyN(io.Discard, r, padded); err != nil {
		return nil, err
	}
	return nil, nil
}

// ============================================================================
// HELPERS
// ============================================================================

func gitBlobSha(body []byte) string {
	h := sha1.New()
	h.Write([]byte(fmt.Sprintf("blob %d\x00", len(body))))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func (s *server) sendFile(sess *session, sha, path string, body []byte) error {
	s.metrics.fetchBytesSent.Add(uint64(len(body)))
	if len(body) <= s.cfg.StreamChunkSize {
		return sess.sendJSON(map[string]any{
			"type": "file", "sha": sha, "path": path,
			"size": len(body), "content": string(body),
		})
	}
	total := (len(body) + s.cfg.StreamChunkSize - 1) / s.cfg.StreamChunkSize
	for i := 0; i < total; i++ {
		start := i * s.cfg.StreamChunkSize
		end := start + s.cfg.StreamChunkSize
		if end > len(body) {
			end = len(body)
		}
		if err := sess.sendJSON(map[string]any{
			"type": "file-chunk", "sha": sha, "path": path,
			"index": i, "total": total, "size": len(body),
			"content": string(body[start:end]),
		}); err != nil {
			return err
		}
	}
	return nil
}

var binaryExts = map[string]struct{}{
	"png": {}, "jpg": {}, "jpeg": {}, "gif": {}, "ico": {}, "webp": {},
	"bmp": {}, "tiff": {}, "mp4": {}, "webm": {}, "mov": {}, "avi": {},
	"mkv": {}, "mp3": {}, "wav": {}, "ogg": {}, "flac": {}, "pdf": {},
	"doc": {}, "docx": {}, "xls": {}, "xlsx": {}, "ppt": {}, "pptx": {},
	"exe": {}, "dll": {}, "so": {}, "dylib": {}, "bin": {}, "dat": {},
	"db": {}, "sqlite": {}, "zip": {}, "tar": {}, "gz": {}, "bz2": {},
	"7z": {}, "rar": {}, "xz": {}, "woff": {}, "woff2": {}, "ttf": {},
	"otf": {}, "eot": {}, "class": {}, "jar": {}, "war": {}, "pyc": {},
	"pyo": {}, "o": {}, "a": {}, "lib": {},
}

func isBinaryPath(path string) bool {
	i := strings.LastIndexByte(path, '.')
	if i < 0 {
		return false
	}
	_, ok := binaryExts[strings.ToLower(path[i+1:])]
	return ok
}

func looksBinary(buf []byte) bool {
	n := len(buf)
	if n > 8192 {
		n = 8192
	}
	for i := 0; i < n; i++ {
		c := buf[i]
		if c == 0 {
			return true
		}
		if c < 9 || (c > 13 && c < 32) {
			return true
		}
	}
	return false
}

func shouldIgnore(path string, patterns []string) bool {
	if len(patterns) == 0 {
		return false
	}
	lower := strings.ToLower(strings.ReplaceAll(path, "\\", "/"))
	for _, raw := range patterns {
		pat := strings.ToLower(strings.ReplaceAll(raw, "\\", "/"))
		isFolder := strings.HasSuffix(raw, "/")
		pat = strings.TrimSuffix(pat, "/")
		if pat == "" {
			continue
		}
		if strings.Contains(pat, "*") {
			if matchGlob(pat, lower) {
				return true
			}
			continue
		}
		if isFolder {
			parts := strings.Split(lower, "/")
			for _, p := range parts {
				if p == pat {
					return true
				}
			}
			if strings.HasPrefix(lower, pat+"/") {
				return true
			}
		} else {
			parts := strings.Split(lower, "/")
			if len(parts) > 0 && parts[len(parts)-1] == pat {
				return true
			}
			for _, p := range parts {
				if p == pat {
					return true
				}
			}
		}
	}
	return false
}

func matchGlob(pattern, target string) bool {
	name := target
	if i := strings.LastIndexByte(target, '/'); i >= 0 {
		name = target[i+1:]
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return name == parts[0]
	}
	if !strings.HasPrefix(name, parts[0]) {
		return false
	}
	name = name[len(parts[0]):]
	for i := 1; i < len(parts)-1; i++ {
		idx := strings.Index(name, parts[i])
		if idx < 0 {
			return false
		}
		name = name[idx+len(parts[i]):]
	}
	last := parts[len(parts)-1]
	return strings.HasSuffix(name, last)
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

var randCounter atomic.Uint64
func randomID() string {
	n := randCounter.Add(1)
	return fmt.Sprintf("%08x", n^uint64(time.Now().UnixNano()))
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ============================================================================
// MAIN
// ============================================================================

func main() {
	cfg := loadConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout,
		&slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	if len(cfg.Tokens) == 0 {
		logger.Warn("no GITHUB_TOKENS set — unauthenticated (60/hr)")
	}

	m := newMetrics()
	srv := newServer(cfg, logger, m)
	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           srv.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	logger.Info("repomix backend starting",
		"port", cfg.Port,
		"tokens", len(cfg.Tokens),
		"maxClients", cfg.MaxClients,
		"maxQueue", cfg.MaxQueue,
		"concurrency", cfg.MaxConcurrentFetches,
		"maxFileSize", humanBytes(uint64(cfg.MaxFileSize)),
		"chunkSize", humanBytes(uint64(cfg.StreamChunkSize)),
		"truncatePreview", humanBytes(uint64(cfg.TruncatePreviewBytes)),
		"idleTimeout", cfg.IdleTimeout.String(),
		"pingInterval", cfg.PingInterval.String(),
	)

	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server exited", "err", err)
		os.Exit(1)
	}
}