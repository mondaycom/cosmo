package core

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/wundergraph/cosmo/router/pkg/health"
	"github.com/wundergraph/cosmo/router/pkg/mondaytweaks"
)

// serverState holds the mux and graph server together for atomic swaps.
// This ensures both values are always consistent - a single atomic operation
// swaps both the handler and the graph server reference together.
// Storing *chi.Mux directly (instead of http.Handler interface) avoids vtable indirection.
type serverState struct {
	mux         *chi.Mux     // HTTP handler, never nil (uses newNotReadyMux as sentinel)
	graphServer *graphServer // graph server for shutdown, nil until first config loaded
}

// newNotReadyMux returns 503 Service Unavailable for all requests.
// Used as sentinel before server is ready or after shutdown.
func newNotReadyMux() *chi.Mux {
	mux := chi.NewMux()
	mux.HandleFunc("/*", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server not ready", http.StatusServiceUnavailable)
	})
	return mux
}

// notReadyState is the sentinel state used before initialization and after shutdown.
// Using a sentinel instead of nil eliminates nil checks on the hot path.
var notReadyState = &serverState{
	mux:         newNotReadyMux(),
	graphServer: nil,
}

type server struct {
	mu          sync.RWMutex
	httpServer  *http.Server
	logger      *zap.Logger
	state       atomic.Pointer[serverState]
	healthcheck health.Checker
	baseURL     string
	listener    net.Listener // Pre-bound listener for synchronous port check
	// gracePeriod bounds the async old-graph-server in-flight drain on config swap
	// (see mondaytweaks.AsyncBoundedOldGraphServerShutdown). Sourced from the router
	// grace_period config value.
	gracePeriod time.Duration
}

type httpServerOptions struct {
	addr               string
	logger             *zap.Logger
	tlsServerConfig    *tls.Config
	healthcheck        health.Checker
	baseURL            string
	maxHeaderBytes     int
	livenessCheckPath  string
	readinessCheckPath string
	healthCheckPath    string
	gracePeriod        time.Duration
}

func newServer(opts *httpServerOptions) (*server, error) {
	// Bind the port synchronously to detect port conflicts immediately
	listener, err := net.Listen("tcp", opts.addr)
	if err != nil {
		return nil, fmt.Errorf("failed to bind to address %s: %w", opts.addr, err)
	}

	httpServer := &http.Server{
		Addr:        opts.addr,
		ReadTimeout: 60 * time.Second,
		// Disable write timeout to keep the connection open until the client closes it
		// This is required for SSE (Server-Sent-Events) subscriptions to work correctly
		WriteTimeout:   0,
		ErrorLog:       zap.NewStdLog(opts.logger),
		TLSConfig:      opts.tlsServerConfig,
		MaxHeaderBytes: opts.maxHeaderBytes,
	}

	// Create default handler for liveness and readiness
	httpRouter := chi.NewMux()
	httpRouter.Get(opts.healthCheckPath, opts.healthcheck.Liveness())
	httpRouter.Get(opts.livenessCheckPath, opts.healthcheck.Liveness())
	httpRouter.Get(opts.readinessCheckPath, opts.healthcheck.Readiness())

	n := &server{
		httpServer:  httpServer,
		logger:      opts.logger,
		mu:          sync.RWMutex{},
		healthcheck: opts.healthcheck,
		baseURL:     opts.baseURL,
		listener:    listener, // Store the pre-bound listener
		gracePeriod: opts.gracePeriod,
	}

	// Store the initial state with health check mux (graphServer nil until first config)
	n.state.Store(&serverState{
		mux:         httpRouter,
		graphServer: nil,
	})

	httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Lock-free hot path: single atomic load gets both mux and graphServer.
		// The state is never nil (initialized with health mux, swapped to notReadyState on shutdown).
		// Direct method call on *chi.Mux avoids interface vtable indirection.
		n.state.Load().mux.ServeHTTP(w, r)
	})

	return n, nil
}

func (s *server) HealthChecks() health.Checker {
	return s.healthcheck
}

func (s *server) HttpServer() *http.Server {
	return s.httpServer
}

// SwapGraphServer swaps the current graph server with a new one. It will shut down the old server gracefully.
// Because we swap the handler immediately, we can guarantee that no new requests will be served by the old graph server.
// However, it is possible that there are still requests in flight that are being processed by the old graph server.
// We wait until all requests are processed or timeout before shutting down the old graph server forcefully.
// Websocket connections are closed after shutdown through context cancellation. In the future, we might want to send
// a complete message to the client and wait until in-flight messages are delivered before closing the connection.
// NOT SAFE FOR CONCURRENT USE.
func (s *server) SwapGraphServer(ctx context.Context, svr *graphServer) {
	// Single atomic swap of both mux and graphServer together.
	// This ensures consistency - both values change atomically.
	newState := &serverState{
		mux:         svr.mux,
		graphServer: svr,
	}
	oldState := s.state.Swap(newState)

	// Shutdown the old graph server if it exists.
	// On first startup, oldState.graphServer is nil.
	if oldState == nil || oldState.graphServer == nil {
		return
	}
	old := oldState.graphServer

	if mondaytweaks.AsyncBoundedOldGraphServerShutdown {
		// New traffic already routes to svr after the swap above. Shut the old server
		// down OFF this goroutine (SwapGraphServer runs synchronously on the config
		// poller) and bound its in-flight drain, so a slow/stuck request can never
		// freeze CDN config hot-reload (ticket #3286) or pin the old generation's
		// schema + caches in memory. Detach from ctx (which stays alive for the router
		// lifetime) but keep its values for tracing, then bound by the grace period.
		go func() {
			shutdownCtx := context.WithoutCancel(ctx)
			if drain := s.oldGraphServerDrainTimeout(); drain > 0 {
				var cancel context.CancelFunc
				shutdownCtx, cancel = context.WithTimeout(shutdownCtx, drain)
				defer cancel()
			}
			if err := old.Shutdown(shutdownCtx); err != nil {
				s.logger.Error("Failed to shutdown old graph server", zap.Error(err))
			}
		}()
		return
	}

	if err := old.Shutdown(ctx); err != nil {
		s.logger.Error("Failed to shutdown old graph", zap.Error(err))
	}
}

// oldGraphServerDrainTimeout bounds the async old-graph-server in-flight drain on a
// config swap. It uses the configured router grace_period; if that is unset (<=0) it
// falls back to defaultOldGraphServerDrainTimeout so the drain is never unbounded —
// an unbounded drain is exactly what froze config reloads (ticket #3286).
func (s *server) oldGraphServerDrainTimeout() time.Duration {
	if s.gracePeriod > 0 {
		return s.gracePeriod
	}
	return defaultOldGraphServerDrainTimeout
}

// defaultOldGraphServerDrainTimeout is the fallback drain bound when grace_period is
// unset. Chosen above the default subgraph request_timeout (60s) so a well-behaved
// in-flight request can finish before the old server is abandoned.
const defaultOldGraphServerDrainTimeout = 90 * time.Second

// listenAndServe starts the server using the pre-bound listener and blocks until shutdown.
// This method is called in a goroutine; the port was already bound in newServer().
func (s *server) listenAndServe() error {
	if s.httpServer.TLSConfig != nil {
		// Use TLS with the pre-bound listener
		if err := s.httpServer.ServeTLS(s.listener, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	} else {
		// Use plain HTTP with the pre-bound listener
		if err := s.httpServer.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}

func (s *server) Shutdown(ctx context.Context) error {
	var err error

	// httpServer is not on the hot path, so we keep using the mutex for it
	s.mu.RLock()
	httpServer := s.httpServer
	s.mu.RUnlock()

	// Get current state and swap to notReadyState atomically
	// This ensures new requests get 503 immediately while we shut down
	oldState := s.state.Swap(notReadyState)

	if httpServer != nil {
		err = errors.Join(err, httpServer.Shutdown(ctx))
	}

	if oldState != nil && oldState.graphServer != nil {
		err = errors.Join(err, oldState.graphServer.Shutdown(ctx))
	}

	return err
}
