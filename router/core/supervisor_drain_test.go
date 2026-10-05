package core

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestDrainBeforeShutdown_WaitsPeriod(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)

	var started atomic.Int32
	begin := time.Now()
	drainBeforeShutdown(context.Background(), 50*time.Millisecond, func() { started.Add(1) }, zap.New(core))
	elapsed := time.Since(begin)

	assert.Equal(t, int32(1), started.Load())
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
	assert.Equal(t, 1, logs.FilterMessage("Draining router").Len())
}

func TestDrainBeforeShutdown_ContextDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	var started atomic.Int32
	begin := time.Now()
	drainBeforeShutdown(ctx, 10*time.Second, func() { started.Add(1) }, zap.NewNop())

	assert.Equal(t, int32(1), started.Load())
	assert.Less(t, time.Since(begin), 200*time.Millisecond)
}

func TestShouldDrain(t *testing.T) {
	tests := []struct {
		name     string
		shutdown bool
		tweak    bool
		period   time.Duration
		want     bool
	}{
		{name: "real stop, tweak on, period set", shutdown: true, tweak: true, period: 35 * time.Second, want: true},
		{name: "TestStopRouter_ReloadNeverDrains", shutdown: false, tweak: true, period: 35 * time.Second, want: false},
		{name: "TestStopRouter_TweakOffNoDrain", shutdown: true, tweak: false, period: 35 * time.Second, want: false},
		{name: "real stop, tweak on, zero period", shutdown: true, tweak: true, period: 0, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldDrain(tt.shutdown, tt.tweak, tt.period))
		})
	}
}

// TestDrainThenShutdown_NoEOFForKeepAliveClient reproduces the production race: a pooled keep-alive
// client writing non-idempotent requests while the server shuts down. Without the drain,
// http.Server.Shutdown closes idle connections the client is about to reuse.
func TestDrainThenShutdown_NoEOFForKeepAliveClient(t *testing.T) {
	// runRound serves POST /echo, hammers it from 4 goroutines through one pooled client and stops
	// the server. It returns the number of requests that failed on a reused (pooled) connection
	// before Shutdown returned.
	runRound := func(t *testing.T, drainPeriod time.Duration) (failures int, firstErr error) {
		srv, base := startTestServer(t)

		mux := chi.NewRouter()
		mux.Post("/echo", func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte("ok"))
		})
		srv.state.Store(&serverState{mux: mux})

		client := &http.Client{Transport: &http.Transport{
			IdleConnTimeout:     200 * time.Millisecond,
			MaxIdleConnsPerHost: 4,
		}}
		defer client.CloseIdleConnections()

		var (
			stop   atomic.Bool
			errsMu sync.Mutex
			wg     sync.WaitGroup
		)
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stop.Load() {
					req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/echo", strings.NewReader("{}"))
					if err != nil {
						return
					}
					req.GetBody = nil // no transparent retry, like the gateway

					var reused bool
					req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused }}))
					resp, err := client.Do(req)
					if err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
					} else if reused && !errors.Is(err, syscall.ECONNREFUSED) {
						// Only failures on a pooled connection are the race under test. A fresh
						// connection that lands in the accept backlog as the listener closes fails
						// too, but in production new connections stop arriving once the pod leaves
						// the endpoints.
						errsMu.Lock()
						failures++
						if firstErr == nil {
							firstErr = err
						}
						errsMu.Unlock()
					}
					time.Sleep(time.Millisecond)
				}
			}()
		}

		time.Sleep(100 * time.Millisecond) // let the pool warm up

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if drainPeriod > 0 {
			drainBeforeShutdown(ctx, drainPeriod, srv.StartDraining, zap.NewNop())
		}
		require.NoError(t, srv.Shutdown(ctx))
		stop.Store(true)
		wg.Wait()

		errsMu.Lock()
		defer errsMu.Unlock()
		return failures, firstErr
	}

	t.Run("drain period longer than client idle timeout", func(t *testing.T) {
		failures, firstErr := runRound(t, 300*time.Millisecond)
		assert.Zero(t, failures, "first error: %v", firstErr)
	})

	t.Run("D=0 reproduces the EOF", func(t *testing.T) {
		for round := 0; round < 20; round++ {
			if failures, _ := runRound(t, 0); failures > 0 {
				return
			}
		}
		t.Fatal("no EOF / connection reset in 20 rounds without the drain; the test does not prove the fix")
	})
}
