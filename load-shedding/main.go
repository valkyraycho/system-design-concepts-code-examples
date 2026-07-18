// Demonstrates concepts/load-shedding.md with a real mechanism: a real
// net/http server under a real 10x flood of concurrent clients, in two
// modes -- accept everything (no admission control) vs a real semaphore-
// based front door that rejects instantly once at capacity. Same downstream
// work, same flood; only the front-door policy differs.
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"
)

const (
	capacity        = 50
	floodMultiplier = 10
	flood           = capacity * floodMultiplier
	baseServiceTime = 200 * time.Millisecond
	clientTimeout   = 800 * time.Millisecond
	fairShareTick   = 20 * time.Millisecond
)

// doWork models a shared, saturable resource with continuous fair-share
// scheduling: every request in flight gets an equal slice each tick, so a
// request makes real progress only in proportion to capacity/currentLoad --
// exactly like a thread pool or CPU scheduler under contention. This is
// what turns "a starving slice" from a one-off number into a genuine
// starvation dynamic that no request can dodge by arriving early.
func doWork(inFlight *atomic.Int64) {
	inFlight.Add(1)
	defer inFlight.Add(-1)

	remaining := baseServiceTime
	for remaining > 0 {
		time.Sleep(fairShareTick)
		share := float64(capacity) / float64(inFlight.Load())
		if share > 1 {
			share = 1
		}
		remaining -= time.Duration(float64(fairShareTick) * share)
	}
}

func acceptEverythingHandler(inFlight *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		doWork(inFlight)
		w.WriteHeader(http.StatusOK)
	}
}

// loadSheddingHandler is the real admission-control front door: a
// concurrency-capped semaphore. At capacity, it rejects in microseconds
// instead of queuing.
func loadSheddingHandler(sem chan struct{}, inFlight *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "at capacity, try again shortly", http.StatusServiceUnavailable)
			return
		}
		doWork(inFlight)
		w.WriteHeader(http.StatusOK)
	}
}

type outcome struct {
	status   int
	latency  time.Duration
	timedOut bool
}

// floodServer fires flood REAL concurrent HTTP requests at url and records
// what REALLY happened to each one.
func floodServer(url string) []outcome {
	client := &http.Client{Timeout: clientTimeout}
	outcomes := make([]outcome, flood)
	var wg sync.WaitGroup
	for i := range flood {
		wg.Go(func() {
			start := time.Now()
			resp, err := client.Get(url)
			latency := time.Since(start)
			if err != nil {
				outcomes[i] = outcome{latency: latency, timedOut: true}
				return
			}
			defer resp.Body.Close()
			outcomes[i] = outcome{status: resp.StatusCode, latency: latency}
		})
	}
	wg.Wait()
	return outcomes
}

func summarize(label string, outcomes []outcome) {
	var success, rejected, timedOut int
	var successLatency, rejectedLatency time.Duration

	for _, o := range outcomes {
		switch {
		case o.timedOut:
			timedOut++
		case o.status == http.StatusOK:
			success++
			successLatency += o.latency
		case o.status == http.StatusServiceUnavailable:
			rejected++
			rejectedLatency += o.latency
		}
	}

	fmt.Printf("--- %s ---\n", label)
	fmt.Printf("Flood: %d real concurrent requests against a server with capacity %d\n", flood, capacity)
	fmt.Printf("Succeeded (200):       %4d", success)
	if success > 0 {
		fmt.Printf("  avg latency %v", successLatency/time.Duration(success))
	}
	fmt.Println()
	fmt.Printf("Rejected fast (503):   %4d", rejected)
	if rejected > 0 {
		fmt.Printf("  avg latency %v", rejectedLatency/time.Duration(rejected))
	}
	fmt.Println()
	fmt.Printf("Timed out client-side: %4d (gave up after the full %v client timeout)\n", timedOut, clientTimeout)
	fmt.Printf("Goodput: %d/%d requests actually completed successfully\n", success, flood)
	fmt.Println()
}

func main() {
	fmt.Println("=== Load shedding: same flood, same downstream, different front door ===")
	fmt.Println()

	var inFlightA atomic.Int64
	serverA := httptest.NewServer(acceptEverythingHandler(&inFlightA))
	defer serverA.Close()
	outcomesA := floodServer(serverA.URL)
	summarize("Mode A: accept everything (no admission control)", outcomesA)

	var inFlightB atomic.Int64
	sem := make(chan struct{}, capacity)
	serverB := httptest.NewServer(loadSheddingHandler(sem, &inFlightB))
	defer serverB.Close()
	outcomesB := floodServer(serverB.URL)
	summarize("Mode B: load shedding (real semaphore front door, capacity-gated 503s)", outcomesB)

	fmt.Println("Takeaway: Mode A tries to give everyone a starving slice and almost")
	fmt.Println("nobody finishes in time -- goodput collapses. Mode B rejects 9/10")
	fmt.Println("requests in microseconds and the admitted 1/10 gets full, fast service.")
	fmt.Println("Rejecting isn't failure -- it's the only path back to the healthy state.")
}
