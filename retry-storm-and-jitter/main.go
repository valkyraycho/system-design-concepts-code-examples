// Demonstrates concepts/retry-storm-and-jitter.md with a real mechanism:
// a real net/http server simulating a dead downstream, and real concurrent
// clients retrying against it with real timers. Part 1 shows an outage is a
// synchronizer -- deterministic backoff makes every real client's request
// land at the same real instant; full jitter decorrelates them. Part 2 wires
// a real, concurrency-safe RetryBudget into the same clients and shows how
// much real load it keeps off the real server.
package main

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	baseDelay   = 200 * time.Millisecond // matches Ray's "2s, 4s, 8s" framing, compressed for a fast demo
	capDelay    = 1600 * time.Millisecond
	maxAttempts = 6

	part1NumClients     = 300
	part1OutageDuration = 5 * time.Second // must outlast every client's full retry sequence
	histogramWindow     = 5 * time.Second
	histogramBucket     = 100 * time.Millisecond
	histogramBarWidth   = 60

	part2NumClients     = 300
	part2OutageDuration = 5 * time.Second
	retryBudgetFraction = 0.10
)

// downstream is a real, dead-for-a-while HTTP service. Every real retry
// (attempt 2+; attempt 1 is the initial synchronized failure itself, shared
// identically by every scenario) is timestamped, so we can build a
// histogram from what actually happened on the wire.
type downstream struct {
	start    time.Time
	outage   time.Duration
	mu       sync.Mutex
	arrivals []time.Duration
	total    int64
}

func newDownstream(outage time.Duration) *downstream {
	return &downstream{start: time.Now(), outage: outage}
}

func (d *downstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	elapsed := time.Since(d.start)
	atomic.AddInt64(&d.total, 1)
	if attempt, err := strconv.Atoi(r.Header.Get("X-Attempt")); err == nil && attempt > 1 {
		d.mu.Lock()
		d.arrivals = append(d.arrivals, elapsed)
		d.mu.Unlock()
	}
	if elapsed < d.outage {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// retryBudget is real, concurrency-safe shared state a fleet of clients
// would consult in production before spending another retry against a
// struggling downstream.
type retryBudget struct {
	maxRatio    float64
	originalCnt atomic.Int64
	retryCnt    atomic.Int64
}

func (b *retryBudget) RecordOriginal() {
	b.originalCnt.Add(1)
}

func (b *retryBudget) AllowRetry() bool {
	orig := b.originalCnt.Load()
	retries := b.retryCnt.Add(1)
	if float64(retries) <= b.maxRatio*float64(orig) {
		return true
	}
	b.retryCnt.Add(-1)
	return false
}

func main() {
	fmt.Println("=== Part 1: real retry timing, with vs without jitter ===")
	fmt.Println()
	runTimingDemo(false, "No jitter (deterministic exponential backoff)")
	fmt.Println()
	runTimingDemo(true, "Full jitter: sleep(random(0, min(cap, base*2^attempt)))")

	fmt.Println()
	fmt.Println("=== Part 2: a real retry budget caps real load on a real dead server ===")
	fmt.Println()
	runRetryBudgetDemo()
}

func scheduledCeiling(attempt int) time.Duration {
	ceiling := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt-1)))
	if ceiling > capDelay {
		return capDelay
	}
	return ceiling
}

// retryUntilSuccessOrExhausted performs REAL HTTP requests against url,
// sleeping for a REAL, timer-based backoff between attempts. If budget is
// non-nil, every attempt past the first must be granted by it first.
func retryUntilSuccessOrExhausted(client *http.Client, url string, jitter bool, budget *retryBudget) {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 && budget != nil && !budget.AllowRetry() {
			return
		}
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("X-Attempt", strconv.Itoa(attempt))
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if attempt == maxAttempts {
			return
		}
		ceiling := scheduledCeiling(attempt)
		wait := ceiling
		if jitter {
			wait = time.Duration(rand.Float64() * float64(ceiling))
		}
		time.Sleep(wait)
	}
}

func runTimingDemo(jitter bool, label string) {
	d := newDownstream(part1OutageDuration)
	server := httptest.NewServer(d)
	defer server.Close()

	client := &http.Client{Timeout: 3 * time.Second}
	var wg sync.WaitGroup
	for range part1NumClients {
		wg.Go(func() {
			retryUntilSuccessOrExhausted(client, server.URL, jitter, nil)
		})
	}
	wg.Wait()

	fmt.Println("--- " + label + " ---")
	fmt.Printf("Real HTTP requests received: %d total (from %d real concurrent clients)\n", d.total, part1NumClients)
	fmt.Println("Histogram below is retries only (attempt 2+) -- attempt 1 is the initial")
	fmt.Println("synchronized failure itself, identical in both runs, not a jitter effect.")
	counts := bucketArrivals(d.arrivals)
	printHistogram(counts)
	peak, avg := peakAndAverage(counts)
	fmt.Printf("Peak retry load: %d req/bucket | Average: %.1f | Peak/avg ratio: %.1fx\n", peak, avg, float64(peak)/avg)
}

func bucketArrivals(arrivals []time.Duration) []int {
	numBuckets := int(histogramWindow / histogramBucket)
	counts := make([]int, numBuckets)
	for _, a := range arrivals {
		bucket := int(a / histogramBucket)
		if bucket >= 0 && bucket < numBuckets {
			counts[bucket]++
		}
	}
	return counts
}

func printHistogram(counts []int) {
	max := 0
	for _, c := range counts {
		if c > max {
			max = c
		}
	}
	for i, c := range counts {
		t := time.Duration(i) * histogramBucket
		barLen := 0
		if max > 0 {
			barLen = int(float64(c) / float64(max) * histogramBarWidth)
		}
		fmt.Printf("t=%6.2fs |%s\n", t.Seconds(), strings.Repeat("#", barLen))
	}
}

func peakAndAverage(counts []int) (peak int, avg float64) {
	sum := 0
	for _, c := range counts {
		if c > peak {
			peak = c
		}
		sum += c
	}
	avg = float64(sum) / float64(len(counts))
	return peak, avg
}

func runRetryBudgetDemo() {
	fmt.Println("--- Scenario A: unlimited retries against a real dead server ---")
	unbudgeted := runBudgetScenario(nil)
	fmt.Printf("Real requests that hit the server: %d\n", unbudgeted)

	fmt.Println()
	fmt.Printf("--- Scenario B: same clients, gated by a real retry budget (+%.0f%%) ---\n", retryBudgetFraction*100)
	budget := &retryBudget{maxRatio: retryBudgetFraction}
	budgeted := runBudgetScenario(budget)
	fmt.Printf("Real requests that hit the server: %d\n", budgeted)

	fmt.Println()
	fmt.Printf("Same %d real clients, same real dead server, same backoff schedule --\n", part2NumClients)
	fmt.Println("the only difference is a small RetryBudget object gating attempt #2+.")
	fmt.Printf("Load reduction: %.1fx fewer requests hit the downstream.\n", float64(unbudgeted)/float64(budgeted))
}

func runBudgetScenario(budget *retryBudget) int64 {
	d := newDownstream(part2OutageDuration)
	server := httptest.NewServer(d)
	defer server.Close()

	client := &http.Client{Timeout: 3 * time.Second}
	var wg sync.WaitGroup
	for range part2NumClients {
		wg.Go(func() {
			if budget != nil {
				budget.RecordOriginal()
			}
			retryUntilSuccessOrExhausted(client, server.URL, true, budget)
		})
	}
	wg.Wait()
	return atomic.LoadInt64(&d.total)
}
