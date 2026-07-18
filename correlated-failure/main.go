// Demonstrates concepts/correlated-failure.md with a real mechanism: two
// real net/http servers, each with its own independent per-request failure
// roll, plus one real shared bool ("a config push landed") that both real
// handlers check. Redundancy math assumes independence; this shows how
// little it takes -- one shared read -- to make that assumption fiction.
package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
)

const (
	numTicks            = 100_000
	pIndependentFailure = 0.02 // each node's own, unrelated failure roll per request
	pSharedConfigPush   = 0.01 // probability a bad push is "in effect" for a given tick
)

// node is a real HTTP handler for one "redundant" server. Two nodes share
// the same *badConfig pointer -- the one channel the naive p^n formula
// doesn't know exists.
type node struct {
	name      string
	badConfig *atomic.Bool
}

func (n *node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if n.badConfig.Load() {
		http.Error(w, "bad config on "+n.name, http.StatusInternalServerError)
		return
	}
	if rand.Float64() < pIndependentFailure {
		http.Error(w, "independent failure on "+n.name, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	fmt.Println("=== Correlated failure: two real HTTP nodes, redundancy math vs reality ===")
	fmt.Println()

	naive := pIndependentFailure * pIndependentFailure
	fmt.Println("--- Naive redundancy math (assumes independence) ---")
	fmt.Printf("P(both fail) = p^2 = %.6f (%.4f%%)\n", naive, naive*100)
	fmt.Println()

	fmt.Println("--- Scenario 1: two real nodes, independent failures only ---")
	indepRate := pollNodes(false)
	fmt.Printf("Empirical P(both fail) over %d real polls: %.6f (%.4f%%)\n", numTicks, indepRate, indepRate*100)
	fmt.Println("  -> matches the naive math: this is the world redundancy math assumes.")
	fmt.Println()

	fmt.Println("--- Scenario 2: same two real nodes, WITH a shared config-push channel ---")
	sharedRate := pollNodes(true)
	fmt.Printf("Empirical P(both fail) over %d real polls: %.6f (%.4f%%)\n", numTicks, sharedRate, sharedRate*100)
	fmt.Printf("  -> %.1fx more frequent than the naive math predicted\n", sharedRate/naive)
	fmt.Println()

	fmt.Println("Takeaway: same two real, independently-failing HTTP servers in both")
	fmt.Println("scenarios. The only difference is one shared bool both handlers check --")
	fmt.Println("and that alone is enough to make the redundancy math fiction.")
}

// pollNodes spins up two real HTTP servers and makes real requests to both,
// numTicks times, counting how often BOTH real responses are failures.
func pollNodes(includeSharedChannel bool) float64 {
	var badConfig atomic.Bool

	nodeA := httptest.NewServer(&node{name: "A", badConfig: &badConfig})
	defer nodeA.Close()
	nodeB := httptest.NewServer(&node{name: "B", badConfig: &badConfig})
	defer nodeB.Close()

	client := &http.Client{}
	bothFailed := 0

	for range numTicks {
		if includeSharedChannel && rand.Float64() < pSharedConfigPush {
			badConfig.Store(true)
		}

		aFailed := requestFails(client, nodeA.URL)
		bFailed := requestFails(client, nodeB.URL)
		if aFailed && bFailed {
			bothFailed++
		}

		badConfig.Store(false)
	}

	return float64(bothFailed) / float64(numTicks)
}

func requestFails(client *http.Client, url string) bool {
	resp, err := client.Get(url)
	if err != nil {
		return true
	}
	defer resp.Body.Close()
	return resp.StatusCode != http.StatusOK
}
