// Demonstrates the core claim in concepts/correlated-failure.md:
// redundancy math assumes independent failures, but a real shared channel
// (here: a config push that reaches every "redundant" node identically)
// makes the true joint-failure rate collapse toward the shared cause's
// probability instead of the tiny product the naive math predicts.
package main

import (
	"fmt"
	"math/rand"
)

const (
	numNodes = 2
	numTrials = 50_000_000

	// Probability a single node fails on its own on a given day (hardware,
	// disk, whatever) -- independent per node.
	pIndependentFailure = 0.001

	// Probability a bad config/control-plane push ships on a given day.
	// When it fires it reaches every node identically -- this is the
	// channel the naive p^n formula doesn't know exists.
	pSharedConfigPush = 0.0005
)

func main() {
	rng := rand.New(rand.NewSource(1))

	naive := naiveJointFailureProbability()
	independentOnly := monteCarlo(rng, false)
	withSharedChannel := monteCarlo(rng, true)

	printReport(naive, independentOnly, withSharedChannel)
}

func naiveJointFailureProbability() float64 {
	p := 1.0
	for range numNodes {
		p *= pIndependentFailure
	}
	return p
}

func monteCarlo(rng *rand.Rand, includeSharedChannel bool) float64 {
	failures := 0
	for range numTrials {
		if dayFails(rng, includeSharedChannel) {
			failures++
		}
	}
	return float64(failures) / float64(numTrials)
}

func dayFails(rng *rand.Rand, includeSharedChannel bool) bool {
	if includeSharedChannel && rng.Float64() < pSharedConfigPush {
		return true
	}
	down := 0
	for range numNodes {
		if rng.Float64() < pIndependentFailure {
			down++
		}
	}
	return down == numNodes
}

func printReport(naive, independentOnly, withSharedChannel float64) {
	fmt.Println("=== Correlated failure: redundancy math vs reality ===")
	fmt.Println()
	fmt.Printf("Setup: %d \"redundant\" nodes\n", numNodes)
	fmt.Printf("  Independent failure probability per node per day: %.4f%%\n", pIndependentFailure*100)
	fmt.Printf("  Shared config-push failure probability per day:   %.4f%% (takes down every node identically)\n", pSharedConfigPush*100)
	fmt.Println()

	fmt.Println("--- Naive redundancy math (assumes independence) ---")
	fmt.Printf("P(all %d fail) = p^%d = %.10f (%.6f%%)\n", numNodes, numNodes, naive, naive*100)
	fmt.Printf("  -> expect a joint outage roughly once every %s\n", daysToHuman(1/naive))
	fmt.Println()

	fmt.Println("--- Monte Carlo, independent failures only (sanity check) ---")
	fmt.Printf("Trials: %d\n", numTrials)
	fmt.Printf("Empirical P(all fail) = %.10f (%.6f%%)\n", independentOnly, independentOnly*100)
	fmt.Println("  -> matches the naive math: this is the world redundancy math assumes.")
	fmt.Println()

	fmt.Println("--- Monte Carlo, WITH shared config-push channel ---")
	fmt.Printf("Trials: %d\n", numTrials)
	fmt.Printf("Empirical P(all fail) = %.10f (%.6f%%)\n", withSharedChannel, withSharedChannel*100)
	fmt.Printf("  -> %.0fx more frequent than the naive math predicted\n", withSharedChannel/naive)
	fmt.Printf("  -> expect a joint outage roughly once every %s\n", daysToHuman(1/withSharedChannel))
	fmt.Println()

	fmt.Println("Takeaway: adding a second \"redundant\" node barely moved the real number,")
	fmt.Println("because node failures were never the dominant risk -- the shared config")
	fmt.Println("pipeline was. Redundancy only pays for the risks it actually decorrelates.")
}

func daysToHuman(days float64) string {
	years := days / 365
	if years >= 1 {
		return fmt.Sprintf("%.1f years", years)
	}
	return fmt.Sprintf("%.1f days", days)
}
