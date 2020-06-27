package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestInitialElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): initial election")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * ElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
}

func TestReElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader.
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no leader should
	// be elected.
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * ElectionTimeout)
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}
// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const ElectionTimeout = 1000 * time.Millisecond
