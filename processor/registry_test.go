package main

import (
	"sync"
	"testing"
)

func snapV1() *registrySnapshot {
	return &registrySnapshot{
		stations: map[string]stationMeta{
			"TR-IST-0001": {numConnectors: 2, operatorID: "ChargeSquare", city: "Istanbul", country: "TR", lat: 41, lon: 29},
		},
		tariffs: map[string]struct{}{"standard-v1": {}},
	}
}

func snapV2() *registrySnapshot {
	return &registrySnapshot{
		stations: map[string]stationMeta{
			"TR-IST-0001": {numConnectors: 4, operatorID: "ChargeSquare", city: "Istanbul", country: "TR", lat: 41, lon: 29},
			"TR-ANK-0007": {numConnectors: 6, operatorID: "ChargeSquare", city: "Ankara", country: "TR", lat: 39, lon: 32},
		},
		tariffs: map[string]struct{}{"standard-v1": {}, "night-v2": {}},
	}
}

// TestRegistrySwapVisibility proves the accessors observe a new snapshot after an atomic
// swap: an added station, an updated connector count, and an added tariff all become visible.
func TestRegistrySwapVisibility(t *testing.T) {
	r := &Registry{}
	r.snap.Store(snapV1())

	if n, ok := r.Station("TR-IST-0001"); !ok || n != 2 {
		t.Fatalf("v1: Station(TR-IST-0001) = %d,%v; want 2,true", n, ok)
	}
	if r.Len() != 1 {
		t.Fatalf("v1: Len = %d; want 1", r.Len())
	}
	if _, ok := r.Station("TR-ANK-0007"); ok {
		t.Fatalf("v1: TR-ANK-0007 should be absent")
	}
	if r.TariffKnown("night-v2") {
		t.Fatalf("v1: night-v2 should be absent")
	}

	r.snap.Store(snapV2())

	if n, ok := r.Station("TR-IST-0001"); !ok || n != 4 {
		t.Fatalf("v2: Station(TR-IST-0001) = %d,%v; want 4,true", n, ok)
	}
	if n, ok := r.Station("TR-ANK-0007"); !ok || n != 6 {
		t.Fatalf("v2: Station(TR-ANK-0007) = %d,%v; want 6,true", n, ok)
	}
	if m, ok := r.StationMeta("TR-ANK-0007"); !ok || m.city != "Ankara" {
		t.Fatalf("v2: StationMeta(TR-ANK-0007).city = %q,%v; want Ankara,true", m.city, ok)
	}
	if !r.TariffKnown("night-v2") {
		t.Fatalf("v2: night-v2 should be known")
	}
	if r.Len() != 2 {
		t.Fatalf("v2: Len = %d; want 2", r.Len())
	}
}

// TestRegistryConcurrentSwap races lock-free reads against atomic swaps. Under -race it
// proves the accessors never observe a torn map while the snapshot is being replaced.
func TestRegistryConcurrentSwap(t *testing.T) {
	r := &Registry{}
	r.snap.Store(snapV1())
	snaps := []*registrySnapshot{snapV1(), snapV2()}

	const iters = 10000
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			r.snap.Store(snaps[i&1])
		}
	}()

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				r.Station("TR-IST-0001")
				r.StationMeta("TR-ANK-0007")
				r.TariffKnown("standard-v1")
				_ = r.Len()
			}
		}()
	}
	wg.Wait()
}

// TestRegistryRefreshKeepsOldOnFailure pins the safety invariant: a failed Refresh (here an
// unreachable DB) returns the error and leaves the previous snapshot in place, so a transient
// outage cannot blank the roster and start dead-lettering every event. The empty-result branch
// shares the same "return before snap.Store" path.
func TestRegistryRefreshKeepsOldOnFailure(t *testing.T) {
	r := &Registry{}
	r.snap.Store(snapV1())

	// 127.0.0.1:1 is a closed port, so the Ping in loadRegistry fails fast (connection refused).
	err := r.Refresh("postgres://x:x@127.0.0.1:1/x?sslmode=disable&connect_timeout=1")
	if err == nil {
		t.Fatalf("expected Refresh to fail against a closed port")
	}
	if n, ok := r.Station("TR-IST-0001"); !ok || n != 2 {
		t.Fatalf("after failed refresh: Station = %d,%v; want 2,true (old snapshot kept)", n, ok)
	}
	if r.Len() != 1 {
		t.Fatalf("after failed refresh: Len = %d; want 1 (old snapshot kept)", r.Len())
	}
}
