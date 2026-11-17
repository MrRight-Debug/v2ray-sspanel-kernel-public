//go:build pprof

package main

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	_ "net/http/pprof"
)

func init() {
	address := os.Getenv("V2RAY_DEBUG_METRICS_ADDR")
	if address == "" {
		return
	}
	http.HandleFunc("/debug/go-metrics", func(response http.ResponseWriter, _ *http.Request) {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]interface{}{
			"gomaxprocs":      runtime.GOMAXPROCS(0),
			"num_goroutine":   runtime.NumGoroutine(),
			"num_gc":          stats.NumGC,
			"gc_cpu_fraction": stats.GCCPUFraction,
			"pause_total_ns":  stats.PauseTotalNs,
			"heap_alloc":      stats.HeapAlloc,
			"heap_inuse":      stats.HeapInuse,
			"heap_idle":       stats.HeapIdle,
			"heap_released":   stats.HeapReleased,
			"sys":             stats.Sys,
			"next_gc":         stats.NextGC,
			"mallocs":         stats.Mallocs,
			"frees":           stats.Frees,
		})
	})
	go func() {
		_ = http.ListenAndServe(address, nil)
	}()
}
