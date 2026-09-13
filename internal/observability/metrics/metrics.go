// Package metrics serves the Prometheus exposition endpoint (/metrics), the
// port of MetricsModule (@willsoto/nestjs-prometheus with defaultMetrics
// enabled). prom-client's default metrics are the Node runtime + process
// families; the Go equivalents are the Go runtime collector (go_goroutines,
// go_memstats_*, go_gc_duration_seconds, …) and the process collector
// (process_cpu_seconds_total, process_resident_memory_bytes, …).
package metrics

import (
	"net/http"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds every metric the process exposes. Application metrics
// register here too.
var Registry = newRegistry()

func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
	return reg
}

// Handler serves the registry in the Prometheus text format.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}

// FamilyNames lists the metric families currently exposed, sorted.
func FamilyNames() ([]string, error) {
	mfs, err := Registry.Gather()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	sort.Strings(names)
	return names, nil
}
