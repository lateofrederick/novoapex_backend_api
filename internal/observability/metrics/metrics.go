package metrics

import (
	"net/http"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var NodeDefaultMetricFamilies = []string{
	"nodejs_active_handles",
	"nodejs_active_handles_total",
	"nodejs_active_requests",
	"nodejs_active_requests_total",
	"nodejs_active_resources",
	"nodejs_active_resources_total",
	"nodejs_eventloop_lag_max_seconds",
	"nodejs_eventloop_lag_mean_seconds",
	"nodejs_eventloop_lag_min_seconds",
	"nodejs_eventloop_lag_p50_seconds",
	"nodejs_eventloop_lag_p90_seconds",
	"nodejs_eventloop_lag_p99_seconds",
	"nodejs_eventloop_lag_seconds",
	"nodejs_eventloop_lag_stddev_seconds",
	"nodejs_external_memory_bytes",
	"nodejs_gc_duration_seconds",
	"nodejs_heap_size_total_bytes",
	"nodejs_heap_size_used_bytes",
	"nodejs_heap_space_size_available_bytes",
	"nodejs_heap_space_size_total_bytes",
	"nodejs_heap_space_size_used_bytes",
	"nodejs_version_info",
	"process_cpu_seconds_total",
	"process_cpu_system_seconds_total",
	"process_cpu_user_seconds_total",
	"process_resident_memory_bytes",
	"process_start_time_seconds",
}

var PortedCustomAppMetrics = []string{}

var KnownGaps = []string{
	"grafana_dashboard_http_request_duration_seconds_served_by_neither_node_nor_go",
	"grafana_loki_label_service_equals_novoapex_api_but_promtail_assigns_compose_service_key_api",
	"go_collector_families_go_goroutines_go_memstats_not_served_by_node_so_not_registered_here",
	"nodejs_runtime_and_heap_and_eventloop_families_have_no_go_equivalent_in_this_registry",
	"nodejs_gc_duration_seconds_histogram_has_no_same_name_go_counterpart",
	"process_cpu_user_system_split_not_exposed_by_go_process_collector",
	"process_heap_bytes_served_by_node_linux_only_not_exposed_by_go_client",
	"promtail_timestamp_stage_parses_rfc3339nano_but_pino_time_field_is_epoch_milliseconds",
}

var Registry = newParityRegistry()

func newParityRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}

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
