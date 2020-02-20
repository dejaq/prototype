package exporter

import (
	"net/http"
	"time"

	"github.com/rcrowley/go-metrics"

	. "github.com/mihaioprea/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "dejaq"
)

func SetupStandardMetricsExporter(subsystem string) {
	pClient := NewPrometheusProvider(metrics.DefaultRegistry, namespace, subsystem, prometheus.DefaultRegisterer, 1*time.Second)
	go pClient.UpdatePrometheusMetrics()

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
