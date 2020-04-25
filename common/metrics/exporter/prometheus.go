package exporter

import (
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "dejaq"
)

var (
	_subsystem  = ""
	hostname, _ = os.Hostname()
)

func GetAndRegisterCounterVec(metricName string, labelNames []string) *prometheus.CounterVec {
	return promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      metricName,
		Subsystem: _subsystem,
		Namespace: namespace,
		ConstLabels: prometheus.Labels{
			"hostname": hostname,
		},
	},
		labelNames,
	)
}

func GetAndRegisterGaugeVec(metricName string, labelNames []string) *prometheus.GaugeVec{
	return promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:      metricName,
		Subsystem: _subsystem,
		Namespace: namespace,
		ConstLabels: prometheus.Labels{
			"hostname": hostname,
		},
	},
		labelNames,
	)
}

func SetupStandardMetricsExporter(subsystem string) {
	_subsystem = subsystem

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
