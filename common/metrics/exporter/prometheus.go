package exporter

import (
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "dejaq"
)

var (
	hostname, _ = os.Hostname()

	registries = sync.Map{}
)

func GetBrokerCounter(metricName string, labelNames []string) *prometheus.CounterVec {
	return getAndRegisterCounterVec("broker", metricName, labelNames)
}

func GetConsumerCounter(metricName string, labelNames []string) *prometheus.CounterVec {
	return getAndRegisterCounterVec("consumer", metricName, labelNames)
}

func GetProducerCounter(metricName string, labelNames []string) *prometheus.CounterVec {
	return getAndRegisterCounterVec("producer", metricName, labelNames)
}

func GetBrokerGauge(metricName string, labelNames []string) *prometheus.GaugeVec {
	return getAndRegisterGaugeVec("broker", metricName, labelNames)
}

func GetConsumerGauge(metricName string, labelNames []string) *prometheus.GaugeVec {
	return getAndRegisterGaugeVec("consumer", metricName, labelNames)
}

func GetProducerGauge(metricName string, labelNames []string) *prometheus.GaugeVec {
	return getAndRegisterGaugeVec("producer", metricName, labelNames)
}

func getAndRegisterCounterVec(subsystem, metricName string, labelNames []string) *prometheus.CounterVec {

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      metricName,
		Subsystem: subsystem,
		Namespace: namespace,
		ConstLabels: prometheus.Labels{
			"hostname": hostname,
		},
	},
		labelNames,
	)
	prometheus.MustRegister()

	registry, _ := registries.LoadOrStore(subsystem, prometheus.NewRegistry())
	registry.(*prometheus.Registry).MustRegister(counter)
	return counter
}

func getAndRegisterGaugeVec(subsystem, metricName string, labelNames []string) *prometheus.GaugeVec{
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      metricName,
		Subsystem: subsystem,
		Namespace: namespace,
		ConstLabels: prometheus.Labels{
			"hostname": hostname,
		},
	},
		labelNames,
	)

	registry, _ := registries.LoadOrStore(subsystem, prometheus.NewRegistry())
	registry.(*prometheus.Registry).MustRegister(gauge)
	return gauge
}

func mergedMetrics(subsystem string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		promhttp.Handler().ServeHTTP(w, r)
		registry, _ := registries.LoadOrStore(subsystem, prometheus.NewRegistry())
		customHandler := promhttp.HandlerFor(registry.(*prometheus.Registry), promhttp.HandlerOpts{})
		customHandler.ServeHTTP(w, r)
	})
}

func GetDefaultExporter(port string) {
	server := http.NewServeMux()
	server.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(fmt.Sprintf(":%s", port), server)
}

func GetMetricsExporter(subsystem, port string) {
	server := http.NewServeMux()
	registry, _ := registries.LoadOrStore(subsystem, prometheus.NewRegistry())
	server.Handle("/metrics", promhttp.InstrumentMetricHandler(registry.(*prometheus.Registry), promhttp.HandlerFor(registry.(*prometheus.Registry), promhttp.HandlerOpts{})))
	http.ListenAndServe(fmt.Sprintf(":%s", port), server)
}
