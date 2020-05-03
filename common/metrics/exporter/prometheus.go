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

func GetBrokerSummary(metricName string, labelNames []string) *prometheus.SummaryVec {
	return getAndRegisterSummaryVec("broker", metricName, labelNames)
}

func GetConsumerSummary(metricName string, labelNames []string) *prometheus.SummaryVec {
	return getAndRegisterSummaryVec("consumer", metricName, labelNames)
}

func GetProducerSummary(metricName string, labelNames []string) *prometheus.SummaryVec {
	return getAndRegisterSummaryVec("producer", metricName, labelNames)
}

func GetBrokerHistogram(metricName string, labelNames []string) *prometheus.HistogramVec {
	return getAndRegisterHistogramVec("broker", metricName, labelNames)
}

func GetConsumerHistogram(metricName string, labelNames []string) *prometheus.HistogramVec {
	return getAndRegisterHistogramVec("consumer", metricName, labelNames)
}

func GetProducerHistogram(metricName string, labelNames []string) *prometheus.HistogramVec {
	return getAndRegisterHistogramVec("producer", metricName, labelNames)
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

func getAndRegisterSummaryVec(subsystem, metricName string, labelNames []string) *prometheus.SummaryVec {
	summary := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: metricName,
		Subsystem: subsystem,
		Namespace: namespace,
		ConstLabels: prometheus.Labels{
			hostname: hostname,
		},
		Objectives: map[float64]float64{0.5: 0.05, 0.75: 0.025, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001, 0.999: 0.0001},
	}, labelNames,
	)

	registry, _ := registries.LoadOrStore(subsystem, prometheus.NewRegistry())
	registry.(*prometheus.Registry).MustRegister(summary)
	return summary
}

func getAndRegisterHistogramVec(subsystem, metricName string, labelNames []string) *prometheus.HistogramVec {
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: metricName,
		Subsystem: subsystem,
		Namespace: namespace,
		ConstLabels: prometheus.Labels{
			hostname: hostname,
		},
		Buckets: []float64{10, 25, 50, 100, 200, 300, 400, 500, 750, 1000, 2000, 5000, 10000},  // expressed in units/MS not as a percentage
	},
		labelNames,
	)

	registry, _ := registries.LoadOrStore(subsystem, prometheus.NewRegistry())
	registry.(*prometheus.Registry).MustRegister(histogram)
	return histogram
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
