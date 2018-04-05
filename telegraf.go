package main

import (
	"fmt"
	"log"
	"reflect"

	"github.com/influxdata/telegraf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"time"
)

type telegrafToSfx struct {
	Exclude []string
	Include []string
}

func newTelegraf() *telegrafToSfx {
	return &telegrafToSfx{
		Exclude: []string{""},
		Include: []string{""},
	}
}

/*Determine and assign a datapoint metric type based on telegraf metric type*/
func getMetricType(metric telegraf.Metric) (metricType datapoint.MetricType, err error) {
	switch metric.Type() {
	case telegraf.Counter:
		metricType = datapoint.Counter
		if metric.Name() == "mem" {
			metricType = datapoint.Gauge
		}
	case telegraf.Gauge:
		metricType = datapoint.Gauge
	case telegraf.Summary, telegraf.Histogram, telegraf.Untyped:
		metricType = datapoint.Gauge
		err = fmt.Errorf("histogram, summary, and untyped metrics will be sent as gauges")
	default:
		metricType = datapoint.Gauge
		err = fmt.Errorf("unrecognized metric type defaulting to gauge")
	}
	return metricType, err
}

/*Determine and assign a datapoint metric type based on telegraf metric type*/
func getMetricTypeAsString(metric telegraf.Metric) (metricType string, err error) {
	switch metric.Type() {
	case telegraf.Counter:
		metricType = "counter"
	case telegraf.Gauge:
		metricType = "gauge"
	case telegraf.Summary:
		metricType = "summary"
		err = fmt.Errorf("summary metrics will be sent as gauges")
	case telegraf.Histogram:
		metricType = "histogram"
		err = fmt.Errorf("histogram metrics will be sent as gauges")
	case telegraf.Untyped:
		metricType = "untyped"
		err = fmt.Errorf("untyped metrics will be sent as gauges")
	default:
		metricType = "unrecognized"
		err = fmt.Errorf("unrecognized metric type defaulting to gauge")
	}
	return metricType, err
}

func getIntegerValue(value interface{}) datapoint.Value {
	var metricValue datapoint.Value
	switch value.(type) {
	case int64:
		metricValue = datapoint.NewIntValue(value.(int64))
	case int32:
		metricValue = datapoint.NewIntValue(int64(value.(int32)))
	case int16:
		metricValue = datapoint.NewIntValue(int64(value.(int16)))
	case int8:
		metricValue = datapoint.NewIntValue(int64(value.(int8)))
	case int:
		metricValue = datapoint.NewIntValue(int64(value.(int)))
	}
	return metricValue
}

func getUnsignedIntegerValue(value interface{}) datapoint.Value {
	var metricValue datapoint.Value
	switch value.(type) {
	case uint64:
		metricValue = datapoint.NewIntValue(int64(value.(uint64)))
	case uint32:
		metricValue = datapoint.NewIntValue(int64(value.(uint32)))
	case uint16:
		metricValue = datapoint.NewIntValue(int64(value.(uint16)))
	case uint8:
		metricValue = datapoint.NewIntValue(int64(value.(uint8)))
	case uint:
		metricValue = datapoint.NewIntValue(int64(value.(uint)))
	}
	return metricValue
}

func getFloatingValue(value interface{}) datapoint.Value {
	var metricValue datapoint.Value
	switch value.(type) {
	case float64:
		metricValue = datapoint.NewFloatValue(value.(float64))
	case float32:
		metricValue = datapoint.NewFloatValue(float64(value.(float32)))
	}
	return metricValue
}

/*Determine and assign the datapoint value based on the telegraf value type*/
func getMetricValue(metric telegraf.Metric,
	field string) (datapoint.Value, error) {
	var err error
	var metricValue datapoint.Value
	var value = metric.Fields()[field]
	switch value.(type) {
	case int64, int32, int16, int8, int:
		metricValue = getIntegerValue(value)
	case uint64, uint32, uint16, uint8, uint:
		metricValue = getUnsignedIntegerValue(value)
	case float64, float32:
		metricValue = getFloatingValue(value)
	default:
		err = fmt.Errorf("unknown metric value type %s", reflect.TypeOf(value))
	}
	return metricValue, err
}

func parseMetricType(metric telegraf.Metric) (metricType datapoint.MetricType, metricTypeString string) {
	var err error
	// Parse the metric type
	metricType, err = getMetricType(metric)
	if err != nil {
		log.Printf("D! Outputs [signalfx] getMetricType() %s {%s}\n", err, metric)
	}
	metricTypeString, err = getMetricTypeAsString(metric)
	if err != nil {
		log.Printf("D! Outputs [signalfx] getMetricTypeAsString()  %s {%s}\n", err, metric)
	}
	return metricType, metricTypeString
}

func getMetricName(metric telegraf.Metric, field string, dims map[string]string, props map[string]interface{}) string {
	var name = metric.Name()

	// If sf_prefix is provided
	if metric.HasTag("sf_prefix") {
		name = dims["sf_prefix"]
	}

	// Include field when it adds to the metric name
	if field != "value" {
		name = name + "." + field
	}

	// If sf_metric is provided
	if metric.HasTag("sf_metric") {
		// If sf_metric is provided
		name = dims["sf_metric"]
	}

	return name
}

// Modify the dimensions of the metric according to the following rules
func modifyDimensions(name string, metricTypeString string, dims map[string]string, props map[string]interface{}) error {
	var err error
	// Add common dimensions
	dims["agent"] = "telegraf"
	dims["telegraf_type"] = metricTypeString

	// If the plugin doesn't define a plugin name use metric.Name()
	if _, in := dims["plugin"]; !in {
		dims["plugin"] = name
	}

	// remove sf_prefix if it exists in the dimension map
	if _, in := dims["sf_prefix"]; in {
		delete(dims, "sf_prefix")
	}

	// if sfMetric exists
	if sfMetric, in := dims["sf_metric"]; in {
		// if the metric is a metadata object
		if sfMetric == "objects.host-meta-data" {
			// If property exists remap it
			if _, in := dims["property"]; in {
				props["property"] = dims["property"]
				delete(dims, "property")
			} else {
				// This is a malformed metadata event
				err = fmt.Errorf("[signalfx] objects.host-metadata object doesn't have a property")
			}
			// remove the sf_metric dimension
			delete(dims, "sf_metric")
		}
	}
	return err
}

func (s *telegrafToSfx) shouldSkipMetric(metricName string, metricTypeString string, metricDims map[string]string, metricProps map[string]interface{}) bool {
	// Check if the metric is explicitly excluded
	if excluded := s.isExcluded(metricName); excluded {
		log.Println("D! Outputs [signalfx] excluding the following metric: ", metricName)
		return true
	}

	// Modify the dimensions of the metric and skip the metric if the dimensions are malformed
	if err := modifyDimensions(metricName, metricTypeString, metricDims, metricProps); err != nil {
		return true
	}

	return false
}

func (s *telegrafToSfx) parse(metrics []telegraf.Metric, dChan chan *datapoint.Datapoint, eChan chan *event.Event) error {
	var err error
	for _, metric := range metrics {
		var timestamp = metric.Time()
		var metricType datapoint.MetricType
		var metricTypeString string

		metricType, metricTypeString = parseMetricType(metric)

		for field := range metric.Fields() {
			var metricValue datapoint.Value
			var metricName string
			var metricProps = make(map[string]interface{})
			var metricDims = metric.Tags()

			// Get metric name
			metricName = getMetricName(metric, field, metricDims, metricProps)

			if s.shouldSkipMetric(metric.Name(), metricTypeString, metricDims, metricProps) {
				continue
			}

			// Get the metric value as a datapoint value
			if metricValue, err = getMetricValue(metric, field); err == nil {
				doDp(metricName, metricDims, metricValue, metricType, timestamp, dChan)
			} else {
				// Skip if it's not an sfx metric and it's not included
				if _, isSFX := metric.Tags()["sf_metric"]; !isSFX && !s.isIncluded(metricName) {
					continue
				}

				// We've already type checked field, so set property with value
				metricProps["message"] = metric.Fields()[field]
				var ev = event.NewWithProperties(metricName,
					event.AGENT,
					metricDims,
					metricProps,
					timestamp)

				// log event
				log.Println("D! Output [signalfx] ", ev.String())

				// Add event we want it to block
				eChan <- ev
			}
		}
	}
	return nil
}

func doDp(metricName string, metricDims map[string]string, metricValue datapoint.Value, metricType datapoint.MetricType, timestamp time.Time, dChan chan *datapoint.Datapoint) {
	var dp = datapoint.New(metricName,
		metricDims,
		metricValue.(datapoint.Value),
		metricType,
		timestamp)
	// log metric
	log.Println("D! Output [signalfx] ", dp.String())
	// Add metric as a datapoint we want it to block
	dChan <- dp
}

// isExcluded - checks whether a metric name was put on the exclude list
func (s *telegrafToSfx) isExcluded(name string) bool {
	for _, exclude := range s.Exclude {
		if name == exclude {
			return true
		}
	}
	return false
}

// isIncluded - checks whether a metric name was put on the include list
func (s *telegrafToSfx) isIncluded(name string) bool {
	for _, include := range s.Include {
		if name == include {
			return true
		}
	}
	return false
}
