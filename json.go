package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mailru/easyjson"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"

	"github.com/signalfx/gateway/protocol/signalfx"
	signalfxformat "github.com/signalfx/gateway/protocol/signalfx/format"
)

type json struct{}

func (j *json) parse(msg []byte, dps chan *datapoint.Datapoint, events chan *event.Event) (int, error) {
	var d signalfxformat.JSONDatapointV2
	if err := easyjson.Unmarshal(msg, &d); err != nil {
		return 0, err
	}
	count := 0
	for metricType, datapoints := range d {
		if len(datapoints) > 0 {
			mt, ok := com_signalfx_metrics_protobuf.MetricType_value[strings.ToUpper(metricType)]
			if !ok {
				log.Printf("Unknown metric type")
				continue
			}
			for _, jsonDatapoint := range datapoints {
				v, err := signalfx.ValueToValue(jsonDatapoint.Value)
				if err != nil {
					log.Printf("Unable to get value for datapoint: %v", err)
					continue
				}
				dps <- datapoint.New(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, fromMT(com_signalfx_metrics_protobuf.MetricType(mt)), fromTs(jsonDatapoint.Timestamp))
				count++
			}
		}
	}
	return count, nil
}

// Copied from github.com/signalfx/gateway
var fromMTMap = map[com_signalfx_metrics_protobuf.MetricType]datapoint.MetricType{
	com_signalfx_metrics_protobuf.MetricType_CUMULATIVE_COUNTER: datapoint.Counter,
	com_signalfx_metrics_protobuf.MetricType_GAUGE:              datapoint.Gauge,
	com_signalfx_metrics_protobuf.MetricType_COUNTER:            datapoint.Count,
}

func fromMT(mt com_signalfx_metrics_protobuf.MetricType) datapoint.MetricType {
	ret, exists := fromMTMap[mt]
	if exists {
		return ret
	}
	panic(fmt.Sprintf("Unknown metric type: %v\n", mt))
}

func fromTs(ts int64) time.Time {
	if ts > 0 {
		return time.Unix(0, ts*time.Millisecond.Nanoseconds())
	}
	return time.Now().Add(-time.Duration(time.Millisecond.Nanoseconds() * ts))
}
