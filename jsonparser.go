package main

import (
	"fmt"
	"github.com/json-iterator/go"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"time"
)

type json struct {
}

type message struct {
	M string `json:"m"`
	T int64 `json:"t"`
	V float64 `json:"v"`
	Dt *string `json:"dt"`
	Dims map[string]string `json:"dims"`
}

func (j *json) parse(msg []byte, dps chan *datapoint.Datapoint, events chan *event.Event) (int, error) {
	m := message{}
	if err := jsoniter.Unmarshal(msg, &m); err != nil {
		return 0, err
	}
	dpType := datapoint.Gauge
	if m.Dt != nil {
		switch *m.Dt {
		case "gauge":
			dpType = datapoint.Gauge
		case "counter":
			dpType = datapoint.Counter
		default:
			return 0, fmt.Errorf("unknown datapoint type %s", *m.Dt)
		}
	}
	dps <- datapoint.New(m.M, m.Dims, datapoint.NewFloatValue(m.V), dpType, time.Unix(0, m.T * 1000000))
	return 1, nil
}
