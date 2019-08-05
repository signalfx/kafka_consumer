package main

import (
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/telegraf/plugins/outputs/signalfx"
)

type telegrafP struct {
	parser   parsers.Parser
	sfx *signalfx.SignalFx
}

func (t *telegrafP) parse(msg []byte, dps chan *datapoint.Datapoint, events chan *event.Event) (int, error) {
	metrics, err := t.parser.Parse(msg)
	if err != nil {
		return 0, err
	}

	t.sfx.GetObjects(metrics, dps, events)
	return len(metrics), nil
}
