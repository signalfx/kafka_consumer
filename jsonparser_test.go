package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
)

func Test_jsonParser_parse(t *testing.T) {
	type args struct {
		msg    []byte
		dps    chan *datapoint.Datapoint
		events chan *event.Event
	}
	tests := []struct {
		name    string
		j       *json
		args    args
		want    int
		wantDp  *datapoint.Datapoint
		wantErr bool
	}{
		{"nil", &json{}, args{[]byte(""), make(chan *datapoint.Datapoint), make(chan *event.Event)}, 0, nil, true},
		{"empty", &json{}, args{[]byte("{}"), make(chan *datapoint.Datapoint, 1), make(chan *event.Event, 1)},
			1, datapoint.New("", nil, datapoint.NewFloatValue(0), datapoint.Gauge, time.Unix(0, 0)), false},
		{"has values", &json{}, args{
			[]byte(`{"m": "metric", "t": 1563892873000, "v": 12.2, "dims": {"dim1": "3", "dim2": "4"}}`),
			make(chan *datapoint.Datapoint, 1),
			make(chan *event.Event, 1)},
			1,
			datapoint.New("metric", map[string]string{
				"dim1": "3",
				"dim2": "4",
			}, datapoint.NewFloatValue(12.2), datapoint.Gauge, time.Unix(1563892873, 0)),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.j.parse(tt.args.msg, tt.args.dps, tt.args.events)
			if (err != nil) != tt.wantErr {
				t.Errorf("jsonParser.parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("jsonParser.parse() = %v, want %v", got, tt.want)
			}
			if tt.wantDp != nil {
				gotDp := <-tt.args.dps
				assert.Equal(t, tt.wantDp, gotDp)
			}
		})
	}
}
