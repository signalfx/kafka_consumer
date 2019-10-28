package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
)

func TestJSONParse(t *testing.T) {
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
			0, nil, false},
		{"has values", &json{}, args{
			[]byte(`{"gauge":[{"metric":"fci-api-rest-status","value":1,"dimensions":{"http_status_code":"429"},"timestamp":1572290806420}]}`),
			make(chan *datapoint.Datapoint, 1),
			make(chan *event.Event, 1)},
			1,
			datapoint.New("fci-api-rest-status", map[string]string{
				"http_status_code": "429",
			}, datapoint.NewIntValue(1), datapoint.Gauge, time.Unix(0, 1572290806420*int64(time.Millisecond))),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.j.parse(tt.args.msg, tt.args.dps, tt.args.events)
			if (err != nil) != tt.wantErr {
				t.Errorf("json.parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("json.parse() = %v, want %v", got, tt.want)
			}
			if tt.wantDp != nil {
				gotDp := <-tt.args.dps
				assert.Equal(t, tt.wantDp, gotDp)
			}
		})
	}
}
