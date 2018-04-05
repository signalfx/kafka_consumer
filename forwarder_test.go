package main

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"time"
)

func Test(t *testing.T) {
	y := []*datapoint.Datapoint{
		dptest.DP(),
		dptest.DP(),
	}
	y[0].Timestamp = y[0].Timestamp.Add(time.Second)
	assert.True(t, y[0].Timestamp.UnixNano() > y[1].Timestamp.UnixNano())
	sort.Sort(byTimestamp(y))
	assert.True(t, y[1].Timestamp.UnixNano() > y[0].Timestamp.UnixNano())
}
