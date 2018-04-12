package main

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/event"
	. "github.com/smartystreets/goconvey/convey"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

func TestHash(t *testing.T) {
	Convey("test hash", t, func() {
		arr := sortedDimensions(map[string]string{}, &[]string{})
		So(arr, ShouldNotBeNil)
		e := GoJavaStringEncoder{
			runes: make([]rune, 1),
			bytes: make([]byte, 1),
		}
		bb := e.Encode("several")
		So(bb, ShouldResemble, []byte{115, 0, 101, 0, 118, 0, 101, 0, 114, 0, 97, 0, 108, 0})
	})
}

func TestForwarder(t *testing.T) {
	Convey("test nohash", t, func() {
		c := getTestConfig(t)
		c1 := *c
		c1.useHashing = false
		f := newSignalFxForwarder(&c1.config)
		So(f.chans[0], ShouldBeNil)
		f.close()
	})
	Convey("test forwarder", t, func() {
		c := getTestConfig(t)
		f := newSignalFxForwarder(&c.config)
		Convey("test sorting", func() {
			y := []*datapoint.Datapoint{
				dptest.DP(),
				dptest.DP(),
			}
			y[0].Timestamp = y[0].Timestamp.Add(time.Second)
			So(y[0].Timestamp.UnixNano(), ShouldBeGreaterThan, y[1].Timestamp.UnixNano())
			sort.Sort(byTimestamp(y))
			So(y[1].Timestamp.UnixNano(), ShouldBeGreaterThan, y[0].Timestamp.UnixNano())
		})
		Convey("stuff being sent", func() {
			dps := f.Datapoints()
			So(len(dps), ShouldEqual, 20)
			for _, d := range dps {
				f.dps <- d
			}
			for {
				runtime.Gosched()
				errs := atomic.LoadInt64(&f.stats.numDpErrors)
				if errs > 0 {
					break
				}
			}
			f.evts <- &event.Event{Category: event.USERDEFINED}
			for {
				runtime.Gosched()
				errs := atomic.LoadInt64(&f.stats.numEErrors)
				if errs > 0 {
					break
				}
			}

		})
		Reset(func() {
			f.close()
		})
	})
}
