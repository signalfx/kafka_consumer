package main

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
)

type writer interface {
	close()
	Datapoints() []*datapoint.Datapoint
}

type stdoutWriter struct {
	done chan struct{}
}

type nullWriter struct {
	done chan struct{}
}

func (*nullWriter) Datapoints() []*datapoint.Datapoint {
	return nil
}

func (n *nullWriter) close() {
	close(n.done)
}

func (s *stdoutWriter) Datapoints() []*datapoint.Datapoint {
	return nil
}

func newStdoutWriter(dps chan *datapoint.Datapoint, evts chan *event.Event) writer {
	w := &stdoutWriter{done: make(chan struct{})}
	go func() {
		for {
			select {
			case <-w.done:
				return
			case dp := <-dps:
				println(dp.String())
			case evt := <-evts:
				println(evt.String())
			}
		}
	}()
	return w
}

func (s *stdoutWriter) close() {
	close(s.done)
}

func newNullWriter(dps chan *datapoint.Datapoint, evts chan *event.Event) writer {
	w := &nullWriter{done: make(chan struct{})}
	go func() {
		for {
			select {
			case <-w.done:
				return
			case _ = <-dps:
			case _ = <-evts:
			}
		}
	}()
	return w
}
