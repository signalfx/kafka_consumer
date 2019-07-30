package main

import (
	"errors"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
)

func Test(t *testing.T) {
	Convey("test kafka_consumer", t, func() {
		config := getTestConfig(t)
		So(config, ShouldNotBeNil)
		config.client.setTopics([]string{"blarg", "foo"})
		c := make(chan struct{})
		go func() {
			for atomic.LoadInt64(&setup) == 0 {
				runtime.Gosched()
			}
			mainInstance.sigs <- syscall.SIGTERM
			c <- struct{}{}
		}()
		main()
		<-c
		So(len(mainInstance.Datapoints()), ShouldEqual, 25)
		logIfErr("Print %s", errors.New("blarg"))
		config.offset = "oldest"
		So(config.getOffset(), ShouldEqual, sarama.OffsetOldest)
		config.offset = "blarg"
		So(config.getOffset(), ShouldEqual, sarama.OffsetNewest)
	})
}

func TestPostConsumer(t *testing.T) {
	Convey("test kafka post consumer", t, func() {
		c := getTestConfig(t)
		Convey("constructors", func() {
			So(c.postConfig(), ShouldBeNil)
			_, err := c.config.newClusterConstructor([]string{}, "", nil)
			So(err, ShouldNotBeNil)
			_, err = c.config.newClientConstructor([]string{}, nil)
			So(err, ShouldNotBeNil)
		})
		Convey("no required params", func() {
			c.kafkaBroker = ""
			So(c.postConfig(), ShouldEqual, errorRequiredOptions)
		})
	})
}
