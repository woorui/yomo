package main

import (
	"context"
	"fmt"
	"time"

	y3 "github.com/yomorun/y3-codec-golang"
	"github.com/yomorun/yomo/pkg/rx"
)

var printer = func(_ context.Context, i interface{}) (interface{}, error) {
	value := i.(float32) / 10
	fmt.Println("serverless get value:", value)
	return value, nil
}

var callback = func(v []byte) (interface{}, error) {
	return y3.ToFloat32(v)
}

// Handler will handle data in Rx way
func Handler(rxstream rx.RxStream) rx.RxStream {
	stream := rxstream.
		Subscribe(0x10).
		OnObserve(callback).
		AuditTime(100 * time.Millisecond).
		Map(printer).
		StdOut()

	return stream
}
