package main

import (
	"fmt"
	"time"

	"github.com/alphacentaurigames/conquest-alpha-go/lib/worker"
)

func doStuff(work interface{}) {
	fmt.Println(work.(int))
	time.Sleep(time.Millisecond * 10)
}

func main() {
	p := worker.NewPool(
		doStuff,
		worker.WithLimits(5, 1, 1000),
		worker.WithAutoScale(time.Second, 5),
		worker.WithCheckInterval(time.Second),
	)

	for {
		for i := 0; i < 10000; i++ {
			p.Queue(i)
		}

		time.Sleep(time.Second * 10)
	}
}
