package main

import (
	"log"
	"time"

	"github.com/mertenvg/worker"
)

func doStuff(work interface{}) {
	//fmt.Println(work.(int))
	time.Sleep(time.Millisecond * 100)
}

func main() {
	p := worker.NewPool(
		doStuff,
		worker.WithLimits(5, 1, 1000),
		worker.WithAutoScale(time.Second, 5),
		worker.WithCheckInterval(time.Second),
		worker.WithLogFunc(log.Printf),
	)

	work := make([]interface{}, 0)
	for w := 0; w < 100; w++ {
		work = append(work, w)
	}

	for i := 0; i < 100; i = i + 10 {
		p.Queue(work...)
	}

	p.Stop()
}
