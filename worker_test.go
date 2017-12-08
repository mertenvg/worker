package worker

import (
	"log"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	expected := 100
	count := 0
	p := NewPool(
		func(work interface{}) {
			count++
			time.Sleep(time.Millisecond * 100)
		},
		WithLimits(5, 1, 10),
		WithAutoScale(time.Millisecond, 1),
		WithLogFunc(log.Printf),
		WithCheckInterval(time.Millisecond*10),
	)

	for i := 0; i < expected; i++ {
		p.Queue(i)
	}

	p.Shrink(1)

	p.Stop()

	if count != expected {
		t.Errorf("Expected work count to be %v but got %v", expected, count)
	}
}
