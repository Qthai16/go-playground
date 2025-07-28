package common

import (
	"log"
	"sync"
	"time"
)

var _TimerPool sync.Pool

func BorrowTimer(d time.Duration) *time.Timer {
	x := _TimerPool.Get()
	if x == nil {
		return time.NewTimer(d)
	}
	t := x.(*time.Timer)
	if t.Reset(d) {
		log.Fatalln(`[timer_pool] pool return an active timer.
		please, take a cup of coffee, draw flowchart, start debugging and think about what fucking are you doing`)
	}
	return t
}

func ReturnTimer(t *time.Timer) {
	if !t.Stop() && len(t.C) != 0 {
		<-t.C
	}
	_TimerPool.Put(t)
}
