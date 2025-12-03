package worker

import (
	"context"
	"sync"
)

type TaskFunc func(context.Context)

type Pool struct {
	size   int
	tasks  chan TaskFunc
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewPool(size int) *Pool {
	if size <= 0 {
		size = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		size:   size,
		tasks:  make(chan TaskFunc, size*2),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (p *Pool) Start() {
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case fn := <-p.tasks:
					if fn != nil {
						fn(p.ctx)
					}
				}
			}
		}()
	}
}

func (p *Pool) Submit(fn TaskFunc) {
	select {
	case <-p.ctx.Done():
		return
	case p.tasks <- fn:
	}
}

func (p *Pool) Stop() {
	p.cancel()
	p.wg.Wait()
}
