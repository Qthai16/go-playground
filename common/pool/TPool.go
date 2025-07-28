package pool

import (
	"sync"
)

type TPoolConfig[T any] struct {
	Generate func() *T // contructor, new(T) is used if nil
	Reset    func(*T)  // called after get T* from pool
	Cleanup  func(*T)  // called before put T* to pool
}

// generic sync.Pool wrapper, refer from thrift.pool
type TPool[T any] struct {
	pool sync.Pool
	Conf TPoolConfig[T]
}

func NewTPool[T any](conf TPoolConfig[T]) *TPool[T] {
	if conf.Generate == nil {
		conf.Generate = func() *T {
			return new(T)
		}
	}
	return &TPool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				return conf.Generate()
			},
		},
		Conf: conf,
	}
}

func (p *TPool[T]) Get() *T {
	r := p.pool.Get().(*T)
	if p.Conf.Reset != nil {
		p.Conf.Reset(r)
	}
	return r
}

func (p *TPool[T]) Put(r **T) {
	if p.Conf.Cleanup != nil {
		p.Conf.Cleanup(*r)
	}
	p.pool.Put(*r)
	*r = nil
}
