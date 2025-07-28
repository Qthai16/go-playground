package pool

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/thrift/lib/go/thrift"
)

var (
	_PoolManager *Pool2Manager
)

type PoolHandle[T any] struct {
	p           IPool[T]
	refCnt      atomic.Int64
	isDestroyed atomic.Int32
	useProxy    bool
}

func NewPoolHandle[T any](host, proxy string, connConf *thrift.TConfiguration, cliCtor ClientCtor) (*PoolHandle[T], error) {
	p, err := NewClientPool[T](PoolDefaultConf(host, proxy, connConf), cliCtor)
	if err != nil {
		return nil, err
	}
	ph := &PoolHandle[T]{
		p:           p,
		refCnt:      atomic.Int64{},
		isDestroyed: atomic.Int32{},
		useProxy:    len(proxy) > 0,
	}
	ph.refCnt.Add(1)
	return ph, nil
}

func NewPoolHandleFactory[T any](host string, connConf *thrift.TConfiguration, clientFactory IThriftClientFactory[T]) (*PoolHandle[T], error) {
	p, err := NewClientPoolFactory[T](PoolDefaultConf(host, "", connConf), clientFactory)
	if err != nil {
		return nil, err
	}
	ph := &PoolHandle[T]{
		p:        p,
		refCnt:   atomic.Int64{},
		useProxy: false,
	}
	ph.refCnt.Add(1)
	return ph, nil
}

type Pool2Manager struct {
	Pools map[string]any
	mu    sync.Mutex
}

func NewPoolManager() *Pool2Manager {
	return &Pool2Manager{
		Pools: make(map[string]any),
		mu:    sync.Mutex{},
	}
}

func GetOrCreatePool[T any](p *Pool2Manager, host string, connConf *thrift.TConfiguration, cliCtor ClientCtor) (any, error) {
	// use type_host as unique key
	uniqKey := fmt.Sprintf("%T_%v", *new(T), strings.TrimSpace(host))
	p.mu.Lock()
	defer p.mu.Unlock()
	if handle, ok := p.Pools[uniqKey]; ok {
		handle := handle.(*PoolHandle[T])
		handle.refCnt.Add(1)
		return handle.p, nil
	}
	handle, err := NewPoolHandle[T](host, "", connConf, cliCtor)
	if err != nil {
		return nil, err
	}
	p.Pools[uniqKey] = handle
	return handle.p, nil
}

func GetOrCreateProxyPool[T any](p *Pool2Manager, host, proxy string, connConf *thrift.TConfiguration, cliCtor ClientCtor) (any, error) {
	host = strings.TrimSpace(host)
	proxy = strings.TrimSpace(proxy)
	p.mu.Lock()
	defer p.mu.Unlock()
	name := fmt.Sprintf("%T_%v_%v", *new(T), host, proxy)
	if handle, ok := p.Pools[name]; ok {
		handle := handle.(*PoolHandle[T])
		handle.refCnt.Add(1)
		return handle.p, nil
	}
	handle, err := NewPoolHandle[T](host, proxy, connConf, cliCtor)
	if err != nil {
		return nil, err
	}
	p.Pools[host] = handle
	return handle.p, nil
}

func GetOrCreatePoolFactory[T any](p *Pool2Manager, host string, connConf *thrift.TConfiguration, clientFactory IThriftClientFactory[T]) (any, error) {
	uniqKey := fmt.Sprintf("%T_%v", *new(T), strings.TrimSpace(host))
	p.mu.Lock()
	defer p.mu.Unlock()
	if handle, ok := p.Pools[uniqKey]; ok {
		handle := handle.(*PoolHandle[T])
		handle.refCnt.Add(1)
		return handle.p, nil
	}
	handle, err := NewPoolHandleFactory[T](host, connConf, clientFactory)
	if err != nil {
		return nil, err
	}
	p.Pools[uniqKey] = handle
	return handle.p, nil
}

func deletePool[T any](p *Pool2Manager, name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if handle, ok := p.Pools[name]; ok {
		handle := handle.(*PoolHandle[T])
		handle.refCnt.Add(-1)
		if handle.refCnt.Load() == 0 && handle.isDestroyed.CompareAndSwap(0, 1) {
			handle.p.Destroy()
			delete(p.Pools, name)
		}
	}
}

func DeletePool[T any](p *Pool2Manager, host string) {
	deletePool[T](p, host)
}

func DeleteProxyPool[T any](p *Pool2Manager, host, proxy string) {
	name := fmt.Sprintf("%T_%v_%v", *new(T), host, proxy)
	deletePool[T](p, name)
}

func GetPoolManager() *Pool2Manager {
	if _PoolManager == nil {
		_PoolManager = NewPoolManager()
	}
	return _PoolManager
}
