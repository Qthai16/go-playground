package pool

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/Qthai16/go-playground/common"
	"github.com/Qthai16/go-playground/utils"
	"github.com/apache/thrift/lib/go/thrift"
)

var (
	ErrInvalidParam   = errors.New("invalid param")
	ErrMaxConnReached = errors.New("max connection reached")
	ErrPoolClosed     = errors.New("pool is closed")
	ErrNoConnection   = errors.New("no connection")
	ErrBadConn        = errors.New("bad connection")
)

const (
	// todo: configurable
	defaultPoolSize  = 200
	defaultAliveIntv = 3 * time.Second
	defaultSlots     = 10
)

type Pool2Config struct {
	Host           string
	Proxy          string
	MaxOpenConn    int32
	AliveCheckIntv time.Duration // minimum wait time to pool reset connection
	ConnConf       *thrift.TConfiguration
}

func PoolDefaultConf(host, proxy string, connConf *thrift.TConfiguration) *Pool2Config {
	return &Pool2Config{
		Host:           host,
		Proxy:          proxy,
		MaxOpenConn:    defaultPoolSize,
		AliveCheckIntv: defaultAliveIntv,
		ConnConf:       connConf,
	}
}

type IThriftClient[T any] interface {
	Open() error
	Close() error
	GetTransport() thrift.TTransport
	GetCore() T
}

type IThriftClientFactory[T any] interface {
	NewClient(conf Pool2Config) (IThriftClient[T], error)
}

type IPool[T any] interface {
	Get() (IThriftClient[T], error)
	Put(IThriftClient[T])
	Len() int
	Destroy()
}

type ClientCtor = func(thrift.TClient) any

type ThriftClient[T any] struct {
	// config
	Addr     string
	Proxy    string
	connConf *thrift.TConfiguration
	cliCtor  ClientCtor
	// control
	Core  T // thrift client ptr
	Trans thrift.TTransport
}

func NewThriftClient[T any](addr string, conf *thrift.TConfiguration, cliCtor ClientCtor) (*ThriftClient[T], error) {
	// todo: check if T type is pointer
	if cliCtor == nil {
		return nil, ErrInvalidParam
	}
	client := &ThriftClient[T]{
		Addr:     addr,
		Proxy:    "",
		connConf: conf,
		cliCtor:  cliCtor,
		Trans:    nil,
	}
	return client, client.Open()
}

func NewThriftProxyClient[T any](addr, proxy string, conf *thrift.TConfiguration, cliCtor ClientCtor) (*ThriftClient[T], error) {
	if cliCtor == nil {
		return nil, ErrInvalidParam
	}
	client := &ThriftClient[T]{
		Addr:     addr,
		Proxy:    proxy,
		connConf: conf,
		cliCtor:  cliCtor,
		Trans:    nil,
	}
	return client, client.OpenProxy()
}

func (c *ThriftClient[T]) Open() error {
	socket := thrift.NewTSocketConf(c.Addr, c.connConf)
	trans := thrift.NewTFramedTransportConf(socket, c.connConf)
	iprot := thrift.NewTBinaryProtocolConf(trans, c.connConf)
	oprot := thrift.NewTBinaryProtocolConf(trans, c.connConf)
	c.Core = c.cliCtor(thrift.NewTStandardClient(iprot, oprot)).(T)
	c.Trans = trans
	return c.Trans.Open()
}

func (c *ThriftClient[T]) OpenProxy() error {
	socket := thrift.NewTSocketConf(c.Proxy, c.connConf)
	trans := thrift.NewTFramedTransportConf(socket, c.connConf)
	var iprot, oprot thrift.TProtocol
	iprot = thrift.NewTBinaryProtocolConf(trans, c.connConf)
	oprot = thrift.NewTBinaryProtocolConf(trans, c.connConf)
	oprot = thrift.NewTMultiplexedProtocol(oprot, c.Addr)
	c.Core = c.cliCtor(thrift.NewTStandardClient(iprot, oprot)).(T)
	c.Trans = trans
	return c.Trans.Open()
}

func (c *ThriftClient[T]) Close() error {
	if c.Trans != nil { // close anyway to avoid leak
		return c.Trans.Close()
	}
	return nil
}

func (c *ThriftClient[T]) GetTransport() thrift.TTransport {
	return c.Trans
}

func (c *ThriftClient[T]) GetCore() T {
	return c.Core
}

type ThriftClientFactory[T any] struct {
	ctor ClientCtor
}

func NewThriftClientFactory[T any](ctor ClientCtor) *ThriftClientFactory[T] {
	return &ThriftClientFactory[T]{
		ctor: ctor,
	}
}

func (f *ThriftClientFactory[T]) NewClient(conf Pool2Config) (IThriftClient[T], error) {
	if len(conf.Proxy) > 0 {
		return NewThriftProxyClient[T](conf.Host, conf.Proxy, conf.ConnConf, f.ctor)
	}
	return NewThriftClient[T](conf.Host, conf.ConnConf, f.ctor)
}

type Pool2[T any] struct {
	Pool2Config
	cliFactory   IThriftClientFactory[T]
	connections  chan IThriftClient[T]
	slots        chan struct{}
	numOpenConn  atomic.Int32
	isClosed     atomic.Bool
	isAlive      atomic.Bool
	lastDeadTime time.Time
}

func (p *Pool2[T]) Get() (IThriftClient[T], error) {
	if p.isClosed.Load() {
		return nil, ErrPoolClosed
	}
	if !p.isAlive.Load() {
		if time.Since(p.lastDeadTime) < p.AliveCheckIntv {
			return nil, ErrNoConnection
		} else {
			p.isAlive.CompareAndSwap(false, true)
		}
	}
	p.slots <- struct{}{}
	defer func() {
		<-p.slots
	}()
	select {
	case conn, ok := <-p.connections:
		if !ok {
			return nil, ErrPoolClosed
		}
		return conn, nil
	default:
		if p.numOpenConn.Load() < p.MaxOpenConn {
			conn, err := p.cliFactory.NewClient(p.Pool2Config)
			if err != nil {
				if p.isAlive.CompareAndSwap(true, false) {
					p.lastDeadTime = time.Now()
					utils.LogInfo("[pool2][%v] dead: %v", p.Host, p.lastDeadTime)
				}
				return nil, ErrNoConnection
			}
			p.isAlive.CompareAndSwap(false, true)
			p.numOpenConn.Add(1)
			return conn, nil
		}
		t := common.BorrowTimer(p.ConnConf.ConnectTimeout)
		defer common.ReturnTimer(t)
		select {
		case conn, ok := <-p.connections:
			if !ok {
				return nil, ErrPoolClosed
			}
			return conn, nil
		case <-t.C:
			return nil, ErrMaxConnReached
		}
	}
}

func (p *Pool2[T]) Put(conn IThriftClient[T]) {
	if conn == nil {
		return
	}
	if p.isClosed.Load() || !p.isAlive.Load() {
		conn.Close()
		p.numOpenConn.Add(-1)
		return
	}
	select {
	case p.connections <- conn:
		return
	default:
		conn.Close()
		p.numOpenConn.Add(-1)
	}
}

func (p *Pool2[T]) Len() int {
	return len(p.connections)
}

func (p *Pool2[T]) IsAlive() bool {
	return p.isAlive.Load()
}

func (p *Pool2[T]) CheckAlive() bool {
	if p.isClosed.Load() {
		return false
	}
	if p.isAlive.Load() {
		return true
	}
	if time.Since(p.lastDeadTime) < p.AliveCheckIntv {
		return false
	}
	select {
	case p.slots <- struct{}{}:
		defer func() {
			<-p.slots
		}()
		conn, err := p.cliFactory.NewClient(p.Pool2Config)
		if err != nil {
			// utils.LogInfo("[pool2][%v] dead: %v", p.Host, p.lastDeadTime)
			p.lastDeadTime = time.Now()
			return false
		} else {
			p.Put(conn)
			p.isAlive.CompareAndSwap(false, true)
			p.numOpenConn.Add(1)
			return true
		}
	default:
		return false
	}
}

func (p *Pool2[T]) Destroy() {
	if p.isClosed.CompareAndSwap(false, true) {
		close(p.connections)
		for conn := range p.connections {
			conn.Close()
			p.numOpenConn.Add(-1)
		}
		utils.LogInfo("[pool2][%v] pool is destroyed", p.Host)
	}
}

func validatePoolConf(conf *Pool2Config) error {
	// todo: check valid host format
	if conf == nil || conf.Host == "" || conf.MaxOpenConn <= 0 {
		return ErrInvalidParam
	}
	return nil
}

func (p *Pool2[T]) InvalidConn(conn IThriftClient[T]) {
	if conn == nil {
		return
	}
	p.numOpenConn.Add(-1)
	conn.Close()
	if p.numOpenConn.Load() == 0 && p.isAlive.CompareAndSwap(true, false) {
		p.lastDeadTime = time.Now()
		utils.LogInfo("[pool2][%v] dead: %v", p.Host, p.lastDeadTime)
	}
}

func (p *Pool2[T]) InvalidConn2(conn IThriftClient[T], err error) {
	// todo: treat protocol and application exception as no error
	// todo: handle transport exception too many open files
	if conn == nil || err == nil {
		return
	}
	var tec thrift.TTransportException
	if errors.As(err, &tec) {
		// assume that other conns in pool should be invalid too --> mark dead immediately
		if p.isAlive.CompareAndSwap(true, false) {
			for len(p.connections) > 0 { // empty cache conn
				c := <-p.connections
				c.Close()
				p.numOpenConn.Add(-1)
			}
			p.lastDeadTime = time.Now()
			utils.LogInfo("[pool2][%v] mark dead due to transport ec: %v, time: %v", p.Host, tec, p.lastDeadTime)
		}
	}
	p.numOpenConn.Add(-1)
	conn.Close()
	if p.numOpenConn.Load() == 0 && p.isAlive.CompareAndSwap(true, false) {
		p.lastDeadTime = time.Now()
		utils.LogInfo("[pool2][%v] dead: %v", p.Host, p.lastDeadTime)
	}
}

func (p *Pool2[T]) PutIfValid(conn IThriftClient[T], err error) {
	// put if no error, otherwise invalid the conn
	if err != nil {
		p.InvalidConn2(conn, err)
		return
	}
	p.Put(conn)
}

// default thrift client factory: construct ThriftClient[T]
func NewClientPool[T any](conf *Pool2Config, cliCtor ClientCtor) (*Pool2[T], error) {
	if err := validatePoolConf(conf); err != nil {
		utils.LogErro("[pool2] invalid pool config: %v", err)
		return nil, err
	}
	return &Pool2[T]{
		Pool2Config:  *conf,
		cliFactory:   NewThriftClientFactory[T](cliCtor),
		connections:  make(chan IThriftClient[T], conf.MaxOpenConn),
		slots:        make(chan struct{}, defaultSlots),
		numOpenConn:  atomic.Int32{},
		isClosed:     atomic.Bool{},
		isAlive:      atomic.Bool{},
		lastDeadTime: time.Time{},
	}, nil
}

// user provided thrift client factory
func NewClientPoolFactory[T any](conf *Pool2Config, clientFactory IThriftClientFactory[T]) (*Pool2[T], error) {
	if err := validatePoolConf(conf); err != nil {
		utils.LogErro("[pool2] invalid pool config: %v", err)
		return nil, err
	}
	return &Pool2[T]{
		Pool2Config:  *conf,
		cliFactory:   clientFactory,
		connections:  make(chan IThriftClient[T], conf.MaxOpenConn),
		slots:        make(chan struct{}, defaultSlots),
		numOpenConn:  atomic.Int32{},
		isClosed:     atomic.Bool{},
		isAlive:      atomic.Bool{},
		lastDeadTime: time.Time{},
	}, nil
}

var (
	_ IThriftClient[any]        = (*ThriftClient[any])(nil)
	_ IThriftClientFactory[any] = (*ThriftClientFactory[any])(nil)
	_ IPool[any]                = (*Pool2[any])(nil)
)
