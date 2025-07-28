package proxyclient

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/Qthai16/go-playground/cmd/thriftgw/message"
	"github.com/Qthai16/go-playground/common/pool"
	"github.com/apache/thrift/lib/go/thrift"
)

var (
	errTransportNotOpen = fmt.Errorf("transport not open")
)

type ResponseCallback func(ctx context.Context, msg message.TMessage, proto thrift.TProtocol) error
type ErrorCallback func(ctx context.Context, msg message.TMessage, proto thrift.TProtocol) error

func defaultCallback(ctx context.Context, msg message.TMessage, proto thrift.TProtocol) error {
	return msg.Write(ctx, proto)
}

func PrependProxyError(err error) error {
	return thrift.PrependError("proxy error: ", err)
}

func treatEofErrAsNil(err error) error {
	// copy from thrift simple_server.go
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	var te thrift.TTransportException
	if errors.As(err, &te) && (te.TypeId() == thrift.END_OF_FILE || te.TypeId() == thrift.NOT_OPEN) {
		return nil
	}
	return err
}

// because ProxyThriftClientCore dont have any specific method like other thrift clients, we alias its Core type to interface{}
type ProxyThriftClientCore = interface{}

type TThriftClient interface {
	pool.IThriftClient[ProxyThriftClientCore]
	Call(ctx context.Context, msg message.TMessage, hostOProto thrift.TProtocol) error
}

type ProxyThriftClient struct {
	destTrans        thrift.TTransport
	destIproto       thrift.TProtocol
	destOproto       thrift.TProtocol
	responseCallback ResponseCallback
	errorCallback    ErrorCallback
}

var _ TThriftClient = (*ProxyThriftClient)(nil)

func NewProxyThriftClient(addr string, connConf *thrift.TConfiguration) (c *ProxyThriftClient, err error) {
	//fmt.Printf("[debug] create proxy client to addr: %v\n", addr)
	socket := thrift.NewTSocketConf(addr, connConf)
	trans := thrift.NewTFramedTransportConf(socket, connConf)
	iprot := thrift.NewTBinaryProtocolConf(trans, connConf)
	oprot := thrift.NewTBinaryProtocolConf(trans, connConf)
	return &ProxyThriftClient{
		destTrans:        trans,
		destIproto:       iprot,
		destOproto:       oprot,
		responseCallback: defaultCallback,
		errorCallback:    defaultCallback,
	}, trans.Open()
}

func NewProxyThriftClient2(addr string, connConf *thrift.TConfiguration, resCb ResponseCallback, errCb ErrorCallback) (c *ProxyThriftClient, err error) {
	socket := thrift.NewTSocketConf(addr, connConf)
	trans := thrift.NewTFramedTransportConf(socket, connConf)
	iprot := thrift.NewTBinaryProtocolConf(trans, connConf)
	oprot := thrift.NewTBinaryProtocolConf(trans, connConf)
	return &ProxyThriftClient{
		destTrans:        trans,
		destIproto:       iprot,
		destOproto:       oprot,
		responseCallback: resCb,
		errorCallback:    errCb,
	}, trans.Open()
}

func (p *ProxyThriftClient) Open() error {
	if p.destTrans.IsOpen() {
		return nil
	}
	return p.destTrans.Open()
}

func (p *ProxyThriftClient) Close() error {
	if p.destTrans != nil {
		return p.destTrans.Close()
	}
	return nil
}

func (p *ProxyThriftClient) GetTransport() thrift.TTransport {
	return p.destTrans
}

func (p *ProxyThriftClient) GetCore() ProxyThriftClientCore {
	return nil
}

func (p *ProxyThriftClient) Send(ctx context.Context, msg message.TMessage) error {
	return msg.Write(ctx, p.destOproto)
}

func (p *ProxyThriftClient) Recv(ctx context.Context, msg message.TMessage) error {
	return msg.Read(ctx, p.destIproto)
}

// call implement TThriftClient Call
func (p *ProxyThriftClient) Call(ctx context.Context, msg message.TMessage, hostOProto thrift.TProtocol) (err error) {
	defer func() {
		err = treatEofErrAsNil(err)
		if err == nil {
			return
		}
		if msg, ok := msg.(*message.ThriftMessage); ok {
			errmsg := message.NewThriftErrorMessage(msg.Header.Name, msg.Header.SeqId, err)
			p.errorCallback(ctx, errmsg, hostOProto) // exception, write exception to host connection
			return
		}
		panic("not a thrift message")
	}()
	if p.Open() != nil {
		return PrependProxyError(errTransportNotOpen)
	}
	if err := p.Send(ctx, msg); err != nil {
		return err
	}
	if msg.IsOneway() {
		return nil
	}
	// read response/error message from destination connection
	outMessage := message.NewThriftMessage()
	if err := p.Recv(ctx, outMessage); err != nil {
		return err
	}
	// forward response/error message to host connection
	return p.responseCallback(ctx, outMessage, hostOProto)
}

type ProxyThriftClientFactory struct {
}

var _ pool.IThriftClientFactory[ProxyThriftClientCore] = (*ProxyThriftClientFactory)(nil)

func NewProxyThriftClientFactory() *ProxyThriftClientFactory {
	return &ProxyThriftClientFactory{}
}

func (p *ProxyThriftClientFactory) NewClient(conf pool.Pool2Config) (pool.IThriftClient[ProxyThriftClientCore], error) {
	return NewProxyThriftClient(conf.Host, conf.ConnConf)
}

type ProxyThriftClientPool struct {
	Pool *pool.Pool2[ProxyThriftClientCore]
}

func NewProxyThriftClientPool(host string, connConf *thrift.TConfiguration) (*ProxyThriftClientPool, error) {
	clientFactory := NewProxyThriftClientFactory()
	cliPool, err := pool.GetOrCreatePoolFactory(pool.GetPoolManager(), host, connConf, clientFactory)
	if err != nil {
		return nil, err
	}
	return &ProxyThriftClientPool{
		Pool: cliPool.(*pool.Pool2[ProxyThriftClientCore]),
	}, nil
}

func (p *ProxyThriftClientPool) Destroy() {
	pool.DeletePool[ProxyThriftClientCore](pool.GetPoolManager(), p.Pool.Pool2Config.Host)
}

// todo: http thrift client
// type ProxyHttpThriftClient struct {

// }
