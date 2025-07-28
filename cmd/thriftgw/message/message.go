package message

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/Qthai16/go-playground/utils"

	"github.com/apache/thrift/lib/go/thrift"
)

type TMessage interface {
	Read(ctx context.Context, proto thrift.TProtocol) error
	Write(ctx context.Context, proto thrift.TProtocol) error
	IsOneway() bool
	Reset()
}

type ThriftMessageHeader struct {
	Addr  string
	Name  string
	Type  thrift.TMessageType
	SeqId int32
}

func NewThriftMessageHeader() *ThriftMessageHeader {
	return &ThriftMessageHeader{
		Addr:  "",
		Name:  "",
		Type:  thrift.INVALID_TMESSAGE_TYPE,
		SeqId: -1,
	}
}

func (p *ThriftMessageHeader) Read(ctx context.Context, proto thrift.TProtocol) error {
	name, typeId, seqId, err := proto.ReadMessageBegin(ctx)
	if err != nil {
		return err
	}
	tokens := strings.SplitN(name, ":", 3)
	if len(tokens) == 3 {
		p.Addr = tokens[0] + ":" + tokens[1]
		p.Name = tokens[2]
	} else {
		p.Name = name
	}
	p.Type = typeId
	p.SeqId = seqId
	return nil
}

func (p *ThriftMessageHeader) Reset() {
	p.Addr = ""
	p.Name = ""
	p.Type = thrift.INVALID_TMESSAGE_TYPE
	p.SeqId = -1
}

func (p *ThriftMessageHeader) String() string {
	return fmt.Sprintf("Addr: %v, Name: %v, Type: %v, SeqId: %v", p.Addr, p.Name, p.Type, p.SeqId)
}

type ThriftMessage struct {
	Header *ThriftMessageHeader
	Body   bytes.Buffer
}

var _ TMessage = (*ThriftMessage)(nil)

func NewThriftMessage() *ThriftMessage {
	return &ThriftMessage{
		Header: NewThriftMessageHeader(),
		Body:   bytes.Buffer{},
	}
}

// read implement TMessage Read interface
func (m *ThriftMessage) Read(ctx context.Context, proto thrift.TProtocol) error {
	err := m.Header.Read(ctx, proto)
	if err != nil {
		//utils.LogErro("read header error: %v\n", err)
		return err
	}
	remainBytes := proto.Transport().RemainingBytes()
	// utils.LogErro("name: %v, typeId: %v, seqId: %v\n", m.Header.Name, m.Header.Type, m.Header.SeqId)
	if remainBytes > 0 {
		_, err := io.CopyN(&m.Body, proto.Transport(), int64(remainBytes))
		// _, err := m.Body.ReadFrom(proto.Transport())
		// utils.LogInfo("[DEBUG] read %v bytes\n", n)
		if err != nil && err != io.EOF {
			utils.LogErro("read error: %v\n", err)
			return err
		}
	}
	return nil
}

// write implement TMessage Write interface
func (m *ThriftMessage) Write(ctx context.Context, proto thrift.TProtocol) (err error) {
	// todo: check if header is valid
	if err = proto.WriteMessageBegin(ctx, m.Header.Name, m.Header.Type, m.Header.SeqId); err != nil {
		return err
	}
	if _, err = m.Body.WriteTo(proto.Transport()); err != nil {
		return err
	}
	return proto.Flush(ctx)
}

func (m *ThriftMessage) IsOneway() bool {
	return m.Header.Type == thrift.ONEWAY
}

func (m *ThriftMessage) Reset() {
	m.Header.Reset()
	m.Body.Reset()
}

type ThriftErrorMessage struct {
	Header *ThriftMessageHeader
	Err    error
}

var _ TMessage = (*ThriftMessage)(nil)

func NewThriftErrorMessage(name string, seqId int32, err error) *ThriftErrorMessage {
	return &ThriftErrorMessage{
		Header: &ThriftMessageHeader{
			Addr:  "",
			Name:  name,
			Type:  thrift.EXCEPTION,
			SeqId: seqId,
		},
		Err: err,
	}
}

func (m *ThriftErrorMessage) Read(ctx context.Context, proto thrift.TProtocol) error {
	return nil
}

func (m *ThriftErrorMessage) Write(ctx context.Context, proto thrift.TProtocol) (err error) {
	if err = proto.WriteMessageBegin(ctx, m.Header.Name, m.Header.Type, m.Header.SeqId); err != nil {
		return err
	}
	wrappedErr := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, m.Err.Error())
	if err = wrappedErr.Write(ctx, proto); err != nil {
		return err
	}
	proto.WriteMessageEnd(ctx)
	return proto.Flush(ctx)
}

func (m *ThriftErrorMessage) IsOneway() bool {
	return m.Header.Type == thrift.ONEWAY
}

func (m *ThriftErrorMessage) Reset() {
	m.Header.Reset()
	m.Err = nil
}
