package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Qthai16/go-playground/cmd/thriftgw/gwstat"
	"github.com/Qthai16/go-playground/cmd/thriftgw/message"
	"github.com/Qthai16/go-playground/cmd/thriftgw/proxyclient"
	"github.com/Qthai16/go-playground/common/pool"
	"github.com/Qthai16/go-playground/utils"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/sevlyar/go-daemon"
)

var (
	connConf *thrift.TConfiguration = &thrift.TConfiguration{ // pool client connection config
		ConnectTimeout:     5 * time.Second,
		SocketTimeout:      5 * time.Second,
		MaxFrameSize:       1024 * 1024 * 256,
		TBinaryStrictRead:  thrift.BoolPtr(true),
		TBinaryStrictWrite: thrift.BoolPtr(true),
	}
	transFactory thrift.TTransportFactory = thrift.NewTFramedTransportFactoryConf(thrift.NewTBufferedTransportFactory(8192), nil)
	protoFactory thrift.TProtocolFactory  = thrift.NewTBinaryProtocolFactoryConf(nil)
	stats                                 = gwstat.NewGWStats()
	cmdLineOpts                           = CmdlineOpts{}
)

type CmdlineOpts struct {
	Addr    string
	LogPath string
	Daemon  bool
}

type ClientPoolMap struct {
	AllPools map[string]*proxyclient.ProxyThriftClientPool
	mu       sync.RWMutex
}

func NewClientPoolMap() *ClientPoolMap {
	return &ClientPoolMap{
		AllPools: make(map[string]*proxyclient.ProxyThriftClientPool),
		mu:       sync.RWMutex{},
	}
}

func (cpm *ClientPoolMap) GetPool(addr string) *proxyclient.ProxyThriftClientPool {
	cpm.mu.RLock()
	defer cpm.mu.RUnlock()
	if p, ok := cpm.AllPools[addr]; ok {
		return p
	}
	return nil
}

func (cpm *ClientPoolMap) PutPool(addr string, pool *proxyclient.ProxyThriftClientPool) {
	cpm.mu.Lock()
	defer cpm.mu.Unlock()
	cpm.AllPools[addr] = pool
}

func (cpm *ClientPoolMap) Destroy() {
	cpm.mu.Lock()
	defer cpm.mu.Unlock()
	for _, p := range cpm.AllPools {
		p.Destroy()
	}
}

func doHandleConn(ctx context.Context, inMessage *message.ThriftMessage, clientPoolMap *ClientPoolMap, hostIproto, hostOproto thrift.TProtocol) (err error) {
	inMessage.Reset()
	if err = inMessage.Read(ctx, hostIproto); err != nil {
		// fmt.Println("[debug]: client close connection")
		return
	}
	stats.AddMethodStat(inMessage.Header.Addr, inMessage.Header.Name)
	var clientPool *proxyclient.ProxyThriftClientPool
	if clientPool = clientPoolMap.GetPool(inMessage.Header.Addr); clientPool == nil {
		clientPool, err = proxyclient.NewProxyThriftClientPool(inMessage.Header.Addr, connConf)
		if err != nil {
			stats.IncPoolCreateErr()
			return err
		}
		clientPoolMap.PutPool(inMessage.Header.Addr, clientPool)
	}
	client, err := clientPool.Pool.Get()
	if err != nil {
		stats.IncPoolGetErr()
		if errors.Is(err, pool.ErrNoConnection) {
			// fmt.Printf("failed to dial to dest addr, err: %v\n", err)
			transErr := thrift.NewTTransportException(thrift.NOT_OPEN, proxyclient.PrependProxyError(err).Error())
			appErr := message.NewThriftErrorMessage(inMessage.Header.Name, inMessage.Header.SeqId, transErr)
			return appErr.Write(ctx, hostOproto)
		}
		// todo: do we need to send pool get connection error to host???
		return
	}
	defer clientPool.Pool.Put(client)
	return client.(*proxyclient.ProxyThriftClient).Call(ctx, inMessage, hostOproto)
}

func handleConn(ctx context.Context, client thrift.TTransport) {
	if client.(*thrift.TSocket) != nil {
		conn := client.(*thrift.TSocket).Conn()
		rmtAddrStr := conn.RemoteAddr().String()
		fmt.Printf("got conn from remote: %v\n", rmtAddrStr)
		// stats.AddRemote(rmtAddrStr)
		// defer stats.DelRemote(rmtAddrStr)
	}
	// todo: configure different factory for in and out
	var inTransport, outTransport thrift.TTransport
	var err error
	if inTransport, err = transFactory.GetTransport(client); err != nil {
		return
	}
	iproto := protoFactory.GetProtocol(inTransport)
	if outTransport, err = transFactory.GetTransport(client); err != nil {
		return
	}
	oproto := protoFactory.GetProtocol(outTransport)
	inMessage := message.NewThriftMessage()
	if inTransport != nil {
		defer inTransport.Close()
	}
	if outTransport != nil {
		defer outTransport.Close()
	}
	clientpoolmap := NewClientPoolMap()
	defer clientpoolmap.Destroy()
	for {
		if err := doHandleConn(ctx, inMessage, clientpoolmap, iproto, oproto); err != nil {
			break
		}
	}
}

func flagInit() {
	flag.StringVar(&cmdLineOpts.Addr, "addr", ":18000", "server listen addr")
	flag.StringVar(&cmdLineOpts.LogPath, "log", "", "log file path")
	flag.BoolVar(&cmdLineOpts.Daemon, "daemon", false, "run as daemon")
}

func startThriftServer(ctx context.Context, addr string) {
	prefix := "thrift-server"
	srvSocket, err := thrift.NewTServerSocketTimeout(addr, 0)
	if err != nil {
		utils.LogErro("%v: failed to create socket %v, err: %v", prefix, addr, err)
		return
	}
	err = srvSocket.Listen()
	if err != nil {
		utils.LogErro("%v: failed to listen, err: %v", prefix, err)
		return
	}
	utils.LogInfo("%v: listening on %v", prefix, addr)
	for {
		trans, err := srvSocket.Accept()
		// fmt.Println("got new conn")
		if err != nil {
			utils.LogErro("%v: accept conn failed, err: %v", prefix, err)
			return
		}
		if trans != nil {
			go handleConn(ctx, trans)
		}
	}
}

func printStats(ctx context.Context, f *os.File) {
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			if jsonstr, err := json.Marshal(stats); err == nil {
				f.Write(jsonstr)
			} else {
				f.WriteString(stats.String())
			}
			f.WriteString("\n")
		}
	}
}

func uniqPidFile() string {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	f := fmt.Sprintf("/tmp/thriftgw.%d.pid", r.Intn(1000))
	for st, err := os.Stat(f); err == nil && !st.IsDir(); {
		// fmt.Printf("file exist: %v, retrying\n", f)
		f = fmt.Sprintf("/tmp/thriftgw.%d.pid", r.Intn(1000))
		st, err = os.Stat(f)
	}
	return f
}

func run() {
	if len(cmdLineOpts.LogPath) > 0 {
		logPath := cmdLineOpts.LogPath
		f, _ := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		log.SetOutput(f)
		utils.RedirectFile(os.Stderr, f)
		defer f.Close()
		// NOTES: stdout and stderr redirect to /dev/null is done by go-daemon if flag daemon is set
		// devNull, _ := os.OpenFile("/dev/null", os.O_WRONLY, 0755)
		// defer devNull.Close()
		// utils.RedirectFile(os.Stdin, devNull)
		// utils.RedirectFile(os.Stdout, devNull)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		startThriftServer(ctx, cmdLineOpts.Addr)
		cancel()
	}()
	go func() {
		printStats(ctx, os.Stdout)
	}()
	select {
	case <-ctx.Done():
	case <-utils.WaitTerminate():
		break
	}
	utils.LogInfo("server exit")
}

func main() {
	flagInit()
	flag.Parse()
	if len(cmdLineOpts.Addr) == 0 {
		utils.LogErro("invalid address")
		return
	}
	if cmdLineOpts.Daemon {
		utils.LogInfo("running process as daemon")
		cntxt := &daemon.Context{
			PidFileName: uniqPidFile(),
			PidFilePerm: 0644,
		}
		d, err := cntxt.Reborn()
		if err != nil {
			utils.LogErro("failed to run as daemon: %v", err)
			return
		}
		if d != nil { // parent process
			return
		}
		defer cntxt.Release()
	}
	run()
}
