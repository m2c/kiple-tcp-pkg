// Package ktcp provides udp,tcp,kcp three kinds of protocol.
package kptcp

import (
	"errors"
	"fmt"
	"github.com/m2c/kiple-tcp-pkg/slog"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	DEFAULT_HEARTBEAT_MESSAGEID = 1392
	DEFAULT_AUTH_MESSAGEID      = 1393

	STATE_RUNNING = 1
	STATE_STOP    = 2

	PIPED = "[ktcp-buffer-in-serial]"
)

// OnMessage and mux are opposite.
// When OnMessage is not nil, users should deal will ctx.Stream themselves.
// When OnMessage is nil, program will handle ctx.Stream via mux routing by messageID
type KpTcp struct {
	OnConnect func(ctx *Context)
	OnMessage func(ctx *Context)
	OnClose   func(ctx *Context)
	Mux       *Mux
	Packx     *Packx

	// deadline setting
	deadLine      time.Time
	writeDeadLine time.Time
	readDeadLine  time.Time

	// heartbeat setting
	HeartBeatOn        bool          // whether start a goroutine to spy on each connection
	HeatBeatInterval   time.Duration // heartbeat should receive in the interval
	HeartBeatMessageID int32         // which messageID to listen to heartbeat
	ThroughMiddleware  bool          // whether heartbeat go through middleware

	// built-in clientPool
	// clientPool is defined in github.com/ktcp/client-pool.go, you might design your own pool yourself as
	// long as you set builtInPool = false
	// - How to add/delete an connection into/from pool?
	// ```
	//    // add
	//    ctx.Online(username)
	//    // delete
	//    ctx.Offline(username)
	// ``
	builtInPool bool
	pool        *ClientPool

	// when auth is set true by `srv.WithAuth(true)`, server will start a goroutine to wait for auth handler signal.
	// If not receive ideal signal in auth-deadline, the established connection will be close forcely by server.
	auth                  bool
	authDeadline          time.Duration
	AuthMessageID         int32
	AuthThroughMiddleware bool // whether auth handler go through middleware

	// external for graceful
	properties []*PropertyCache
	pLock      *sync.RWMutex
	state      int // 1- running, 2- stopped

	// external for broadcast
	withSignals    bool
	closeAllSignal chan int // used to close all connection's spying goroutines, srv instance controls it

	// external for handle any stream
	// only support tcp/kcp
	HandleRaw func(c *Context)
}

type PropertyCache struct {
	Network string
	Port    string

	// only when network is 'tcp','kcp', Listener can assert to net.Listener.
	// when network is 'udp', it can assert to net.PackConn
	Listener interface{}
}

// new an kptcp srv instance
func NewKptcp(marshaller Marshaller) *KpTcp {
	return &KpTcp{
		Packx:      NewPackx(marshaller),
		Mux:        NewMux(),
		properties: make([]*PropertyCache, 0, 10),
		pLock:      &sync.RWMutex{},
		state:      2,
	}
}

// whether using built-in pool
func (ktcp *KpTcp) WithBuiltInPool(yes bool) *KpTcp {
	ktcp.builtInPool = yes
	ktcp.pool = NewClientPool()
	return ktcp
}

func (ktcp *KpTcp) WithAuthDetail(yes bool, duration time.Duration, throughMiddleware bool, messageID int32, f func(c *Context)) *KpTcp {
	ktcp.auth = yes
	ktcp.authDeadline = duration

	ktcp.AuthMessageID = messageID
	ktcp.AuthThroughMiddleware = throughMiddleware

	if yes {
		ktcp.AddHandler(messageID, f)
	}
	return ktcp
}

// Set deadline
// This should be set before server start.
// If you want change deadline while it's running, use ctx.SetDeadline(t time.Time) instead.
func (ktcp *KpTcp) SetDeadline(t time.Time) {
	ktcp.deadLine = t
}

// Set read deadline
// This should be set before server start.
// If you want change deadline while it's running, use ctx.SetDeadline(t time.Time) instead.
func (ktcp *KpTcp) SetReadDeadline(t time.Time) {
	ktcp.readDeadLine = t
}

// Set write deadline
// This should be set before server start.
// If you want change deadline while it's running, use ctx.SetDeadline(t time.Time) instead.
func (ktcp *KpTcp) SetWriteDeadline(t time.Time) {
	ktcp.writeDeadLine = t
}

// Whether using signal-broadcast.
// Used for these situations:
// closeAllSignal - close all connection and remove them from the built-in pool
func (ktcp *KpTcp) WithBroadCastSignal(yes bool) *KpTcp {
	ktcp.withSignals = yes
	ktcp.closeAllSignal = make(chan int, 1)
	return ktcp
}

// Set built in heart beat on
// Default heartbeat handler will be added by messageID ktcp.DEFAULT_HEARTBEAT_MESSAGEID(-1392),
// and default heartbeat handler will not execute all kinds of middleware.
//
// ...
// srv := ktcp.Newktcp(nil)
// srv.HeartBeatMode(true, 10 * time.Second)
// ...
//
// * If you want specific official heartbeat handler detail:
// srv.HeartBeatModeDetail(true, 10 * time.Second, true, 1)
//
// * If you want to rewrite heartbeat handler:
// srv.RewriteHeartBeatHandler(func(c *ktcp.Context){})
//
// * If you think built in heartbeat not good, abandon it:
// ```
// srv.AddHandler(1111, func(c *ktcp.Context){
//    //do nothing by default and define your heartbeat yourself
// })
// ```
func (ktcp *KpTcp) HeartBeatMode(on bool, duration time.Duration) *KpTcp {
	ktcp.HeartBeatOn = on
	ktcp.HeatBeatInterval = duration
	ktcp.ThroughMiddleware = false
	ktcp.HeartBeatMessageID = DEFAULT_HEARTBEAT_MESSAGEID

	if on {
		ktcp.AddHandler(DEFAULT_HEARTBEAT_MESSAGEID, func(c *Context) {
			slog.Infof("recv '%s' heartbeat:", c.ClientIP())
			c.RecvHeartBeat()
		})
	}
	return ktcp
}

// specific args for heartbeat
func (ktcp *KpTcp) HeartBeatModeDetail(on bool, duration time.Duration, throughMiddleware bool, messageID int32) *KpTcp {
	ktcp.HeartBeatOn = on
	ktcp.HeatBeatInterval = duration
	ktcp.ThroughMiddleware = throughMiddleware
	ktcp.HeartBeatMessageID = messageID

	if on {
		ktcp.AddHandler(messageID, func(c *Context) {
			slog.Infof("recv '%s' heartbeat:", c.ClientIP())
			c.RecvHeartBeat()
		})
	}
	return ktcp
}

// Rewrite heartbeat handler
// It will inherit properties of the older heartbeat handler:
//   * heartbeatInterval
//   * throughMiddleware
func (ktcp *KpTcp) RewriteHeartBeatHandler(messageID int32, f func(c *Context)) *KpTcp {
	ktcp.removeHandler(ktcp.HeartBeatMessageID)
	ktcp.HeartBeatMessageID = messageID
	ktcp.AddHandler(messageID, f)
	return ktcp
}

// remove a handler by messageID.
// this method is used for rewrite heartbeat handler
func (ktcp *KpTcp) removeHandler(messageID int32) {
	delete(ktcp.Mux.Handlers, messageID)
	delete(ktcp.Mux.MessageIDAnchorMap, messageID)
}

// Middleware typed 'AnchorTypedMiddleware'.
// Add middlewares ruled by (string , func(c *Context),string , func(c *Context),string , func(c *Context)...).
// Middlewares will be added with an indexed key, which is used to unUse this middleware.
// Each middleware added will be well set an anchor index, when UnUse this middleware, its expire_anchor_index will be well set too.
func (ktcp *KpTcp) Use(mids ...interface{}) {
	if ktcp.Mux == nil {
		ktcp.Mux = NewMux()
	}

	if len(mids)%2 != 0 {
		panic(errors.New(fmt.Sprintf("ktcp.Use(mids ...),'mids' should show in pairs,but got length(mids) %d", len(mids))))
	}
	var middlewareKey string
	var ok bool
	var middleware func(c *Context)

	//var middlewareAnchor MiddlewareAnchor
	for i := 0; i < len(mids)-1; i += 2 {
		j := i + 1
		middlewareKey, ok = mids[i].(string)
		if !ok {
			panic(errors.New(fmt.Sprintf("ktcp.Use(mids ...), 'mids' index '%d' should be string key type but got %v", i, mids[i])))
		}
		middleware, ok = mids[j].(func(c *Context))
		if !ok {
			panic(errors.New(fmt.Sprintf("ktcp.Use(mids ...), 'mids' index '%d' should be func(c *ktcp.Context) type but got %s", j, reflect.TypeOf(mids[j]).Kind().String())))
		}

		middlewareAnchor, ok := ktcp.Mux.MiddlewareAnchorMap[middlewareKey]
		if ok {
			middlewareAnchor.callUse(ktcp.Mux.CurrentAnchorIndex())
			ktcp.Mux.MiddlewareAnchorMap[middlewareKey] = middlewareAnchor
		} else {
			var middlewareAnchor MiddlewareAnchor
			middlewareAnchor.Middleware = middleware
			middlewareAnchor.MiddlewareKey = middlewareKey
			middlewareAnchor.callUse(ktcp.Mux.CurrentAnchorIndex())
			ktcp.Mux.AddMiddlewareAnchor(middlewareAnchor)
		}
	}
}

// UnUse an middleware.
// a unused middleware will expired among handlers added after it.For example:
//
// 	srv := ktcp.Newktcp(ktcp.JsonMarshaller{})
//  srv.Use("middleware1", Middleware1, "middleware2", Middleware2)
//	srv.AddHandler(1, SayHello)
//	srv.UnUse("middleware2")
//	srv.AddHandler(3, SayGoodBye)
//
// middleware1 and middleware2 will both work to handler 'SayHello'.
// middleware1 will work to handler 'SayGoodBye' but middleware2 will not work to handler 'SayGoodBye'
func (ktcp *KpTcp) UnUse(middlewareKeys ...string) {
	var middlewareAnchor MiddlewareAnchor
	var ok bool
	for _, k := range middlewareKeys {
		if middlewareAnchor, ok = ktcp.Mux.MiddlewareAnchorMap[k]; !ok {
			panic(errors.New(fmt.Sprintf("middlewareKey '%s' not found in mux.MiddlewareAnchorMap", k)))
		}
		middlewareAnchor.callUnUse(ktcp.Mux.CurrentAnchorIndex())
		ktcp.Mux.ReplaceMiddlewareAnchor(middlewareAnchor)
	}
}

// Middleware typed 'GlobalTypedMiddleware'.
// GlobalMiddleware will work to all handlers.
func (ktcp *KpTcp) UseGlobal(mids ...func(c *Context)) {
	if ktcp.Mux == nil {
		ktcp.Mux = NewMux()
	}
	ktcp.Mux.AddGlobalMiddleware(mids...)
}

// Middleware typed 'SelfRelatedTypedMiddleware'.
// Add handlers routing by messageID
func (ktcp *KpTcp) AddHandler(messageID int32, handlers ...func(ctx *Context)) {
	if len(handlers) <= 0 {
		panic(errors.New(fmt.Sprintf("handlers should more than 1 but got %d", len(handlers))))
	}
	if len(handlers) > 1 {
		ktcp.Mux.AddMessageIDSelfMiddleware(messageID, handlers[:len(handlers)-1]...)
	}

	f := handlers[len(handlers)-1]
	if ktcp.Mux == nil {
		ktcp.Mux = NewMux()
	}
	ktcp.Mux.AddHandleFunc(messageID, f)
	var messageIDAnchor MessageIDAnchor
	messageIDAnchor.MessageID = messageID
	messageIDAnchor.AnchorIndex = ktcp.Mux.CurrentAnchorIndex()
	ktcp.Mux.AddMessageIDAnchor(messageIDAnchor)
}

// Start to listen.
// Serve can decode stream generated by packx.
// Support tcp and udp
func (ktcp *KpTcp) ListenAndServe(network, addr string) error {
	ktcp.checkPrepare()

	if In(network, []string{"tcp", "tcp4", "tcp6", "unix", "unixpacket"}) {
		return ktcp.ListenAndServeTCP(network, addr)
	}
	if In(network, []string{"udp", "udp4", "udp6", "unixgram", "ip%"}) {
		return ktcp.ListenAndServeUDP(network, addr)
	}
	if In(network, []string{"kcp"}) {
		return fmt.Errorf("ktcp only supports kcp in v3.0.0-")
		// return ktcp.ListenAdServeKCP(network, addr)
	}
	//if In(network, []string{"http", "https"}) {
	//	return ktcp.ListenAndServeHTTP(network, addr)
	//}
	return errors.New(fmt.Sprintf("'network' doesn't support '%s'", network))
}

// Check necessary preparations,if not good yet ,will panic and print error
// This means You should handle this panic before your app is running
func (ktcp *KpTcp) checkPrepare() {

	// Check anchor middleware valid or not
	for i, _ := range ktcp.Mux.MiddlewareAnchors {
		ktcp.Mux.MiddlewareAnchors[i].checkValidBeforeRun()
	}
}

func (ktcp *KpTcp) fillProperty(network, addr string, listener interface{}) {

	ktcp.pLock.Lock()
	defer ktcp.pLock.Unlock()

	if ktcp.properties == nil {
		ktcp.properties = make([]*PropertyCache, 0, 10)
	}

	// if property exists, only replace listener
	for i, v := range ktcp.properties {
		if v.Network == network && v.Port == addr {
			ktcp.properties[i].Listener = listener
			return
		}
	}
	prop := &PropertyCache{
		Network:  network,
		Port:     addr,
		Listener: listener,
	}
	ktcp.properties = append(ktcp.properties, prop)
}

// raw
func (ktcp *KpTcp) ListenAndServeRaw(network, addr string) error {
	defer func() {
		if e := recover(); e != nil {
			slog.Info(fmt.Sprintf("recover from panic %v", e))
			slog.Info(string(debug.Stack()))
			return
		}
	}()
	listener, err := net.Listen(network, addr)
	if err != nil {
		return err
	}
	ktcp.fillProperty(network, addr, listener)

	defer listener.Close()
	ktcp.openState()
	for {

		if ktcp.State() == STATE_STOP {
			break
		}
		conn, err := listener.Accept()
		if err != nil {
			log.Println(fmt.Sprintf(err.Error()))
			break
		}

		// SetDeadline
		conn.SetDeadline(ktcp.deadLine)
		conn.SetReadDeadline(ktcp.readDeadLine)
		conn.SetWriteDeadline(ktcp.writeDeadLine)

		ctx := NewContext(conn, ktcp.Packx.Marshaller)

		if ktcp.builtInPool {
			ctx.poolRef = ktcp.pool
		}

		if ktcp.OnConnect != nil {
			ktcp.OnConnect(ctx)
		}

		go broadcastSignalWatch(ctx, ktcp)
		go heartBeatWatch(ctx, ktcp)

		go func(ctx *Context, ktcp *KpTcp) {
			defer func() {
				if e := recover(); e != nil {
					slog.Info(fmt.Sprintf("recover from panic %v", e))
					// Logger.Println(string(debug.Stack()))
				}
			}()
			//defer ctx.Conn.Close()
			defer ctx.CloseConn()
			if ktcp.OnClose != nil {
				defer ktcp.OnClose(ctx)
			}

			ctx.InitReaderAndWriter()

			tmpContext := copyContext(*ctx)
			handleRaw(tmpContext, ktcp)
		}(ctx, ktcp)
	}
	return nil
}

// tcp
func (ktcp *KpTcp) ListenAndServeTCP(network, addr string) error {
	defer func() {
		if e := recover(); e != nil {
			slog.Info((fmt.Sprintf("recover from panic %v", e)))
			slog.Info(string(debug.Stack()))
			return
		}
	}()
	listener, err := net.Listen(network, addr)
	if err != nil {
		return err
	}
	ktcp.fillProperty(network, addr, listener)

	defer listener.Close()
	ktcp.openState()
	for {

		if ktcp.State() == STATE_STOP {
			break
		}
		conn, err := listener.Accept()
		if err != nil {
			log.Println(fmt.Sprintf(err.Error()))
			break
		}

		// SetDeadline
		conn.SetDeadline(ktcp.deadLine)
		conn.SetReadDeadline(ktcp.readDeadLine)
		conn.SetWriteDeadline(ktcp.writeDeadLine)

		ctx := NewContext(conn, ktcp.Packx.Marshaller)

		if ktcp.builtInPool {
			ctx.poolRef = ktcp.pool
		}

		if ktcp.OnConnect != nil {
			ktcp.OnConnect(ctx)
		}

		if ktcp.withSignals {
			go broadcastSignalWatch(ctx, ktcp)
		}

		if ktcp.HeartBeatOn {
			go heartBeatWatch(ctx, ktcp)
		}
		if ktcp.auth {
			go authWatch(ctx, ktcp)
		}

		go func(ctx *Context, ktcp *KpTcp) {
			defer func() {
				if e := recover(); e != nil {
					slog.Info(fmt.Sprintf("recover from panic %v", e))
					// Logger.Println(string(debug.Stack()))
				}
			}()
			//defer ctx.Conn.Close()
			defer ctx.CloseConn()
			if ktcp.OnClose != nil {
				defer ktcp.OnClose(ctx)
			}
			var e error
			for {
				ctx.Stream, e = ctx.Packx.FirstBlockOf(ctx.Conn)
				if e != nil {
					if e == io.EOF {
						break
					}
					slog.Error(e)
					break
				}
				tmpContext := copyContext(*ctx)

				isPipe, restN, e := isPipe(tmpContext.Stream)
				if e != nil {
					slog.Error(e)
					break
				}
				// fmt.Println("pipe args", isPipe, restN)

				if isPipe {
					// fmt.Println("recv pipe", isPipe, restN)
					ctxs := make([]*Context, restN+1)
					ctxs[0] = tmpContext
					for i := 1; i < restN+1; i++ {
						ctx.Stream, e = ctx.Packx.FirstBlockOf(ctx.Conn)
						if e != nil {
							if e == io.EOF {
								break
							}
							slog.Error(e)
							break
						}
						tmpContext := copyContext(*ctx)
						ctxs[i] = tmpContext
					}

					go handlePipe(ctxs, ktcp)
				} else {
					go handleMiddleware(tmpContext, ktcp)
				}
				continue
			}
		}(ctx, ktcp)
	}
	return nil
}

// set srv state running
func (ktcp *KpTcp) openState() {
	ktcp.pLock.Lock()
	defer ktcp.pLock.Unlock()
	ktcp.state = 1
}

// set srv state stopped
// tcp, udp, kcp will stop for circle and close listener/conn
func (ktcp *KpTcp) stopState() {
	ktcp.pLock.Lock()
	defer ktcp.pLock.Unlock()
	ktcp.state = STATE_STOP
}
func (ktcp *KpTcp) State() int {
	ktcp.pLock.RLock()
	defer ktcp.pLock.RUnlock()
	return ktcp.state
}

// udp
// maxBufferSize can set buffer length, if receive a message longer than it ,
func (ktcp *KpTcp) ListenAndServeUDP(network, addr string, maxBufferSize ...int) error {
	if len(maxBufferSize) > 1 {
		panic(errors.New(fmt.Sprintf("'ktcp.ListenAndServeUDP''s maxBufferSize should has length less by 1 but got %d", len(maxBufferSize))))
	}

	conn, err := net.ListenPacket(network, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ktcp.fillProperty(network, addr, conn)

	ktcp.openState()

	conn.SetDeadline(ktcp.deadLine)
	conn.SetReadDeadline(ktcp.readDeadLine)
	conn.SetWriteDeadline(ktcp.writeDeadLine)

	// listen to incoming udp packets
	go func(conn net.PacketConn, ktcp *KpTcp) {
		defer func() {
			if e := recover(); e != nil {
				slog.Error(fmt.Sprintf("recover from panic %v", e))
			}
		}()
		var buffer []byte
		var addr net.Addr
		var e error
		for {
			if ktcp.State() == STATE_STOP {
				break
			}
			// read from udp conn
			buffer, addr, e = ReadAllUDP(conn, maxBufferSize...)

			// global
			if e != nil {
				if e == io.EOF {
					break
				}
				slog.Error(e.Error())
				continue
				//conn.Close()
				//conn, err = net.ListenPacket(network, addr)
				//if err != nil {
				//	panic(err)
				//}
			}
			ctx := NewUDPContext(conn, addr, ktcp.Packx.Marshaller)

			go broadcastSignalWatch(ctx, ktcp)

			go heartBeatWatch(ctx, ktcp)
			if ktcp.builtInPool {
				ctx.poolRef = ktcp.pool
			}

			ctx.Stream, e = ktcp.Packx.FirstBlockOfBytes(buffer)
			if e != nil {
				slog.Error(e.Error())
				continue
			}
			// This function are shared among udp ListenAndServe,tcp ListenAndServe and kcp ListenAndServe.
			// But there are some important differences.
			// tcp's context is per-connection scope, some middleware offset and temporary handlers are saved in
			// this context,which means, this function can't work in parallel goroutines.But udp's context is
			// per-request scope, middleware's args are request-apart, it can work in parallel goroutines because
			// different request has different context instance.It's concurrently safe.
			// Thus we can use it like : `go func(ctx *Context, ktcp *ktcp){...}(ctx, ktcp)`
			go handleMiddleware(ctx, ktcp)

			continue
		}
	}(conn, ktcp)

	select {}

	//return nil
}

func ReadAllUDP(conn net.PacketConn, maxBufferSize ...int) ([]byte, net.Addr, error) {
	if len(maxBufferSize) > 1 {
		panic(errors.New(fmt.Sprintf("'ktcp.ListenAndServeUDP calls ReadAllUDP''s maxBufferSize should has length less by 1 but got %d", len(maxBufferSize))))
	}
	var buffer []byte
	if len(maxBufferSize) <= 0 {
		buffer = make([]byte, 4096, 4096)
	} else {
		buffer = make([]byte, maxBufferSize[0], maxBufferSize[0])
	}

	n, addr, e := conn.ReadFrom(buffer)
	fmt.Println(n)

	if e != nil {
		return nil, nil, e
	}
	return buffer[0:n], addr, nil
}

// http
// developing, do not use.
//
// Deprecated: on developing.
func (ktcp *KpTcp) ListenAndServeHTTP(network, addr string) error {
	//r := gin.New()
	//	//r.Any("/ktcp/message/:messageID/", func(ginCtx *gin.Context) {
	//	//
	//	//})
	//	//s := &http.Server{
	//	//	Addr:           addr,
	//	//	Handler:        cors.AllowAll().Handler(r),
	//	//	ReadTimeout:    60 * time.Second,
	//	//	WriteTimeout:   60 * time.Second,
	//	//	MaxHeaderBytes: 1 << 21,
	//	//}
	//	//return s.ListenAndServe()
	return nil
}

// grpc
// developing, do not use.
// marshaller must be protobuf, clients should send message bytes which body is protobuf bytes
//
// Deprecated: on developing.
func (ktcp *KpTcp) ListenAndServeGRPC(network, addr string) error {
	return nil
}

// This method is abstracted from ListenAndServe[,TCP,UDP] for handling middlewares.
// can perfectly work concurrently
//
// However, this method is not open export for outer uset. When rebuild new protocol server, this will be considerately used.
func handleMiddleware(ctx *Context, ktcp *KpTcp) {
	if ktcp.OnMessage != nil {
		// ktcp.Mux.execAllMiddlewares(ctx)
		//ktcp.OnMessage(ctx)
		if ctx.handlers == nil {
			ctx.handlers = make([]func(c *Context), 0, 10)
		}
		ctx.handlers = append(ctx.handlers, ktcp.Mux.GlobalMiddlewares...)
		for _, v := range ktcp.Mux.MiddlewareAnchors {
			ctx.handlers = append(ctx.handlers, v.Middleware)
		}
		ctx.handlers = append(ctx.handlers, ktcp.OnMessage)
		if len(ctx.handlers) > 0 {
			ctx.Next()
		}
		ctx.Reset()
	} else {
		messageID, e := ktcp.Packx.MessageIDOf(ctx.Stream)
		if e != nil {
			slog.Error(e.Error())
			return
		}

		handler, ok := ktcp.Mux.Handlers[messageID]
		if !ok {
			slog.Info(fmt.Sprintf("messageID %d handler not found", messageID))
			return
		}
		if messageID == ktcp.HeartBeatMessageID && !ktcp.ThroughMiddleware {
			handler(ctx)
			return
		}

		if messageID == ktcp.AuthMessageID && !ktcp.AuthThroughMiddleware {
			handler(ctx)
			return
		}

		if ctx.handlers == nil {
			ctx.handlers = make([]func(c *Context), 0, 10)
		}

		// global middleware
		ctx.handlers = append(ctx.handlers, ktcp.Mux.GlobalMiddlewares...)
		// anchor middleware
		messageIDAnchorIndex := ktcp.Mux.AnchorIndexOfMessageID(messageID)
		// ######## BUG REPORT ########
		// old: anchor type middleware may be added unordered.
		// ############################
		//for _, v := range ktcp.Mux.MiddlewareAnchorMap {
		//	if messageIDAnchorIndex > v.AnchorIndex && messageIDAnchorIndex <= v.ExpireAnchorIndex {
		//		ctx.handlers = append(ctx.handlers, v.Middleware)
		//	}
		//}
		// new:
		for i, _ := range ktcp.Mux.MiddlewareAnchors {
			v := ktcp.Mux.MiddlewareAnchors[i]
			if v.Contains(messageIDAnchorIndex) {
				ctx.handlers = append(ctx.handlers, v.Middleware)
			}
		}

		// self-related middleware
		ctx.handlers = append(ctx.handlers, ktcp.Mux.MessageIDSelfMiddleware[messageID]...)
		// handler
		ctx.handlers = append(ctx.handlers, handler)

		if len(ctx.handlers) > 0 {
			ctx.Next()
		}
		ctx.Reset()
	}
}

func handlePipe(ctxs []*Context, ktcp *KpTcp) {
	// fmt.Println("handle pipe ", len(ctxs))
	for i, _ := range ctxs {
		handleMiddleware(ctxs[i], ktcp)
	}
}

func handleRaw(ctx *Context, ktcp *KpTcp) {
	if ctx.handlers == nil {
		ctx.handlers = make([]func(c *Context), 0, 10)
	}
	ctx.handlers = append(ctx.handlers, ktcp.Mux.GlobalMiddlewares...)
	for _, v := range ktcp.Mux.MiddlewareAnchors {
		ctx.handlers = append(ctx.handlers, v.Middleware)
	}
	ctx.handlers = append(ctx.handlers, ktcp.HandleRaw)
	if len(ctx.handlers) > 0 {
		ctx.Next()
	}
	ctx.Reset()
}

// Start a goroutine to watch heartbeat for a connection
// When a connection is built and heartbeat mode is true, the
// then, client should do it in 5 second and continuous sends heartbeat each heart beat interval.
// ATTENTION:
// If server side set heartbeat 10s,
// client should consider the message transport price, when client send heartbeat 10s,server side might receive beyond 10s.
// Once heartbeat fail more than 3 times, it will close the connection.
// In these cases heartbeat watching goroutine will stop:
// - ktcp.closeAllSignal: when ktcp srv calls `srv.Stop()`, closeAllSignal will be closed and stop this watching goroutine.
// - ctx.recvEnd: when connection's context calls 'ctx.CloseConn()', recvEnd will be closed and stop this watching goroutine.
// - time out receiving interval heartbeat pack.
func heartBeatWatch(ctx *Context, ktcp *KpTcp) {
	if ktcp.HeartBeatOn == true {
		go func() {
			defer func() {
				// fmt.Println("心跳结束)
				if e := recover(); e != nil {
					slog.Error("recover from : %v", e)
				}
			}()

			var times int
		L:
			for {
				if ktcp.State() == STATE_STOP {
					ctx.CloseConn()
					break L
				}
				select {
				case <-ctx.HeartBeatChan():
					times = 0
					continue L
				case <-time.After(ktcp.HeatBeatInterval):
					times++
					if times == 3 {
						_ = ctx.CloseConn()
						return
					}
					continue L
				case <-ktcp.closeAllSignal:
					ctx.CloseConn()
					return
				case <-ctx.recvEnd:
					return
				}
			}
		}()
	}
}

// Each connection will have this goroutine, it bind relation with ktcp server.
// ktcp.closeAllSignal: when ktcp srv calls `srv.Stop()`, closeAllSignal will be closed and stop this watching goroutine.
// ctx.recvEnd: when connection's context calls 'ctx.CloseConn()', recvEnd will be closed and stop this watching goroutine.
func broadcastSignalWatch(ctx *Context, ktcp *KpTcp) {
	if ktcp.withSignals == true {
		for {
			select {
			case <-ktcp.closeAllSignal:
				ctx.CloseConn()
				return
			case <-ctx.recvEnd:
				return
			}
		}
	}
}

// Each connection will start this as goroutine when ktcp.auth is true.
// It will start a handler to wait for an auth signal.
func authWatch(ctx *Context, ktcp *KpTcp) {
	if ktcp.auth {
		select {
		case <-time.After(ktcp.authDeadline):
			slog.Error("connection auth time out, closed")
			ctx.CloseConn()
			return
		case v := <-ctx.AuthChan():
			if v >= 0 {
				// auth pass
				return
			} else {
				// auth fail
				slog.Error("connection auth fail, closed")
				ctx.CloseConn()
				return
			}
		case <-ktcp.closeAllSignal:
			ctx.CloseConn()
			return
		case <-ctx.recvEnd:
			return
		}

	}
}

// Before exist do ending jobs
func (ktcp *KpTcp) BeforeExit(f ...func()) {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				fmt.Println(fmt.Sprintf("panic from %v", e))
			}
		}()
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT)
		fmt.Println("receive signal:", <-ch)
		fmt.Println("prepare to stop server")
		for _, handler := range f {
			handler()
		}
		os.Exit(0)
	}()
}

// Graceful stop server parts generated by `srv.ListenAndServe()`, this will not stop process, if param 'closeAllConnection' is false, only stop server listener.
// Older connections will remain safe and kept in pool.If param 'closeAllConnection' is true, it will not only stop the
// listener, but also kill all connections(stops their net.Conn, stop all sub-routine, clear the pool)
func (ktcp *KpTcp) Stop(closeAllConnection bool) error {
	fmt.Println("graceful stop")
	if ktcp.State() == STATE_STOP {
		return errors.New("already stopped")
	}

	ktcp.stopState()

	// close all listener
	func() {
		ktcp.pLock.Lock()
		defer ktcp.pLock.Unlock()
		for i, v := range ktcp.properties {
			switch v.Network {
			case "kcp", "tcp":
				ktcp.properties[i].Listener.(net.Listener).Close()
			case "udp":
				ktcp.properties[i].Listener.(net.PacketConn).Close()
			}
		}
	}()

	// close all connections
	if closeAllConnection == true {
		ktcp.closeAllConnection()
	}
	return nil
}

func (ktcp *KpTcp) closeAllConnection() {
	if ktcp.withSignals == true {
		close(ktcp.closeAllSignal)
	} else {
		if ktcp.pool != nil {
			oldPool := ktcp.pool
			go func() {
				oldPool.m.Lock()
				defer oldPool.m.Unlock()

				for k, _ := range oldPool.Clients {
					oldPool.Clients[k].CloseConn()
					delete(oldPool.Clients, k)
				}
			}()

			ktcp.pool = NewClientPool()
		}
	}
}

// Graceful start an existed ktcp srv, former server is stopped by ktcp.Stop()
func (ktcp *KpTcp) Start() error {
	if ktcp.State() == STATE_RUNNING {
		return errors.New("already running")
	}

	for _, v := range ktcp.properties {
		go func() {
			defer func() {
				if e := recover(); e != nil {
					slog.Error(fmt.Sprintf("panic from '%v' \n %s", e, debug.Stack()))
				}
			}()
			fmt.Println(fmt.Sprintf("graceful restart %s server on %s", v.Network, v.Port))
			e := ktcp.ListenAndServe(v.Network, v.Port)
			if e != nil {
				slog.Error(fmt.Sprintf("%s \n %s", e.Error(), debug.Stack()))
			}
		}()
	}
	return nil
}

// Graceful Restart = Stop and Start.Besides, you can
func (ktcp *KpTcp) Restart(closeAllConnection bool, beforeStart ...func()) error {
	if e := ktcp.Stop(closeAllConnection); e != nil {
		return e
	}

	for _, v := range beforeStart {
		v()
	}
	// time.Sleep(5 * time.Second)
	if e := ktcp.Start(); e != nil {
		return e
	}
	return nil
}

// return isPipe, rest serial block number, error
func isPipe(block []byte) (bool, int, error) {
	header, e := HeaderOf(block)
	if e != nil {
		return false, 0, e
	}
	// fmt.Println("header: ", header)

	if len(header) == 0 {
		return false, 0, nil
	}

	pipeArgsI, ok := header[PIPED]
	if !ok {
		return false, 0, nil
	}

	pipeArgs, ok := pipeArgsI.(string)
	if !ok {
		return false, 0, errors.New(fmt.Sprintf("bad pipe args, should format as a  string, bug got type '%s'", reflect.TypeOf(pipeArgsI).Name()))
	}

	arr := strings.Split(pipeArgs, ";")
	if len(arr) != 2 {
		return false, 0, errors.New(fmt.Sprintf("bad pipe args, should format as 'enable:<length>' but got %s", pipeArgs))
	}

	length, err := strconv.Atoi(arr[1])
	if err != nil {
		return false, 0, errors.New(fmt.Sprintf("bad pipe args length, should format as 'enable:<length>' but got %s,\n %v", arr[1], err))
	}
	if arr[0] == "enable" {
		return true, length - 1, nil
	}
	return false, 0, nil
}
