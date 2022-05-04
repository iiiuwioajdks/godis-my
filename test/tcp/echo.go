package tcp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"qydis/lib/logger"
	"qydis/lib/sync/atomic"
	"qydis/lib/sync/wait"
	"sync"
	"syscall"
	"time"
)

type Handle interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

func Serve(listener net.Listener, handler Handle, closeChan <-chan struct{}) {
	go func() {
		<-closeChan
		logger.Info("shutting down")
		listener.Close()
		handler.Close()
	}()

	defer func() {
		listener.Close()
		handler.Close()
	}()

	ctx := context.Background()
	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Debug(err)
			break
		}
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	wg.Wait()
}

func ListenAndServeWithSignal(addr string,handler Handle) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Debug(err)
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", addr))
	Serve(listener, handler, closeChan)
	return nil
}

// 连接的客户端
type Client struct {
	Conn net.Conn
	wait wait.Wait
}

type EchoHandler struct {
	// 存储正在连接的客户端
	activeConn sync.Map
	// 是否正在关闭
	closing atomic.Boolean
}

func MakeEchoHandler()(*EchoHandler) {
	return &EchoHandler{}
}

func (h *EchoHandler)Handle(ctx context.Context, conn net.Conn) {
	// 关闭中的 handler 不会处理新连接
	if h.closing.Get() {
		conn.Close()
		return
	}

	client := &Client {
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{}) // 记住仍然存活的连接

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		// 发送数据前先置为waiting状态，阻止连接被关闭
		client.wait.Add(1)

		// 模拟关闭时未完成发送的情况
		//logger.Info("sleeping")
		//time.Sleep(10 * time.Second)

		b := []byte(msg)
		conn.Write(b)
		// 发送完毕, 结束waiting
		client.wait.Done()
	}
}

// 关闭客户端连接
func (c *Client)Close()error {
	// 等待数据发送完成或超时
	c.wait.WaitWithTimeOut(10 * time.Second)
	c.Conn.Close()
	return nil
}

// 关闭服务器
func (h *EchoHandler)Close()error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// 逐个关闭连接
	h.activeConn.Range(func(key interface{}, val interface{})bool {
		client := key.(*Client)
		client.Close()
		return true
	})
	return nil
}