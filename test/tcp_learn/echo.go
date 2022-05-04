package main

import (
	"bufio"
	"io"
	"net"
	"qydis/lib/logger"
)

/*
just test the tcp_learn how to use
 */

func ListenAndServer(addr string) {
	listen, err := net.Listen("tcp_learn", addr)
	if err != nil {
		logger.Fatal(err)
	}
	defer listen.Close()
	logger.Infof("bind: %s, start listening...",addr)

	for {
		conn, err := listen.Accept()
		if err != nil {
			logger.Fatal("accept err :", err)
		}
		go Handle(conn)
	}
}

func Handle(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for  {
		msg, err := reader.ReadString('\n')
		if err != nil {
			// 连接中断
			if err == io.EOF {
				logger.Info("connect close")
			}else {
				logger.Debug(err)
			}
			return
		}
		b := []byte(msg)
		// 简单的将收到的东西发送给客户端
		conn.Write(b)
	}
}

func main() {
	// telnet 127.0.0.1 8000
	logger.SetLevel(logger.LogLevelInfo)
	ListenAndServer(":8000")
}