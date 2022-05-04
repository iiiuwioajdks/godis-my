package tcp

import (
	"context"
	"net"
)

type HandleFunc func(ctx context.Context, conn net.Conn)

type Handle interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
