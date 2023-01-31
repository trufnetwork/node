package server

import (
	"context"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"kwil/x/logx"
	"net"
)

type Server struct {
	server *grpc.Server
}

func New(logger *zap.Logger, opts ...Option) *Server {
	l := logger.WithOptions(zap.WithCaller(false))

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_zap.StreamServerInterceptor(l),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_zap.UnaryServerInterceptor(l),
			BodyLoggerInterceptor(l),
		)),
	)

	s := &Server{
		server: server,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *Server) RegisterService(sd *grpc.ServiceDesc, ss interface{}) {
	s.server.RegisterService(sd, ss)
}

func (s *Server) Serve(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		s.server.GracefulStop()
	}()

	return s.server.Serve(lis)
}

func (s *Server) Stop() {
	s.server.Stop()
}

// BodyLoggerInterceptor returns a new unary server interceptor that logs the
// request body.
func BodyLoggerInterceptor(logger logx.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		logger.Debug("request body", zap.Any("body", req))
		return handler(ctx, req)
	}
}
