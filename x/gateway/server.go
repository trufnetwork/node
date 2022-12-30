package gateway

import (
	"context"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"kwil/x/gateway/middleware"
	"kwil/x/graphql"
	"kwil/x/logx"
	"kwil/x/proto/apipb"
	"net/http"
)

type GWServer struct {
	mux         *runtime.ServeMux
	addr        string
	middlewares []*middleware.NamedMiddleware
	logger      logx.SugaredLogger
	h           http.Handler
}

func NewGWServer(m *runtime.ServeMux, addr string) *GWServer {
	return &GWServer{mux: m,
		addr:   addr,
		logger: logx.New().Sugar()}
}

func (g *GWServer) AddMiddleWares(ms []*middleware.NamedMiddleware) {
	g.h = g.mux
	for _, m := range ms {
		g.middlewares = append(g.middlewares, m)
		g.logger.Infof("apply %s middleware", m.Name)
		g.h = m.Mw(g.h)
	}
}

func (g *GWServer) ApplyMiddleWares(ms []*middleware.NamedMiddleware) {
	for _, m := range ms {
		g.middlewares = append(g.middlewares, m)
	}
}

func (g *GWServer) Serve() error {
	g.logger.Info("Starting gateway service at: ", g.addr)
	return http.ListenAndServe(g.addr, g)
}

func (g *GWServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.h.ServeHTTP(w, r)
}

func (g *GWServer) SetupGrpcSvc(ctx context.Context) error {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	return apipb.RegisterKwilServiceHandlerFromEndpoint(ctx, g.mux, viper.GetString(GrpcEndpointName), opts)
}

func (g *GWServer) SetupHttpSvc(ctx context.Context) error {
	err := g.mux.HandlePath(http.MethodGet, "/api/v0/swagger.json", func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
		apipb.ServeSwaggerJSON(w, r)
	})
	if err != nil {
		return err
	}

	err = g.mux.HandlePath(http.MethodGet, "/swagger/ui", func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
		apipb.ServeSwaggerUI(w, r)
	})

	if err != nil {
		return err
	}

	graphqlRProxy := graphql.NewRProxy()
	err = g.mux.HandlePath(http.MethodPost, "/graphql", graphqlRProxy.Handler)
	if err != nil {
		return err
	}

	return err
}
