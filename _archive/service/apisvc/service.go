package apisvc

import (
	"encoding/json"
	gm "kwil/x/graphql/manager"
	"kwil/x/logx"
	pricing "kwil/x/pricing/service"
	"kwil/x/proto/apipb"
	"kwil/x/sqlx/manager"
	"kwil/x/sqlx/models"
)

type Service struct {
	apipb.UnimplementedKwilServiceServer

	log     logx.Logger
	p       pricing.PricingService
	manager *manager.Manager
	// hasura manager
	hm gm.Client
}

func NewService(mngr *manager.Manager, hm gm.Client) *Service {
	return &Service{
		log:     logx.New(),
		p:       pricing.NewService(),
		manager: mngr,
		hm:      hm,
	}
}

type RequestBody interface {
	models.QueryTx | models.DropDatabase | models.CreateDatabase
}

func Marshal[B RequestBody](v B) ([]byte, error) {
	return json.Marshal(v)
}

func Unmarshal[B RequestBody](data []byte) (*B, error) {
	out := new(B)

	// trimming off the version and message type
	if err := json.Unmarshal(data[2:], out); err != nil {
		return nil, err
	}
	return out, nil
}