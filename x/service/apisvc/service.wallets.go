package apisvc

import (
	"context"
	pricing "kwil/x/pricing/entity"
	"kwil/x/proto/apipb"
)

func (s *Service) Connect(ctx context.Context, req *apipb.ConnectRequest) (*apipb.ConnectResponse, error) {
	return &apipb.ConnectResponse{Address: "0xAfFDC06cF34aFD7D5801A13d48C92AD39609901D"}, nil
}

// right now this uses the pricing entity but not the service
func (s *Service) EstimateCost(ctx context.Context, req *pricing.EstimateRequest) (*pricing.EstimateResponse, error) {
	p, err := s.p.EstimatePrice(req)
	if err != nil {
		return nil, err
	}
	return p, nil
}

/*
func (s *Service) GetWithdrawalsForWallet(ctx context.Context, req *apipb.GetWithdrawalsRequest) (*apipb.GetWithdrawalsResponse, error) {
	wdr, err := s.ds.GetWithdrawalsForWallet(ctx, req.Wallet)
	if err != nil {
		return nil, err
	}

	// Marshal and unmarshal
	bts, err := json.Marshal(wdr)
	if err != nil {
		return nil, err
	}

	var m apipb.GetWithdrawalsResponse
	err = json.Unmarshal(bts, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *Service) GetBalance(ctx context.Context, req *apipb.GetBalanceRequest) (*apipb.GetBalanceResponse, error) {

	bal, sp, err := s.ds.GetBalanceAndSpent(ctx, req.Wallet)
	if err != nil {
		return nil, err
	}

	return &apipb.GetBalanceResponse{
		Balance: bal,
		Spent:   sp,
	}, nil
}

func (s *Service) ReturnFunds(ctx context.Context, req *apipb.ReturnFundsRequest) (*apipb.ReturnFundsResponse, error) {

	// THIS SHOULD NOT YET BE USED IN PRODUCTION WITH REAL FUNDS

	// reconstruct id
	// id for return funds is generated from amount, nonce, and address (from)

	id := createFundsReturnID(req.Amount, req.Nonce, req.From)

	if id != req.Id {
		return nil, ErrInvalidID
	}

	// check to make sure the the ID is signed
	valid, err := crypto.CheckSignature(req.From, req.Signature, []byte(req.Id))
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, ErrInvalidSignature
	}
	wdr, err := s.ds.Withdraw(ctx, req.From, req.Amount)
	if err != nil {
		return nil, err
	}

	return &apipb.ReturnFundsResponse{
		Tx:            wdr.Tx,
		Amount:        wdr.Amount,
		Fee:           wdr.Fee,
		CorrelationId: wdr.Cid,
		Expiration:    wdr.Expiration,
	}, nil
}

func (s *Service) EstimateCost(ctx context.Context, req *apipb.EstimateCostRequest) (*apipb.EstimateCostResponse, error) {
	p, err := s.p.GetPrice(ctx)
	if err != nil {
		return nil, err
	}
	return &apipb.EstimateCostResponse{
		Fee: p.String(),
	}, nil
}
*/
