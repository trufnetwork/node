package tasks

import (
	"context"
	"kwil/kwil/repository"
	"kwil/x/chain"
)

type heightTask struct {
	chainCode chain.ChainCode
	dao       repository.Queries
	queries   repository.Queries
}

func NewHeightTask(dao repository.Queries, chainCode chain.ChainCode) Runnable {
	return &heightTask{
		chainCode: chainCode,
		dao:       nil,
		queries:   dao,
	}
}

func (t *heightTask) Run(ctx context.Context, chunk *Chunk) error {
	t.dao = t.queries.WithTx(chunk.Tx)

	err := t.dao.SetHeight(ctx, int32(t.chainCode), chunk.Finish)
	if err != nil {
		return err
	}

	t.dao = nil

	return nil
}