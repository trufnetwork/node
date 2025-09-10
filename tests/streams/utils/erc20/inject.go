package erc20

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	evmsync "github.com/trufnetwork/kwil-db/node/exts/evm-sync"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
)

// InjectERC20Transfer forces an instance synced and injects a synthetic Transfer log that credits balance.
func InjectERC20Transfer(ctx context.Context, app *common.App, chain, escrow, erc20Addr, fromHex, toHex string, valueStr string, point int64, prev *int64) error {
	// 1) Ensure instance exists and is synced
	id, err := erc20bridge.ForTestingForceSyncInstance(ctx, app, chain, escrow, erc20Addr, 18)
	if err != nil {
		return fmt.Errorf("force sync instance: %w", err)
	}

	// 2) Compute ordered-sync topic
	topic := erc20bridge.ForTestingTransferListenerTopic(*id)

	// 3) Build a synthetic transfer log
	from := ethcommon.HexToAddress(fromHex)
	to := ethcommon.HexToAddress(toHex)
	erc20Address := ethcommon.HexToAddress(erc20Addr)
	var bn big.Int
	if _, ok := bn.SetString(valueStr, 10); !ok {
		return fmt.Errorf("invalid value: %s", valueStr)
	}
	// topics: signature + from + to
	topics := []ethcommon.Hash{
		ethcommon.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
		ethcommon.BytesToHash(from.Bytes()),
		ethcommon.BytesToHash(to.Bytes()),
	}
	// data: 32-byte big-endian value
	val32 := types.BigIntToHash32(&bn)
	lg := &ethtypes.Log{
		Address:     erc20Address,
		Topics:      topics,
		Data:        val32[:],
		BlockNumber: uint64(point),
		TxHash:      ethcommon.Hash{},
		TxIndex:     0,
		BlockHash:   ethcommon.Hash{},
		Index:       0,
		Removed:     false,
	}
	ethLog := &evmsync.EthLog{Metadata: []byte("e20trsnfr"), Log: lg}

	// 4) Serialize like production and store via ordered-sync
	logsData, err := serializeEthLogsLocal([]*evmsync.EthLog{ethLog})
	if err != nil {
		return fmt.Errorf("serialize logs: %w", err)
	}

	if err := orderedsync.ForTestingStoreLogs(ctx, app, topic, logsData, point, prev); err != nil {
		return fmt.Errorf("store logs: %w", err)
	}

	// 5) Resolve via end-block path
	if err := orderedsync.ForTestingResolve(ctx, app, &common.BlockContext{Height: point, Timestamp: point}); err != nil {
		return fmt.Errorf("resolve: %w", err)
	}

	return nil
}

// serializeEthLogsLocal mirrors evmsync.serializeEthLogs for test tag builds
func serializeEthLogsLocal(logs []*evmsync.EthLog) ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, l := range logs {
		b, err := l.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, uint64(len(b))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(b); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
