//go:build kwiltest

package erc20

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	evmsync "github.com/trufnetwork/kwil-db/node/exts/evm-sync"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

// InjectERC20Transfer forces an instance synced and injects a synthetic Deposit log that credits balance.
func InjectERC20Transfer(ctx context.Context, platform *kwilTesting.Platform, chain, escrow, erc20Addr, fromHex, toHex string, valueStr string, point int64, prev *int64) error {
	// 1) Ensure instance exists and is synced
	id, err := erc20bridge.ForTestingForceSyncInstance(ctx, platform, chain, escrow, erc20Addr, 18)
	if err != nil {
		log.Printf("force sync instance: %v", err) // instance already exists
	}

	// 2) Compute ordered-sync topic
	topic := erc20bridge.ForTestingTransferListenerTopic(*id)

	// 3) Build a synthetic deposit log
	if !ethcommon.IsHexAddress(fromHex) {
		return fmt.Errorf("invalid address: %s", fromHex)
	}
	if !ethcommon.IsHexAddress(toHex) {
		return fmt.Errorf("invalid address: %s", toHex)
	}
	to := ethcommon.HexToAddress(toHex)
	escrowAddress := ethcommon.HexToAddress(escrow)
	var bn big.Int
	if _, ok := bn.SetString(valueStr, 10); !ok {
		return fmt.Errorf("invalid value: %s", valueStr)
	}
	// topics: only signature (no indexed params)
	topics := []ethcommon.Hash{
		ethcommon.HexToHash("0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"),
	}

	var recipientWord [32]byte
	copy(recipientWord[32-len(to.Bytes()):], to.Bytes())
	val32 := types.BigIntToHash32(&bn)
	data := append(recipientWord[:], val32[:]...)
	lg := &ethtypes.Log{
		Address:     escrowAddress,
		Topics:      topics,
		Data:        data,
		BlockNumber: uint64(point),
		TxHash:      ethcommon.Hash{},
		TxIndex:     0,
		BlockHash:   ethcommon.Hash{},
		Index:       0,
		Removed:     false,
	}
	ethLog := &evmsync.EthLog{Metadata: []byte("rcpdepst"), Log: lg}

	// 4) Serialize like production and store via ordered-sync
	logsData, err := serializeEthLogsLocal([]*evmsync.EthLog{ethLog})
	if err != nil {
		return fmt.Errorf("serialize logs: %w", err)
	}

	if err := orderedsync.ForTestingStoreLogs(ctx, platform, topic, logsData, point, prev); err != nil {
		return fmt.Errorf("store logs: %w", err)
	}

	// 5) Resolve via end-block path
	if err := orderedsync.ForTestingResolve(ctx, platform, &common.BlockContext{Height: point, Timestamp: point}); err != nil {
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
