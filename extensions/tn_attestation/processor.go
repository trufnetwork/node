package tn_attestation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/trufnetwork/kwil-db/common"
)

type attestationRecord struct {
	hash          []byte
	requester     []byte
	canonical     []byte
	createdHeight int64
}

// PreparedSignature captures the data needed to call sign_attestation once
// broadcasting is wired: the attestation hash, the generated signature, and
// metadata for logging and auditing.
type PreparedSignature struct {
	HashHex       string
	Hash          []byte
	Requester     []byte
	Signature     []byte
	Payload       *CanonicalPayload
	CreatedHeight int64
}

func (e *signerExtension) fetchUnsignedAttestations(ctx context.Context, hash []byte) ([]attestationRecord, error) {
	engine := e.Engine()
	db := e.DB()
	if engine == nil || db == nil {
		return nil, fmt.Errorf("attestation extension not initialised with engine/db")
	}

	// Returns multiple rows per hash: composite key is (hash, requester, created_height).
	// Different requesters can request identical attestations.
	records := []attestationRecord{}
	err := engine.ExecuteWithoutEngineCtx(
		ctx,
		db,
		`SELECT attestation_hash, requester, result_canonical, created_height
		 FROM attestations
		 WHERE attestation_hash = $hash AND signature IS NULL
		 ORDER BY created_height ASC`,
		map[string]any{"hash": hash},
		func(row *common.Row) error {
			if len(row.Values) < 4 {
				return fmt.Errorf("unexpected attestation row format: got %d columns", len(row.Values))
			}

			rec := attestationRecord{
				hash:          bytesClone(row.Values[0].([]byte)),
				requester:     bytesCloneOrNil(row.Values[1]),
				canonical:     bytesClone(row.Values[2].([]byte)),
				createdHeight: row.Values[3].(int64),
			}
			records = append(records, rec)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (e *signerExtension) prepareSigningWork(ctx context.Context, hashHex string) ([]*PreparedSignature, error) {
	hashHex = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(hashHex)), "0x")
	if hashHex == "" {
		return nil, fmt.Errorf("attestation hash cannot be empty")
	}

	hashBytes, err := hex.DecodeString(hashHex)
	if err != nil {
		return nil, fmt.Errorf("invalid attestation hash %q: %w", hashHex, err)
	}

	records, err := e.fetchUnsignedAttestations(ctx, hashBytes)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	signer := GetValidatorSigner()
	if signer == nil {
		return nil, fmt.Errorf("validator signer not initialised")
	}

	prepared := make([]*PreparedSignature, 0, len(records))
	for _, rec := range records {
		payload, err := ParseCanonicalPayload(rec.canonical)
		if err != nil {
			return nil, fmt.Errorf("parse canonical payload: %w", err)
		}

		// Hash verification prevents signing corrupted/tampered payloads. SQL generates
		// both canonical blob and hash independently; recomputing ensures they match.
		// Without this, a corrupted result_canonical could produce wrong signatures.
		expectedHash := computeAttestationHash(payload)
		if !bytes.Equal(expectedHash[:], rec.hash) {
			return nil, fmt.Errorf("attestation hash mismatch: expected %x, got %x", rec.hash, expectedHash)
		}

		digest := payload.SigningDigest()
		signature, err := signer.SignDigest(digest[:])
		if err != nil {
			return nil, fmt.Errorf("sign digest: %w", err)
		}

		prepared = append(prepared, &PreparedSignature{
			HashHex:       hashHex,
			Hash:          bytesClone(rec.hash),
			Requester:     bytesCloneOrNil(rec.requester),
			Signature:     signature,
			Payload:       payload,
			CreatedHeight: rec.createdHeight,
		})
	}

	return prepared, nil
}

func (e *signerExtension) fetchPendingHashes(ctx context.Context, limit int) ([]string, error) {
	engine := e.Engine()
	db := e.DB()
	if engine == nil || db == nil {
		return nil, fmt.Errorf("attestation extension not initialised with engine/db")
	}
	if limit <= 0 {
		limit = int(e.ScanBatchLimit())
	}

	hashes := make([]string, 0, limit)
	err := engine.ExecuteWithoutEngineCtx(
		ctx,
		db,
		`SELECT encode(attestation_hash, 'hex') AS hash
		 FROM attestations
		 WHERE signature IS NULL
		 GROUP BY attestation_hash
		 ORDER BY MIN(created_height) ASC
		 LIMIT $limit`,
		map[string]any{"limit": limit},
		func(row *common.Row) error {
			if len(row.Values) == 0 {
				return nil
			}
			hash, ok := row.Values[0].(string)
			if !ok {
				return fmt.Errorf("unexpected hash column type %T", row.Values[0])
			}
			hash = strings.TrimSpace(hash)
			if hash != "" {
				hashes = append(hashes, hash)
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return hashes, nil
}

func computeAttestationHash(p *CanonicalPayload) [sha256.Size]byte {
	var buf bytes.Buffer
	buf.WriteByte(p.Version)
	buf.WriteByte(p.Algorithm)
	buf.Write(p.DataProvider)
	buf.Write(p.StreamID)

	actionBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(actionBytes, p.ActionID)
	buf.Write(actionBytes)
	buf.Write(p.Args)

	return sha256.Sum256(buf.Bytes())
}

func bytesClone(b []byte) []byte {
	return bytes.Clone(b)
}

func bytesCloneOrNil(v any) []byte {
	if v == nil {
		return nil
	}
	return bytes.Clone(v.([]byte))
}
