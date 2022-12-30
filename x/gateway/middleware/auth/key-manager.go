package auth

import (
	"encoding/json"
	"io"
)

type keyJson struct {
	Keys []string `json:"keys"`
}

type KeyManager struct {
	keys map[string]struct{}
}

func NewKeyManager(r io.Reader) (*KeyManager, error) {
	keys, err := loadKeys(r)
	if err != nil {
		return nil, err
	}
	return &KeyManager{keys: keys}, nil
}

func (k *KeyManager) IsAllowed(t *token) bool {
	_, ok := k.keys[t.ApiKey]
	return ok
}

func loadKeys(h io.Reader) (map[string]struct{}, error) {
	bts, err := io.ReadAll(h)
	if err != nil {
		return nil, err
	}

	var keys keyJson
	err = json.Unmarshal(bts, &keys)
	if err != nil {
		return nil, err
	}

	km := make(map[string]struct{})
	for _, k := range keys.Keys {
		km[k] = struct{}{}
	}

	return km, nil
}
