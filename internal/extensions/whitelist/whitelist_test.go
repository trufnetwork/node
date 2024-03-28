package whitelist

import (
	"encoding/hex"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sort"
	"testing"
)

func TestWhitelistExt_check(t *testing.T) {
	tests := []struct {
		name       string
		whitelists []string
		inputs     []interface{}
		want       []interface{}
		wantErr    bool
		errMsg     string
	}{
		{
			name: "HappyPath",
			whitelists: []string{
				"wallet1",
				"wallet2",
				"wallet3",
			},
			inputs: []interface{}{
				"wallet2",
			},
			want: []interface{}{
				true,
			},
			wantErr: false,
		},
		{
			name: "MultipleInputs",
			whitelists: []string{
				"wallet1",
				"wallet2",
				"wallet3",
			},
			inputs: []interface{}{
				"wallet2",
				"wallet3",
			},
			wantErr: true,
			errMsg:  "expected 1 input, got 2",
		},
		{
			name: "NonStringInput",
			whitelists: []string{
				"wallet1",
				"wallet2",
				"wallet3",
			},
			inputs: []interface{}{
				123,
			},
			wantErr: true,
			errMsg:  "expected string, got int",
		},
		{
			name: "WalletNotWhitelisted",
			whitelists: []string{
				"wallet1",
				"wallet2",
				"wallet3",
			},
			inputs: []interface{}{
				"wallet5",
			},
			wantErr: true,
			errMsg:  "wallet wallet5 is not whitelisted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &WhitelistExt{
				whitelistedWallets: tt.whitelists,
			}

			got, err := w.check(tt.inputs)
			if (err != nil) != tt.wantErr {
				t.Errorf("WhitelistExt.check() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("WhitelistExt.check() error message = %v, wantErrMsg %v", err.Error(), tt.errMsg)
			}

			if !tt.wantErr && err == nil {
				if len(got) != len(tt.want) {
					t.Errorf("WhitelistExt.check() = %v, want %v", got, tt.want)
				}

				for i, v := range got {
					if v != tt.want[i] {
						t.Errorf("WhitelistExt.check() = %v, want %v", got, tt.want)
					}
				}
			}
		})
	}
}

func TestInitializeExtension(t *testing.T) {
	ownerAddress := "0x0000000000000000000000000000000000000001"
	validAddress := "0x0000000000000000000000000000000000000011"
	validAddress2 := "0x0000000000000000000000000000000000000111"

	byteOwner, err := hex.DecodeString(ownerAddress[2:])
	if err != nil {
		t.Fatalf("Error decoding owner address %v", err)
	}

	invalidAddress := "notgood"
	invalidAddress2 := "000000000000000000000000000000000000000001"
	var ctx = &precompiles.DeploymentContext{Schema: &common.Schema{Owner: byteOwner}}

	tests := []struct {
		name            string
		metadata        map[string]string
		expectError     bool
		expectedWallets []string
	}{
		{
			"Empty metadata",
			make(map[string]string),
			false,
			[]string{ownerAddress},
		},
		{
			"Too much arguments",
			map[string]string{"whitelist_wallets": "wallet1,wallet2", "whitelist_wallet2": "wallet3,wallet4"},
			true,
			nil,
		},
		{
			name:            "Wallet format not correct",
			metadata:        map[string]string{"whitelist_wallets": "wallet1,wallet2," + invalidAddress},
			expectError:     true,
			expectedWallets: nil,
		},
		{
			"Valid metadata and wallet",
			map[string]string{"whitelist_wallets": validAddress},
			false,
			[]string{ownerAddress, validAddress},
		},
		{
			"Valid metadata and multiple wallets",
			map[string]string{"whitelist_wallets": validAddress + "," + validAddress2},
			false,
			[]string{ownerAddress, validAddress, validAddress2},
		},
		{
			"Address not start with 0x",
			map[string]string{"whitelist_wallets": invalidAddress2},
			true,
			nil,
		},
		{
			"wrong metadata",
			map[string]string{"wrong": "wrong"},
			true,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext, err := InitializeExtension(ctx, nil, tt.metadata)
			if (err != nil) != tt.expectError {
				t.Fatalf("InitializeExtension() error = %v, expectError %v", err, tt.expectError)
			}

			if err == nil {
				actualWallets := ext.(*WhitelistExt).whitelistedWallets

				// order is not important
				sort.Strings(actualWallets)
				sort.Strings(tt.expectedWallets)

				if !reflect.DeepEqual(actualWallets, tt.expectedWallets) {
					t.Errorf("Expected wallets %v, but got %v", tt.expectedWallets, actualWallets)
				}
			}
		})
	}
}

func TestWhitelistExt_Call(t *testing.T) {
	t.Run("success - it should return nil when method is check", func(t *testing.T) {
		instance := &WhitelistExt{
			whitelistedWallets: []string{"wallet1", "wallet2"},
		}
		_, err := instance.Call(nil, nil, "check", []interface{}{"wallet1"})
		assert.NoError(t, err, "WhitelistExt.Call returned an error")
	})

	t.Run("validation - it should return error when method is unknown", func(t *testing.T) {
		instance := &WhitelistExt{}
		_, err := instance.Call(nil, nil, "unknown", nil)
		assert.Contains(t, err.Error(), "unknown method")
	})
}
