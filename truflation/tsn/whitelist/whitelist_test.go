package whitelist

import (
	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"github.com/kwilteam/kwil-db/internal/engine/types"
	"reflect"
	"testing"
)

func TestWhitelistExt_check(t *testing.T) {
	tests := []struct {
		name       string
		whitelists [][]byte
		inputs     []interface{}
		want       []interface{}
		wantErr    bool
		errMsg     string
	}{
		{
			name: "HappyPath",
			whitelists: [][]byte{
				[]byte("wallet1"),
				[]byte("wallet2"),
				[]byte("wallet3"),
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
			whitelists: [][]byte{
				[]byte("wallet1"),
				[]byte("wallet2"),
				[]byte("wallet3"),
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
			whitelists: [][]byte{
				[]byte("wallet1"),
				[]byte("wallet2"),
				[]byte("wallet3"),
			},
			inputs: []interface{}{
				123,
			},
			wantErr: true,
			errMsg:  "expected string, got int",
		},
		{
			name: "WalletNotWhitelisted",
			whitelists: [][]byte{
				[]byte("wallet1"),
				[]byte("wallet2"),
				[]byte("wallet3"),
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
	ownerAddress := "0x00000000000000000000000000000000000owner"
	validAddress := "0x00000000000000000000000000000000000valid"
	validAddress2 := "0x0000000000000000000000000000000000valid2"

	invalidAddress := "notgood"
	var ctx = &execution.DeploymentContext{Schema: &types.Schema{Owner: []byte(ownerAddress)}}

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext, err := InitializeExtension(ctx, tt.metadata)
			if (err != nil) != tt.expectError {
				t.Fatalf("InitializeExtension() error = %v, expectError %v", err, tt.expectError)
			}

			if err == nil {
				actualWalletsByte := ext.(*WhitelistExt).whitelistedWallets
				actualWallets := make([]string, len(actualWalletsByte))
				for i, wallet := range actualWalletsByte {
					actualWallets[i] = string(wallet)
				}
				if !reflect.DeepEqual(actualWallets, tt.expectedWallets) {
					t.Errorf("Expected wallets %v, but got %v", tt.expectedWallets, actualWallets)
				}
			}
		})
	}
}
