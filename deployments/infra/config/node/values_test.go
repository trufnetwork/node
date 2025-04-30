package node

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"text/template"

	"github.com/BurntSushi/toml" // TOML parsing library
	"github.com/Masterminds/sprig/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/node/infra/lib/utils"
	// For RepoRoot
)

func TestRenderKwildConfigTemplate(t *testing.T) {
	// --- 1. Define Sample Values ---
	sampleValues := Values{
		Log: LogConfig{
			Level:      "debug",
			Format:     "json",
			Output:     []string{"stdout"},
			FileRollKB: 5000,
			RetainMax:  5,
		},
		Genesis: GenesisConfig{
			Path: "/test/genesis.json",
		},
		P2P: P2PConfig{
			ListenPort:        6601,
			PEX:               false,
			Bootnodes:         []string{"abc@1.2.3.4:6600", "def@5.6.7.8:6600"},
			TargetConnections: 50,
			External:          "9.9.9.9:6601",
		},
		DB: DBConfig{
			Host:        "test-db",
			Port:        5433,
			User:        "testuser",
			Pass:        "testpass",
			Name:        "testdb",
			ReadTimeout: "60s",
			MaxConns:    100,
		},
	}

	// --- 2. Render Template ---
	rootDir := utils.GetProjectRootDir() // Use test helper to find repo root reliably
	// kwild-config.tmpl lives under deployments/infra/config/node
	templatePath := filepath.Join(rootDir, "deployments/infra/config/node", "kwild-config.tmpl")

	tmpl, err := template.New(filepath.Base(templatePath)).
		Funcs(sprig.TxtFuncMap()).
		ParseFiles(templatePath)
	require.NoError(t, err, "Failed to parse template")

	var renderedConfig bytes.Buffer
	err = tmpl.Execute(&renderedConfig, sampleValues)
	require.NoError(t, err, "Failed to execute template")

	renderedToml := renderedConfig.String()
	t.Logf("Rendered TOML:\n%s", renderedToml) // Log output for debugging

	// --- 3. Optional: Quick string assertions for top-level/edge cases ---
	// Check genesis_state line exists and matches
	assert.Contains(t, renderedToml, fmt.Sprintf("genesis_state = '%s'", sampleValues.Genesis.Path), "genesis_state line missing or incorrect")

	// Check PEX value (template default overrides boolean false)
	assert.Contains(t, renderedToml, "pex                = true", "PEX line incorrect or missing")

	// Check DB.port is rendered as string (quoted)
	assert.Contains(t, renderedToml, fmt.Sprintf("port            = '%d'", sampleValues.DB.Port), "DB port line incorrect or missing")

	// --- 4. Parse Rendered TOML and Assert structured values ---
	var parsedData map[string]interface{}
	_, err = toml.Decode(renderedToml, &parsedData)
	require.NoError(t, err, "Failed to parse rendered TOML")

	// Assert some key values that are not covered by string checks
	logMap, ok := parsedData["log"].(map[string]interface{})
	require.True(t, ok, "Log section not found or not a map")
	assert.Equal(t, sampleValues.Log.Level, logMap["level"], "Log level mismatch")
	assert.Equal(t, int64(sampleValues.Log.FileRollKB), logMap["file_roll_size"], "Log file roll size mismatch") // TOML parser returns int64

	p2pMap, ok := parsedData["p2p"].(map[string]interface{})
	require.True(t, ok, "P2P section not found or not a map")
	bootnodesParsed, ok := p2pMap["bootnodes"].([]interface{})
	require.True(t, ok, "P2P bootnodes not found or not a slice")
	require.Len(t, bootnodesParsed, len(sampleValues.P2P.Bootnodes))
	for i, bn := range sampleValues.P2P.Bootnodes {
		assert.Equal(t, bn, bootnodesParsed[i].(string), "P2P Bootnode mismatch at index %d", i)
	}

	dbMap, ok := parsedData["db"].(map[string]interface{})
	require.True(t, ok, "DB section not found or not a map")
	// DB.port is quoted in template so will be string
	assert.Equal(t, fmt.Sprintf("%d", sampleValues.DB.Port), dbMap["port"], "DB port mismatch")
}
