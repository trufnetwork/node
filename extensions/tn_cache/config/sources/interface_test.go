package sources

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/trufnetwork/kwil-db/core/log"
)

func TestInferRootDir(t *testing.T) {
	// Save original args and env
	originalArgs := os.Args
	originalEnv := os.Getenv(RootEnvVar)

	// Clean up after test
	defer func() {
		os.Args = originalArgs
		if originalEnv != "" {
			os.Setenv(RootEnvVar, originalEnv)
		} else {
			os.Unsetenv(RootEnvVar)
		}
	}()

	tests := []struct {
		name        string
		args        []string
		envVar      string
		expectedErr bool
		description string
	}{
		{
			name:        "root flag long form",
			args:        []string{"kwild", "--root", "/custom/path", "start"},
			envVar:      "",
			expectedErr: false,
			description: "Should parse --root flag correctly",
		},
		{
			name:        "root flag short form",
			args:        []string{"kwild", "-r", "/another/path", "start"},
			envVar:      "",
			expectedErr: false,
			description: "Should parse -r flag correctly",
		},
		{
			name:        "root flag with equals long form",
			args:        []string{"kwild", "--root=/equals/path", "start"},
			envVar:      "",
			expectedErr: false,
			description: "Should parse --root=/path correctly",
		},
		{
			name:        "root flag with equals short form",
			args:        []string{"kwild", "-r=/short/equals", "start"},
			envVar:      "",
			expectedErr: false,
			description: "Should parse -r=/path correctly",
		},
		{
			name:        "no root flag with env var",
			args:        []string{"kwild", "start"},
			envVar:      "/env/path",
			expectedErr: false,
			description: "Should fallback to environment variable",
		},
		{
			name:        "no root flag no env var",
			args:        []string{"kwild", "start"},
			envVar:      "",
			expectedErr: false,
			description: "Should fallback to default ~/.kwild",
		},
		{
			name:        "missing value for root flag",
			args:        []string{"kwild", "--root"},
			envVar:      "",
			expectedErr: true,
			description: "Should error when --root has no value",
		},
		{
			name:        "invalid value for root flag",
			args:        []string{"kwild", "--root", "-invalid", "start"},
			envVar:      "",
			expectedErr: true,
			description: "Should error when --root value looks like a flag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test environment
			os.Args = tt.args
			if tt.envVar != "" {
				os.Setenv(RootEnvVar, tt.envVar)
			} else {
				os.Unsetenv(RootEnvVar)
			}

			// Call function under test
			result, err := inferRootDir()

			// Check error expectation
			if tt.expectedErr {
				if err == nil {
					t.Errorf("Expected error but got none for test: %s", tt.description)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error for test '%s': %v", tt.description, err)
				return
			}

			// Check specific expected results
			switch tt.name {
			case "root flag long form":
				if result != "/custom/path" {
					t.Errorf("Expected '/custom/path', got '%s'", result)
				}
			case "root flag short form":
				if result != "/another/path" {
					t.Errorf("Expected '/another/path', got '%s'", result)
				}
			case "root flag with equals long form":
				if result != "/equals/path" {
					t.Errorf("Expected '/equals/path', got '%s'", result)
				}
			case "root flag with equals short form":
				if result != "/short/equals" {
					t.Errorf("Expected '/short/equals', got '%s'", result)
				}
			case "no root flag with env var":
				if result != "/env/path" {
					t.Errorf("Expected '/env/path', got '%s'", result)
				}
			case "no root flag no env var":
				// Should be home directory + .kwild
				homeDir, _ := os.UserHomeDir()
				expected := filepath.Join(homeDir, DefaultRootDir)
				if result != expected {
					t.Errorf("Expected '%s', got '%s'", expected, result)
				}
			}
		})
	}
}

func TestCreateSourcesWithAutoInferredBasePath(t *testing.T) {
	// Save original args
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
	}()

	// Set up test args with a custom root
	os.Args = []string{"kwild", "--root", "/test/root", "start"}

	factory := NewSourceFactory(log.DiscardLogger)

	// Test with CSV file but no explicit config_base_path
	rawConfig := map[string]string{
		ConfigKeyStreamsCSVFile: "test.csv",
		// Note: no ConfigKeyConfigBasePath set
	}

	sources, err := factory.CreateSources(rawConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(sources) != 1 {
		t.Fatalf("Expected 1 source, got %d", len(sources))
	}

	csvSource, ok := sources[0].(*CSVSource)
	if !ok {
		t.Fatalf("Expected CSVSource, got %T", sources[0])
	}

	// Check that the basePath was automatically inferred
	if csvSource.basePath != "/test/root" {
		t.Errorf("Expected basePath '/test/root', got '%s'", csvSource.basePath)
	}
}
