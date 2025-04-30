package fronting

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseKind_Invalid ensures that an unrecognized kind returns an error.
func TestParseKind_Invalid(t *testing.T) {
	_, err := ParseKind("typo")
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid fronting type")
}

// TestParseKind_Valid ensures that valid kinds are parsed correctly.
func TestParseKind_Valid(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  Kind
	}{
		{string(KindAPI), KindAPI},
		{"api", KindAPI}, // Check lower case normalization
		{string(KindCloudFront), KindCloudFront},
		{string(KindALB), KindALB},
	} {
		k, err := ParseKind(tc.input)
		require.NoError(t, err)
		require.Equal(t, tc.want, k, "Input: %s", tc.input)
	}
}
