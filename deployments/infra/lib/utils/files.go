package utils

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// WriteToTempFile writes the given content to a temporary file in the OS temp directory.
// This is useful when needing to provide a file path to constructs like awss3assets.NewAsset
// from in-memory content generated during synthesis. The asset construct will copy this file
// into the CDK staging directory.
func WriteToTempFile(scope constructs.Construct, filename string, content []byte) *string {
	// Use the OS temp directory.
	tempDir := os.TempDir()
	tempFilePath := filepath.Join(tempDir, filename)

	// Write the content to the file.
	err := os.WriteFile(tempFilePath, content, 0644)
	if err != nil {
		panic(fmt.Sprintf("failed to write temporary asset file %s: %v", tempFilePath, err))
	}

	// Return the absolute path to the temporary file.
	return jsii.String(tempFilePath)
}
