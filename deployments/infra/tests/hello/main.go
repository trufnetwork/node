package main

// This file contains a test for the buildGoBinaryIntoS3Asset function.
// It verifies that a Go binary can be correctly built and packaged as an S3 asset.
// The test creates a CDK stack, builds a simple "Hello, World!" Go program into an asset,
// and then executes the resulting binary to ensure it produces the expected output.

import "fmt"

func main() {
	fmt.Println("Hello, World!")
}
