package main

import (
	"fmt"
	"kwil/cmd/kwild/server"
	"os"
)

func main() {
	if err := server.Start(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
