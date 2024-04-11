// package cli contains a command line tool for generating contracts
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	contractgen "github.com/truflation/tsn-db/scripts/contract_gen"
)

func main() {
	// Define flags
	nameFlag := flag.String("name", "World", "a name")
	importFlag := flag.String("import", "", "imports and weights, separated by commas")
	outFlag := flag.String("out", "output.txt", "output file")

	// Parse flags
	flag.Parse()

	weightMap := make(map[string]int64)

	// Assuming the importFlag follows the format "import1:weight1,import2:weight2"
	if *importFlag != "" {
		importsAndWeights := strings.Split(*importFlag, ",")
		for _, iw := range importsAndWeights {
			iwSplit := strings.Split(iw, ":")
			if len(iwSplit) != 2 {
				fmt.Println("Invalid import format")
				return
			}

			intWeight, err := strconv.ParseInt(iwSplit[1], 10, 64)
			if err != nil {
				fmt.Println("Invalid weight: ", err)
				return
			}

			weightMap[iwSplit[0]] = intWeight
		}
	}

	// Generate contract
	contract, err := contractgen.GenerateComposedStreamContract(*nameFlag, weightMap)
	if err != nil {
		fmt.Println("Error generating contract: ", err)
		return
	}

	// Write contract to file
	bts, err := json.Marshal(contract)
	if err != nil {
		fmt.Println("Error marshalling contract: ", err)
		return
	}

	err = os.WriteFile(*outFlag, bts, 0644)
	if err != nil {
		fmt.Println("Error writing contract to file: ", err)
		return
	}

	fmt.Println("Contract written to ", *outFlag)
}
