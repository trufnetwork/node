package contractgen

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/kwilteam/kwil-db/core/types/transactions"
)

//go:embed composed_stream_template.json
var template []byte

// GenerateComposedStreamContract generates a contract that composes across other streams.
// It can be given a map[string]int64, which maps the streams it imports to
// the weight it should be given.
func GenerateComposedStreamContract(name string, imports map[string]int64) (*transactions.Schema, error) {
	var contract transactions.Schema
	err := json.Unmarshal(template, &contract)
	if err != nil {
		return nil, err
	}

	contract.Name = name

	count := 0
	found := false
	for _, ext := range contract.Extensions {
		// If the extension is a composed_stream extension, we can
		// add the imports to it.
		if ext.Name == "composed_stream" { // TODO: we should use a global constant string for this once other PR is merged
			found = true
			for stream, weight := range imports {

				// as discussed here: https://github.com/truflation/tsn-db/pull/52#discussion_r1506333753
				// we need to make two entries for id and weight

				ext.Config = append(ext.Config, &transactions.ExtensionConfig{
					Argument: fmt.Sprintf("stream_%d_id", count),
					Value:    stream,
				})

				ext.Config = append(ext.Config, &transactions.ExtensionConfig{
					Argument: fmt.Sprintf("stream_%d_weight", count),
					Value:    fmt.Sprint(weight),
				})

				count++
			}
		}
	}

	// if not found, we need to add it
	if !found {
		return nil, fmt.Errorf("composed_stream extension not found")
	}

	return &contract, nil
}
