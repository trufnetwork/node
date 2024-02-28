// package stream is an extension for the Truflation stream primitive.
// It allows data to be pulled from valid streams.
package compose_streams

import (
	"fmt"
	"github.com/kwilteam/kwil-db/internal/sql"
	utils "github.com/kwilteam/kwil-db/truflation/tsn/utils"
	"math"
	"strconv"
	"strings"

	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"github.com/kwilteam/kwil-db/truflation/tsn"
)

// InitializeStream initializes the stream extension.
//
//	usage: use truflation_streams {
//	   stream_1_id: '/com_yahoo_finance_corn_futures',
//	   stream_1_weight: '0.1',
//	   stream_2_id: '/com_truflation_us_hotel_price',
//	   stream_2_weight: '0.9'
//	} as streams;
//
// It takes no configs.
func InitializeStream(ctx *execution.DeploymentContext, metadata map[string]string) (execution.ExtensionNamespace, error) {
	ids := make(map[string]string)
	for key, value := range metadata {
		if strings.HasSuffix(key, "_id") {
			prefix := key[:len(key)-3]
			ids[prefix] = value
		}
	}

	weightMap := make(map[string]float64)
	for key, dbIdOrPath := range ids {
		weight, ok := metadata[key+"_weight"]
		if !ok {
			return nil, fmt.Errorf("missing weight for stream %s", dbIdOrPath)
		}
		weightFloat, err := strconv.ParseFloat(weight, 32)
		if err != nil {
			return nil, err
		}
		DBID, err := utils.GetDBIDFromPath(ctx, dbIdOrPath)
		if err != nil {
			return nil, err
		}
		weightMap[DBID] = weightFloat
	}

	// ensure weightmap sum is 1
	var sum float64
	for _, weight := range weightMap {
		sum += weight
	}
	if sum != 1 {
		return nil, fmt.Errorf("weights do not sum to 1")
	}

	return &Stream{
		weightMap: weightMap,
	}, nil
}

// Stream is the namespace for the stream extension.
// Stream has two methods: "index" and "value".
// Both of them get the value of the target stream at the given time.
type Stream struct {
	weightMap map[string]float64
}

func (s *Stream) Call(scoper *execution.ProcedureContext, method string, inputs []any) ([]any, error) {
	switch strings.ToLower(method) {
	// we verify that the method is one of the known methods, as they should be also implemented by the target
	case string(knownMethodIndex):
		// do nothing
	case string(knownMethodValue):
		// do nothing
	default:
		return nil, fmt.Errorf("unknown method '%s'", method)
	}

	if len(inputs) != 2 {
		return nil, fmt.Errorf("expected at least 2 inputs, got %d", len(inputs))
	}

	date, ok := inputs[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", inputs[1])
	}

	dateTo, ok := inputs[1].(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", inputs[1])
	}

	if !tsn.IsValidDate(date) {
		return nil, fmt.Errorf("invalid date: %s", date)
	}
	if !tsn.IsValidDate(dateTo) {
		if dateTo != "" {
			return nil, fmt.Errorf("invalid date: %s", dateTo)
		}
	}

	// easier to test if we pass the function as a parameter
	calculateWeightedResultsFromStream := func(dbId string) ([]int64, error) {
		return CallOnTargetDBID(scoper, method, dbId, date, dateTo)
	}

	finalValues, err := s.CalculateWeightedResultsWithFn(calculateWeightedResultsFromStream)
	if err != nil {
		return nil, err
	}

	rowsResult := make([][]any, len(finalValues))
	for i, val := range finalValues {
		rowsResult[i] = []any{val}
	}

	scoper.Result = &sql.ResultSet{
		ReturnedColumns: []string{"value"},
		Rows:            rowsResult,
	}

	// dummy return, as result is what matters
	return []any{0}, nil
}

func (s *Stream) CalculateWeightedResultsWithFn(fn func(string) ([]int64, error)) ([]int64, error) {
	// although weights are stored as float64, we will use int64 for the final value. It should remain correct, as the
	// original result is already multiplied by 1000.
	var resultsSet [][]float64
	// for each database, get the value and multiply by the weight
	for dbId, weight := range s.weightMap {
		results, err := fn(dbId)
		//results, err := CallOnTargetDBID(scoper, method, dbId, date, dateTo)
		if err != nil {
			return nil, err
		}

		weightedResults := make([]float64, len(results))
		for i, val := range results {
			weightedResults[i] = float64(val) * weight
		}
		resultsSet = append(resultsSet, weightedResults)
	}

	numResults := len(resultsSet[0])

	// sum the results
	finalValuesFloat := make([]float64, numResults)
	for _, results := range resultsSet {
		// error if the number of results is different
		if len(results) != numResults {
			return nil, fmt.Errorf("different number of results from databases")
		}
		for i, val := range results {
			finalValuesFloat[i] += val
		}
	}

	finalValues := make([]int64, numResults)
	for i, intVal := range finalValuesFloat {
		// round values to the nearest integer
		finalValues[i] = int64(math.Round(intVal))
	}
	return finalValues, nil
}

// CallOnTargetDBID calls the given method on the target database.
// So streams extension is about calling the same method on similar databases, that implements the same methods with the
// same signature.
func CallOnTargetDBID(scoper *execution.ProcedureContext, method string, target string,
	date string, dateTo string) ([]int64, error) {
	dataset, err := scoper.Dataset(target)
	if err != nil {
		return nil, err
	}

	// the stream protocol returns results as relations
	// we need to create a new scope to get the result
	newScope := scoper.NewScope()
	_, err = dataset.Call(newScope, method, []any{date, dateTo})
	if err != nil {
		return nil, err
	}

	if newScope.Result == nil {
		return nil, fmt.Errorf("stream returned nil result")
	}

	// create a result array that will be the rows of the result
	result := make([]int64, len(newScope.Result.Rows))
	for i, row := range newScope.Result.Rows {
		// expect all rows to return int64 results in 1 column only
		if len(row) != 1 {
			return nil, fmt.Errorf("stream returned %d columns, expected 1", len(row))
		}
		val, ok := row[0].(int64)
		if !ok {
			return nil, fmt.Errorf("stream returned %T, expected int64", row[0])
		}
		result[i] = val
	}

	return result, nil
}

type knownMethod string

const (
	knownMethodIndex knownMethod = "get_index"
	knownMethodValue knownMethod = "get_value"
)
