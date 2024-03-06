// package stream is an extension for the Truflation stream primitive.
// It allows data to be pulled from valid streams.
package compose_streams

import (
	"fmt"
	"github.com/kwilteam/kwil-db/internal/sql"
	utils "github.com/kwilteam/kwil-db/truflation/tsn/utils"
	"sort"
	"strconv"
	"strings"

	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"github.com/kwilteam/kwil-db/truflation/tsn"
)

// InitializeStream initializes the stream extension.
//
//	usage: use truflation_streams {
//	   stream_1_id: '/com_yahoo_finance_corn_futures',
//	   stream_1_weight: 1,
//	   stream_2_id: '/com_truflation_us_hotel_price',
//	   stream_2_weight: 9
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

	totalWeight := int64(0)
	weightMap := make(map[string]int64)
	for key, dbIdOrPath := range ids {
		weightStr, ok := metadata[key+"_weight"]
		if !ok {
			return nil, fmt.Errorf("missing weightStr for stream %s", dbIdOrPath)
		}
		weightInt, err := strconv.ParseInt(weightStr, 10, 64)
		if err != nil {
			return nil, err
		}
		DBID, err := utils.GetDBIDFromPath(ctx, dbIdOrPath)
		if err != nil {
			return nil, err
		}
		totalWeight += weightInt
		weightMap[DBID] = weightInt
	}

	return &Stream{
		weightMap:   weightMap,
		totalWeight: totalWeight,
	}, nil
}

// Stream is the namespace for the stream extension.
// Stream has two methods: "index" and "value".
// Both of them get the value of the target stream at the given time.
type Stream struct {
	weightMap   map[string]int64
	totalWeight int64
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
		return nil, fmt.Errorf("expected 2 inputs, got %d", len(inputs))
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
	calculateWeightedResultsFromStream := func(dbId string) ([]utils.ValueWithDate, error) {
		return CallOnTargetDBID(scoper, method, dbId, date, dateTo)
	}

	finalValues, err := s.CalculateWeightedResultsWithFn(calculateWeightedResultsFromStream)
	if err != nil {
		return nil, fmt.Errorf("error calculating weighted results: %w", err)
	}

	rowsResult := make([][]any, len(finalValues))
	for i, val := range finalValues {
		rowsResult[i] = []any{val.Date, val.Value}
	}

	scoper.Result = &sql.ResultSet{
		ReturnedColumns: []string{"date", "value"},
		Rows:            rowsResult,
	}

	// dummy return, as result is what matters
	return []any{0}, nil
}

func (s *Stream) CalculateWeightedResultsWithFn(fn func(string) ([]utils.ValueWithDate, error)) ([]utils.ValueWithDate, error) {
	var resultsSet [][]utils.ValueWithDate
	// for each database, get the value and multiply by the weight
	for dbId, weight := range s.weightMap {
		results, err := fn(dbId)
		//results, err := CallOnTargetDBID(scoper, method, dbId, date, dateTo)
		if err != nil {
			return nil, err
		}

		weightedResults := make([]utils.ValueWithDate, len(results))
		for i, val := range results {
			// by multiplying by `precisionMagnifier`, we can keep 3 decimal places of precision with int64 operations
			precisionMagnifier := int64(1000)
			weightedValue, err := utils.Fraction(val.Value, weight*precisionMagnifier, s.totalWeight*precisionMagnifier)
			if err != nil {
				return nil, err
			}
			weightedResults[i] = utils.ValueWithDate{Date: val.Date, Value: weightedValue}
		}
		resultsSet = append(resultsSet, weightedResults)
	}

	filledResultSet := FillForwardWithLatestFromCols(resultsSet)

	numResults := len(filledResultSet[0])
	// sum the results
	finalValues := make([]utils.ValueWithDate, numResults)
	for _, results := range filledResultSet {
		// error if the number of results is different
		if len(results) != numResults {
			return nil, fmt.Errorf("different number of results from databases")
		}
		for i, val := range results {
			finalValues[i].Value += val.Value
			finalValues[i].Date = val.Date
		}
	}

	return finalValues, nil
}

// FillForwardWithLatestFromCols fills forward the given values with the latest value from the given columns.
// Example:
// | Date  | stream A | stream B |   composed   |
// |-------|----------|----------|--------------|
// | 01jan | A        | B        | A . B        |
// | 02jan | A        | -        | A . (01jan)B |
// | 03jan | A        | -        | A . (01jan)B |
// | …     | ..       | ..       |              |
// | 01feb | A        | B        | A . B        |
func FillForwardWithLatestFromCols(originalResultsSet [][]utils.ValueWithDate) [][]utils.ValueWithDate {
	numOfStreams := len(originalResultsSet)

	// Determine the total number of dates across all streams to ensure we cover all dates
	// we use map[int] because we want to track the index of streams
	allDatesMap := make(map[string]map[int]utils.ValueWithDate)
	for streamIdx, streamResults := range originalResultsSet {
		for _, result := range streamResults {
			dateResultMap := allDatesMap[result.Date]

			// if the dateResultMap is nil, we create it
			if dateResultMap == nil {
				dateResultMap = make(map[int]utils.ValueWithDate)
				allDatesMap[result.Date] = dateResultMap
			}
			dateResultMap[streamIdx] = result
		}
	}

	// Convert dates from set to slice and sort
	allDates := make([]string, 0, len(allDatesMap))
	for date := range allDatesMap {
		allDates = append(allDates, date)
	}

	// if something was processed out of order, here we make sure to make it deterministic
	sort.Strings(allDates)

	// Prepare the latest value holder for each streamResults
	latestValueMap := make([]any, numOfStreams)

	// Prepare the structure for new results
	newResults := make([][]utils.ValueWithDate, numOfStreams)
	for i := range newResults {
		newResults[i] = make([]utils.ValueWithDate, 0, len(allDates))
	}

	// Fill in the new results, iterating through each date
	for _, date := range allDates {
		newValuesToBePushed := make([]utils.ValueWithDate, numOfStreams)
		should_discard := false
		for streamIdx := range originalResultsSet {
			valueFoundForCurrentStream := allDatesMap[date][streamIdx]

			// this means that there was no value for this stream at this date
			if valueFoundForCurrentStream.Date == "" {
				// there were no value for this stream at this date

				// is there a last value already?
				latestValue := latestValueMap[streamIdx]
				if latestValue == nil {
					// there was no value, nor a latest value. It may happen at the beginning of the stream
					should_discard = true
					continue
				}

				latestValueCorrectType, ok := latestValue.(int64)
				if !ok {
					// this should never happen
					panic(fmt.Sprintf("latest value has wrong type: %T", latestValue))
				}
				// if there was a latest value, we update the latest value
				newValuesToBePushed[streamIdx] = utils.ValueWithDate{Date: date, Value: latestValueCorrectType}
			} else {
				// if there was a value, we update the latest value
				latestValueMap[streamIdx] = valueFoundForCurrentStream.Value
				newValuesToBePushed[streamIdx] = valueFoundForCurrentStream
			}

		}

		if !should_discard {
			// if we made to here, this date has values for all streams, we can push it to the new results
			for streamIdx, val := range newValuesToBePushed {
				newResults[streamIdx] = append(newResults[streamIdx], val)
			}
		}
	}

	return newResults
}

// CallOnTargetDBID calls the given method on the target database.
// So streams extension is about calling the same method on similar databases, that implements the same methods with the
// same signature.
func CallOnTargetDBID(scoper *execution.ProcedureContext, method string, target string,
	date string, dateTo string) ([]utils.ValueWithDate, error) {
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

	result, err := utils.GetScalarWithDate(newScope.Result)
	if err != nil {
		return nil, fmt.Errorf("error getting scalar with date (dbid=%s): %w", target, err)
	}

	return result, nil
}

type knownMethod string

const (
	knownMethodIndex knownMethod = "get_index"
	knownMethodValue knownMethod = "get_value"
)
