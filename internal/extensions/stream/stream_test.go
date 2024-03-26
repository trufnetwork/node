package stream_test

import (
	"testing"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/truflation/tsn-db/internal/extensions/stream"
	"github.com/truflation/tsn-db/mocks"
)

type streamTest struct {
	stream *stream.Stream
	scoper *precompiles.ProcedureContext
	app    *common.App
}

func newStreamTest() streamTest {
	return streamTest{
		stream: &stream.Stream{},
		scoper: &precompiles.ProcedureContext{},
		app:    &common.App{},
	}
}

func TestInitializeStream(t *testing.T) {
	t.Run("success - it should return nil", func(t *testing.T) {
		_, err := stream.InitializeStream(nil, nil, nil)
		assert.NoError(t, err, "InitializeStream returned an error")
	})

	t.Run("validation - it should return error stream does not take any configs", func(t *testing.T) {
		falseMetadata := map[string]string{"key": "value"}
		_, err := stream.InitializeStream(nil, nil, falseMetadata)
		assert.EqualError(t, err, "stream does not take any configs")
	})
}

func TestStream_Call(t *testing.T) {
	instance := newStreamTest()
	mockEngine := mocks.NewEngine(t)
	instance.app.Engine = mockEngine

	t.Run("success - it should return nil when method is get_index", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil

		expectedResultSet := &sql.ResultSet{
			Columns: []string{"id"},
			Rows:    [][]interface{}{{1}},
			Status: sql.CommandTag{
				Text:         "SELECT 1",
				RowsAffected: 1,
			},
		}

		mockEngine.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(expectedResultSet, nil)
		res, err := instance.stream.Call(instance.scoper, instance.app, "get_index", []interface{}{"/path", "2021-01-01"})
		assert.NoError(t, err, "stream.Call returned an error")
		assert.Equal(t, expectedResultSet, instance.scoper.Result, "stream.Call did not return expected result")
		assert.Nil(t, res, "stream.Call returned a result")
	})

	t.Run("success - it should return nil when method is get_value", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&sql.ResultSet{}, nil)
		_, err := instance.stream.Call(instance.scoper, instance.app, "get_value", []interface{}{"path", "2021-01-01"})
		assert.NoError(t, err, "stream.Call returned an error")
	})

	t.Run("error - it should return error when Engine.Execute returns error", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		_, err := instance.stream.Call(instance.scoper, instance.app, "get_index", []interface{}{"/path", "2021-01-01"})
		assert.EqualError(t, err, assert.AnError.Error())
	})

	t.Run("validation - it should return unknown method error", func(t *testing.T) {
		_, err := instance.stream.Call(nil, nil, "unknown", nil)
		assert.Contains(t, err.Error(), "unknown method")
	})

	t.Run("validation - it should return error when inputs length is less than 2", func(t *testing.T) {
		_, err := instance.stream.Call(nil, nil, "get_index", []interface{}{})
		assert.Contains(t, err.Error(), "expected at least 2 inputs")
	})

	t.Run("validation - it should return error when inputs[0] is not string", func(t *testing.T) {
		_, err := instance.stream.Call(nil, nil, "get_index", []interface{}{1, "2021-01-01"})
		assert.Contains(t, err.Error(), "expected string")
	})

	t.Run("validation - it should return error when inputs[1] is not string", func(t *testing.T) {
		_, err := instance.stream.Call(nil, nil, "get_index", []interface{}{"path", 1})
		assert.Contains(t, err.Error(), "expected string")
	})

	t.Run("validation - it should return error when inputs[2] is not string", func(t *testing.T) {
		_, err := instance.stream.Call(nil, nil, "get_index", []interface{}{"path", "2021-01-01", 1})
		assert.Contains(t, err.Error(), "expected string")
	})

	t.Run("validation - it should return error when date is invalid", func(t *testing.T) {
		_, err := instance.stream.Call(nil, nil, "get_index", []interface{}{"path", "invalid-date"})
		assert.Contains(t, err.Error(), "invalid date")
	})
}
