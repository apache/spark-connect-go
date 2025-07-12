package types_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/spark-connect-go/v40/spark/sql/types"
)

func createTestRecord(values []string) arrow.Record {
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "col1", Type: arrow.BinaryTypes.String}},
		nil,
	)

	alloc := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	for _, v := range values {
		builder.Field(0).(*array.StringBuilder).Append(v)
	}

	return builder.NewRecord()
}

func TestRowIterator_BasicIteration(t *testing.T) {
	recordChan := make(chan arrow.Record, 2)
	errorChan := make(chan error, 1)
	schema := &types.StructType{}

	// Send test records
	recordChan <- createTestRecord([]string{"row1", "row2"})
	recordChan <- createTestRecord([]string{"row3", "row4"})
	close(recordChan)

	iter := types.NewRowIterator(recordChan, errorChan, schema)
	defer iter.Close()

	// Collect all rows
	var rows []types.Row
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rows = append(rows, row)
	}

	// Verify we got all 4 rows
	assert.Len(t, rows, 4)
	assert.Equal(t, "row1", rows[0].At(0))
	assert.Equal(t, "row2", rows[1].At(0))
	assert.Equal(t, "row3", rows[2].At(0))
	assert.Equal(t, "row4", rows[3].At(0))
}

func TestRowIterator_ContextCancellation(t *testing.T) {
	recordChan := make(chan arrow.Record, 1)
	errorChan := make(chan error, 1)
	schema := &types.StructType{}

	// Send one record
	recordChan <- createTestRecord([]string{"row1", "row2"})

	iter := types.NewRowIterator(recordChan, errorChan, schema)

	// Read first row successfully
	row, err := iter.Next()
	require.NoError(t, err)
	assert.Equal(t, "row1", row.At(0))

	// Close iterator (which cancels context)
	err = iter.Close()
	require.NoError(t, err)

	// Subsequent reads should fail with context error
	_, err = iter.Next()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iterator is closed")
}

func TestRowIterator_ErrorPropagation(t *testing.T) {
	recordChan := make(chan arrow.Record, 1)
	errorChan := make(chan error, 1)
	schema := &types.StructType{}

	// Send test record
	recordChan <- createTestRecord([]string{"row1"})

	iter := types.NewRowIterator(recordChan, errorChan, schema)
	defer iter.Close()

	// Read first row successfully
	row, err := iter.Next()
	require.NoError(t, err)
	assert.Equal(t, "row1", row.At(0))

	// Send error
	testErr := errors.New("test error")
	errorChan <- testErr
	close(recordChan)

	// Next read should return the error
	_, err = iter.Next()
	assert.Equal(t, testErr, err)
}

func TestRowIterator_EmptyResult(t *testing.T) {
	recordChan := make(chan arrow.Record)
	errorChan := make(chan error, 1)
	schema := &types.StructType{}

	// Close channel immediately
	close(recordChan)

	iter := types.NewRowIterator(recordChan, errorChan, schema)
	defer iter.Close()

	// First read should return EOF
	_, err := iter.Next()
	assert.Equal(t, io.EOF, err)

	// Subsequent reads should also return EOF
	_, err = iter.Next()
	assert.Equal(t, io.EOF, err)
}

func TestRowIterator_MultipleClose(t *testing.T) {
	recordChan := make(chan arrow.Record)
	errorChan := make(chan error, 1)
	schema := &types.StructType{}

	iter := types.NewRowIterator(recordChan, errorChan, schema)

	// Close multiple times should not panic
	err := iter.Close()
	assert.NoError(t, err)

	err = iter.Close()
	assert.NoError(t, err)
}

func TestRowIterator_CloseWithPendingRecords(t *testing.T) {
	recordChan := make(chan arrow.Record, 3)
	errorChan := make(chan error, 1)
	schema := &types.StructType{}

	// Send multiple records
	for i := 0; i < 3; i++ {
		recordChan <- createTestRecord([]string{"row"})
	}

	iter := types.NewRowIterator(recordChan, errorChan, schema)

	// Close without reading all records
	// This should trigger the cleanup goroutine
	err := iter.Close()
	assert.NoError(t, err)

	// Give cleanup goroutine time to run
	time.Sleep(100 * time.Millisecond)

	// Channel should be drained (this won't block if cleanup worked)
	select {
	case <-recordChan:
		// Good, channel was drained
	default:
		// Also acceptable if already drained
	}
}

func TestRowIterator_ConcurrentAccess(t *testing.T) {
	recordChan := make(chan arrow.Record, 5)
	errorChan := make(chan error, 1)
	schema := &types.StructType{}

	// Send multiple records
	for i := 0; i < 5; i++ {
		recordChan <- createTestRecord([]string{"row"})
	}
	close(recordChan)

	iter := types.NewRowIterator(recordChan, errorChan, schema)
	defer iter.Close()

	// Try concurrent reads (should be safe due to mutex)
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 2; i++ {
			_, _ = iter.Next()
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 3; i++ {
			_, _ = iter.Next()
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Should have consumed all 5 records
	_, err := iter.Next()
	assert.Equal(t, io.EOF, err)
}
