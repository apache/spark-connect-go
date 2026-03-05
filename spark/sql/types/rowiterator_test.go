package types_test

import (
	"context"
	"errors"
	"io"
	"iter"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/spark-connect-go/spark/sql/types"
)

// Helper function to create test records
func createTestRecord(values []string) arrow.Record {
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "col1", Type: arrow.BinaryTypes.String}},
		nil,
	)

	alloc := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(alloc, schema)

	for _, v := range values {
		builder.Field(0).(*array.StringBuilder).Append(v)
	}

	record := builder.NewRecord()
	builder.Release()

	return record
}

// Helper function to create a Seq2 iterator from test data
func createTestSeq2(records []arrow.Record, err error) iter.Seq2[arrow.Record, error] {
	return func(yield func(arrow.Record, error) bool) {
		// Yield each record
		for _, record := range records {
			// Retain before yielding since consumer will release
			record.Retain()
			if !yield(record, nil) {
				return
			}
		}

		if err != nil {
			yield(nil, err)
		}
	}
}

func TestRowIterator_BasicIteration(t *testing.T) {
	// Create test records
	records := []arrow.Record{
		createTestRecord([]string{"row1", "row2"}),
		createTestRecord([]string{"row3", "row4"}),
	}

	// Clean up records after test
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	seq2 := createTestSeq2(records, nil)

	rowIter := types.NewRowSequence(context.Background(), seq2)

	// Collect all rows
	var rows []types.Row
	for row, err := range rowIter {
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

func TestRowIterator_EmptyResult(t *testing.T) {
	// Create empty Seq2
	seq2 := func(yield func(arrow.Record, error) bool) {
		// Don't yield anything - sequence is immediately over
	}

	next := types.NewRowSequence(context.Background(), seq2)

	// Should iterate zero times
	count := 0
	for _, err := range next {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
}

func TestRowIterator_ErrorPropagation(t *testing.T) {
	testErr := errors.New("test error")

	// Create Seq2 that yields one record then an error
	seq2 := func(yield func(arrow.Record, error) bool) {
		record := createTestRecord([]string{"row1"})
		record.Retain() // Consumer will release
		if !yield(record, nil) {
			record.Release() // Clean up if yield returns false
			return
		}
		yield(nil, testErr)
	}

	next := types.NewRowSequence(context.Background(), seq2)

	var rows []types.Row
	var gotError error

	for row, err := range next {
		if err != nil {
			gotError = err
			break
		}
		rows = append(rows, row)
	}

	// Should have read first row successfully
	assert.Len(t, rows, 1)
	assert.Equal(t, "row1", rows[0].At(0))

	// Should have received the error
	assert.Equal(t, testErr, gotError)
}

func TestRowIterator_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Create a Seq2 that yields records indefinitely
	seq2 := func(yield func(arrow.Record, error) bool) {
		for {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
				record := createTestRecord([]string{"row"})
				record.Retain() // Consumer will release
				if !yield(record, nil) {
					record.Release()
					return
				}
			}
		}
	}

	next := types.NewRowSequence(ctx, seq2)

	var rows []types.Row
	count := 0

	for row, err := range next {
		if err != nil {
			assert.ErrorIs(t, err, context.Canceled)
			break
		}
		rows = append(rows, row)
		count++

		// Cancel after first row
		if count == 1 {
			cancel()
		}

		if count > 10 {
			break
		}
	}

	assert.GreaterOrEqual(t, len(rows), 1)
	assert.Equal(t, "row", rows[0].At(0))
}

func TestRowIterator_EarlyBreak(t *testing.T) {
	// Create multiple records
	records := []arrow.Record{
		createTestRecord([]string{"row1"}),
		createTestRecord([]string{"row2"}),
		createTestRecord([]string{"row3"}),
	}

	// Clean up records after test
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	seq2 := createTestSeq2(records, nil)

	next := types.NewRowSequence(context.Background(), seq2)

	// Read only one row then break
	var rows []types.Row
	for row, err := range next {
		require.NoError(t, err)
		rows = append(rows, row)
		if len(rows) >= 1 {
			break // Early termination
		}
	}

	// Should have only one row
	assert.Len(t, rows, 1)
	assert.Equal(t, "row1", rows[0].At(0))
}

func TestRowIterator_EmptyBatchHandling(t *testing.T) {
	// Test handling of empty records (0 rows but valid record)
	emptyRecord := createTestRecord([]string{}) // No rows
	validRecord := createTestRecord([]string{"row1"})

	records := []arrow.Record{emptyRecord, validRecord}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	seq2 := createTestSeq2(records, nil)
	next := types.NewRowSequence(context.Background(), seq2)

	// Should skip empty batch and return row from second batch
	var rows []types.Row
	for row, err := range next {
		require.NoError(t, err)
		rows = append(rows, row)
	}

	assert.Len(t, rows, 1)
	assert.Equal(t, "row1", rows[0].At(0))
}

func TestRowIterator_DatabricksEOFBehavior(t *testing.T) {
	// Test Databricks-specific behavior where io.EOF is sent as an error
	// value rather than just ending the sequence. NewRowSequence treats
	// io.EOF as clean termination.
	seq2 := func(yield func(arrow.Record, error) bool) {
		record1 := createTestRecord([]string{"row1", "row2"})
		record1.Retain()
		if !yield(record1, nil) {
			record1.Release()
			return
		}

		record2 := createTestRecord([]string{"row3"})
		record2.Retain()
		if !yield(record2, nil) {
			record2.Release()
			return
		}

		// Databricks sends io.EOF as error — should terminate cleanly
		yield(nil, io.EOF)
	}

	next := types.NewRowSequence(context.Background(), seq2)

	// Read all rows successfully
	var rows []types.Row
	for row, err := range next {
		require.NoError(t, err)
		rows = append(rows, row)
	}

	// Should have all 3 rows
	assert.Len(t, rows, 3)
	assert.Equal(t, "row1", rows[0].At(0))
	assert.Equal(t, "row2", rows[1].At(0))
	assert.Equal(t, "row3", rows[2].At(0))
}

func TestRowIterator_NilRecordReturnsError(t *testing.T) {
	// Test that receiving a nil record returns an error
	seq2 := func(yield func(arrow.Record, error) bool) {
		record := createTestRecord([]string{"row1"})
		record.Retain()
		if !yield(record, nil) {
			record.Release()
			return
		}

		// Yield nil record (shouldn't happen in production)
		yield(nil, nil)
	}

	next := types.NewRowSequence(context.Background(), seq2)

	var rows []types.Row
	var gotError error

	for row, err := range next {
		if err != nil {
			gotError = err
			break
		}
		rows = append(rows, row)
	}

	// Should have read first row successfully
	assert.Len(t, rows, 1)
	assert.Equal(t, "row1", rows[0].At(0))

	// Should have received error about nil record
	assert.Error(t, gotError)
	assert.Contains(t, gotError.Error(), "expected non-nil arrow.Record, got nil")
}

func TestRowSeq2_DirectUsage(t *testing.T) {
	// Test using NewRowSequence directly as a Seq2
	records := []arrow.Record{
		createTestRecord([]string{"row1", "row2"}),
		createTestRecord([]string{"row3"}),
	}

	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	recordSeq := createTestSeq2(records, nil)
	rowSeq := types.NewRowSequence(context.Background(), recordSeq)

	var rows []types.Row
	for row, err := range rowSeq {
		require.NoError(t, err)
		rows = append(rows, row)
	}

	// Should have all 3 rows flattened
	assert.Len(t, rows, 3)
	assert.Equal(t, "row1", rows[0].At(0))
	assert.Equal(t, "row2", rows[1].At(0))
	assert.Equal(t, "row3", rows[2].At(0))
}

func TestRowIterator_MultipleIterations(t *testing.T) {
	// Test that ranging the same iterator twice works safely when the
	// upstream is single-use (like a real gRPC stream).
	records := []arrow.Record{
		createTestRecord([]string{"row1", "row2"}),
	}

	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	// Build a single-use upstream to simulate a gRPC stream.
	exhausted := false
	seq2 := func(yield func(arrow.Record, error) bool) {
		if exhausted {
			return
		}
		exhausted = true
		for _, record := range records {
			record.Retain()
			if !yield(record, nil) {
				return
			}
		}
	}

	next := types.NewRowSequence(context.Background(), seq2)

	var rows1 []types.Row
	for row, err := range next {
		require.NoError(t, err)
		rows1 = append(rows1, row)
	}
	assert.Len(t, rows1, 2)

	// Second iteration — upstream exhausted, should yield nothing
	var rows2 []types.Row
	for row, err := range next {
		require.NoError(t, err)
		rows2 = append(rows2, row)
	}
	assert.Len(t, rows2, 0)
}
