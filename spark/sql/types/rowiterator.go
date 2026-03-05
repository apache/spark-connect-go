package types

import (
	"context"
	"errors"
	"io"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
)

// rowIterFromRecord converts an Arrow record into a row iterator,
// releasing the record when iteration completes or the consumer stops.
func rowIterFromRecord(rec arrow.Record) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		defer rec.Release()
		rows, err := ReadArrowRecordToRows(rec)
		if err != nil {
			_ = yield(nil, err)
			return
		}
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

// NewRowSequence flattens record batches to a sequence of rows stream.
func NewRowSequence(ctx context.Context, recordSeq iter.Seq2[arrow.Record, error]) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		for rec, recErr := range recordSeq {
			select {
			case <-ctx.Done():
				_ = yield(nil, ctx.Err())
				return
			default:
			}

			// Treat io.EOF as clean stream termination. Some Spark
			// implementations (notably Databricks clusters as of 05/2025)
			// yield EOF as an error value instead of ending the sequence.
			if errors.Is(recErr, io.EOF) {
				return
			}
			if recErr != nil {
				_ = yield(nil, recErr)
				return
			}
			if rec == nil {
				_ = yield(nil, errors.New("expected non-nil arrow.Record, got nil"))
				return
			}

			for row, err := range rowIterFromRecord(rec) {
				cont := yield(row, err)
				if err != nil || !cont {
					return
				}
			}
		}
	}
}
