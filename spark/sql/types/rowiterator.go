package types

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow"
	"io"
	"sync"
	"time"
)

// RowIterator provides streaming access to individual rows
type RowIterator interface {
	Next() (Row, error)
	io.Closer
}

// rowIteratorImpl implements RowIterator with robust cancellation handling
type rowIteratorImpl struct {
	recordChan   <-chan arrow.Record
	errorChan    <-chan error
	schema       *StructType
	currentRows  []Row
	currentIndex int
	exhausted    bool
	closed       bool
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewRowIterator(recordChan <-chan arrow.Record, errorChan <-chan error, schema *StructType) RowIterator {
	// Create a context that we can cancel when the iterator is closed
	ctx, cancel := context.WithCancel(context.Background())

	return &rowIteratorImpl{
		recordChan:   recordChan,
		errorChan:    errorChan,
		schema:       schema,
		currentRows:  nil,
		currentIndex: 0,
		exhausted:    false,
		closed:       false,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (iter *rowIteratorImpl) Next() (Row, error) {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return nil, errors.New("iterator is closed")
	}
	if iter.exhausted {
		return nil, io.EOF
	}

	// Check if context was cancelled
	select {
	case <-iter.ctx.Done():
		return nil, iter.ctx.Err()
	default:
	}

	// If we have rows in the current batch, return the next one
	if iter.currentIndex < len(iter.currentRows) {
		row := iter.currentRows[iter.currentIndex]
		iter.currentIndex++
		return row, nil
	}

	// Fetch the next batch
	if err := iter.fetchNextBatch(); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			iter.exhausted = true
		}
		return nil, err
	}

	// Return the first row from the new batch
	if len(iter.currentRows) == 0 {
		iter.exhausted = true
		return nil, io.EOF
	}

	row := iter.currentRows[0]
	iter.currentIndex = 1
	return row, nil
}

func (iter *rowIteratorImpl) fetchNextBatch() error {
	select {
	case <-iter.ctx.Done():
		return iter.ctx.Err()

	case record, ok := <-iter.recordChan:
		if !ok {
			// Channel closed - check for any errors
			select {
			case err := <-iter.errorChan:
				return err
			case <-iter.ctx.Done():
				return iter.ctx.Err()
			default:
				return io.EOF
			}
		}

		// Make sure to release the record even if conversion fails
		defer record.Release()

		// Convert the Arrow record directly to rows using the helper
		rows, err := ReadArrowRecordToRows(record)
		if err != nil {
			return err
		}

		iter.currentRows = rows
		iter.currentIndex = 0
		return nil

	case err := <-iter.errorChan:
		return err
	}
}

func (iter *rowIteratorImpl) Close() error {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return nil
	}
	iter.closed = true

	// Cancel our context to signal cleanup
	iter.cancel()

	// Drain any remaining records to prevent goroutine leaks
	// Use a separate goroutine with timeout to avoid blocking
	go func() {
		timeout := time.NewTimer(5 * time.Second)
		defer timeout.Stop()

		for {
			select {
			case record, ok := <-iter.recordChan:
				if !ok {
					return // Channel closed
				}
				record.Release()
			case <-timeout.C:
				// Timeout reached - force exit to prevent hanging
				return
			}
		}
	}()

	return nil
}
