package types

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
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
	cleanupOnce  sync.Once
}

// NewRowIterator creates a new row iterator with the given context
func NewRowIterator(ctx context.Context, recordChan <-chan arrow.Record, errorChan <-chan error, schema *StructType) RowIterator {
	// Create a cancellable context derived from the parent
	iterCtx, cancel := context.WithCancel(ctx)

	return &rowIteratorImpl{
		recordChan:   recordChan,
		errorChan:    errorChan,
		schema:       schema,
		currentRows:  nil,
		currentIndex: 0,
		exhausted:    false,
		closed:       false,
		ctx:          iterCtx,
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
		iter.exhausted = true
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

// fetchNextBatch with deterministic handling to release rows before returning EOF
func (iter *rowIteratorImpl) fetchNextBatch() error {
	for {
		select {
		case <-iter.ctx.Done():
			return iter.ctx.Err()

		case record, ok := <-iter.recordChan:
			if !ok {
				// Record channel is closed - check for any final error
				return iter.checkErrorChannelOnClose()
			}

			// We have a valid record - handle nil check
			if record == nil {
				continue
			}

			// Convert to rows and release the record immediately
			rows, err := func() ([]Row, error) {
				defer record.Release()
				return ReadArrowRecordToRows(record)
			}()
			if err != nil {
				return err
			}

			iter.currentRows = rows
			iter.currentIndex = 0
			return nil

		case err, ok := <-iter.errorChan:
			if !ok {
				// Error channel closed - continue to check record channel
				// Don't immediately return EOF if there are still records to process
				select {
				case record, ok := <-iter.recordChan:
					if !ok {
						// Both channels are closed
						return io.EOF
					}

					// We have a valid record - handle nil check
					if record == nil {
						continue // Skip nil records
					}

					// Convert to rows and release the record immediately
					rows, err := func() ([]Row, error) {
						defer record.Release()
						return ReadArrowRecordToRows(record)
					}()
					if err != nil {
						return err
					}

					iter.currentRows = rows
					iter.currentIndex = 0
					return nil

				default:
					// No immediate record available, but channel isn't closed
					// Continue with the main select loop
				}
			}

			// Error received - return it (nil errors become EOF)
			if err == nil {
				return io.EOF
			}
			return err
		}
	}
}

// checkErrorChannelOnClose handles error channel when record channel closes
func (iter *rowIteratorImpl) checkErrorChannelOnClose() error {
	// If error channel is already closed, return EOF
	select {
	case err, ok := <-iter.errorChan:
		if !ok || err == nil {
			// Channel closed or nil error - normal EOF
			return io.EOF
		}
		// Got actual error
		return err
	default:
		// Error channel still open, use timeout approach
	}

	// Use a small timeout to check for any trailing errors
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()

	select {
	case err, ok := <-iter.errorChan:
		if !ok || err == nil {
			// Channel closed or nil error - normal EOF
			return io.EOF
		}
		return err
	case <-timer.C:
		// No error within timeout - assume normal EOF
		return io.EOF
	case <-iter.ctx.Done():
		// Context cancelled during wait
		return iter.ctx.Err()
	}
}

func (iter *rowIteratorImpl) Close() error {
	iter.mu.Lock()
	if iter.closed {
		iter.mu.Unlock()
		return nil
	}
	iter.closed = true
	iter.mu.Unlock()

	// Cancel the context to signal any blocked operations to stop
	iter.cancel()

	// Ensure cleanup happens only once
	iter.cleanupOnce.Do(func() {
		// Start a goroutine to drain channels
		// This prevents the producer goroutine from blocking
		go iter.drainChannels()
	})

	return nil
}

// drainChannels drains both channels to prevent producer goroutine from blocking
func (iter *rowIteratorImpl) drainChannels() {
	// Use a reasonable timeout for cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		select {
		case record, ok := <-iter.recordChan:
			if !ok {
				// Channel closed, check error channel one more time
				select {
				case <-iter.errorChan:
					// Drained
				case <-ctx.Done():
					// Timeout
				}
				return
			}
			// Release any remaining records to prevent memory leaks
			if record != nil {
				record.Release()
			}

		case <-iter.errorChan:
			// Just drain, don't process

		case <-ctx.Done():
			// Cleanup timeout - exit
			return
		}
	}
}
