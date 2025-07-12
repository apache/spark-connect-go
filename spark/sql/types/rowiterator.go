package types

import "io"

// RowIterator allows callers to pull one row at a time. `Next` returns
// `io.EOF` when the iterator is exhausted.
type RowIterator interface {
	Next() (Row, error)
}

// arrowRowIterator is the concrete implementation returned by
// `ReadArrowTableToIterator`.
type arrowRowIterator struct {
	cols       [][]any
	fieldNames []string
	totalRows  int
	idx        int
}

// Next returns the next row or `io.EOF` when finished.
func (it *arrowRowIterator) Next() (Row, error) {
	if it.idx >= it.totalRows {
		return nil, io.EOF
	}

	// materialise one row
	values := make([]any, len(it.cols))
	for j, col := range it.cols {
		values[j] = col[it.idx]
	}
	r := &rowImpl{
		values:  values,
		offsets: make(map[string]int, len(it.fieldNames)),
	}
	for j, name := range it.fieldNames {
		r.offsets[name] = j
	}

	it.idx++
	return r, nil
}
