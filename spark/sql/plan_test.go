package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPlanIdGivesNewIDs(t *testing.T) {
	id1 := newPlanId()
	id2 := newPlanId()
	assert.NotEqual(t, id1, id2)
}
