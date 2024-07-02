package sparkerrors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithTypeGivesAndErrorThatIsOfThatType(t *testing.T) {
	err := WithType(assert.AnError, ConnectionError)
	assert.ErrorIs(t, err, ConnectionError)
}

func TestErrorStringContainsErrorType(t *testing.T) {
	err := WithType(assert.AnError, ConnectionError)
	assert.Contains(t, err.Error(), ConnectionError.Error())
}
