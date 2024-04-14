package processors

import (
	. "github.com/deqing/batching-api/api"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDummyProcessor(t *testing.T) {
	processor := &DummyProcessor{}

	jobs := processor.Process([]Job{{}, {}})

	assert.Equal(t, []Job{{}, {}}, jobs)
}
