package processors

import (
	. "github.com/deqing/batching-api/api"
)

type DummyProcessor struct {
}

func (m *DummyProcessor) Process(jobs []Job) []Job {
	return jobs
}
