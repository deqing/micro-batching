package service

import (
	"fmt"
	. "github.com/deqing/batching-api/api"
	"github.com/google/uuid"
)

type Queue struct {
	buffer []Job
}

func (q *Queue) Enqueue(jobs Job) {
	q.buffer = append(q.buffer, jobs)
}

func (q *Queue) EnqueueJobs(jobs []Job) {
	q.buffer = append(q.buffer, jobs...)
}

func (q *Queue) Dequeue(n int) []Job {
	if n > len(q.buffer) {
		n = len(q.buffer)
	}

	jobs := q.buffer[:n]
	q.buffer = q.buffer[n:]
	return jobs
}

func (q *Queue) Find(id uuid.UUID) (Job, error) {
	for _, job := range q.buffer {
		if job.Id == id {
			return job, nil
		}
	}

	return Job{}, fmt.Errorf("job not found")
}

func (q *Queue) Size() int {
	return len(q.buffer)
}
