package preprocess

import (
	. "github.com/deqing/batching-api/api"
)

// Split jobs based on their types
func Split(jobs []Job) map[JobType][]Job {
	jobMap := make(map[JobType][]Job)
	for _, job := range jobs {
		jobMap[job.Type] = append(jobMap[job.Type], job)
	}
	return jobMap
}
