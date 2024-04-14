package processors

import (
	. "github.com/deqing/batching-api/api"
	"golang.org/x/exp/maps"
)

type BalanceUpdate struct {
}

// Process keeps the latest balance update of a user only
// for example, if we have multiple balance updates for the same user, we only need to keep the latest one
func (m *BalanceUpdate) Process(jobs []Job) []Job {
	otherJobs := make([]Job, 0)
	jobMap := make(map[string]Job)
	for _, job := range jobs {
		if job.Type == BALANCEUPDATE {
			params, _ := job.Params.AsBalanceUpdateParams()
			jobMap[params.UserId] = job
		} else {
			otherJobs = append(otherJobs, job)
		}
	}
	return append(maps.Values(jobMap), otherJobs...)
}
