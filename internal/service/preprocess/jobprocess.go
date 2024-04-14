package preprocess

import (
	. "github.com/deqing/batching-api/api"
	"sync"
)

type Processor interface {
	Process([]Job) []Job
}

type JobProcess struct {
	processors map[JobType][]Processor
}

func NewJobProcess() *JobProcess {
	return &JobProcess{
		make(map[JobType][]Processor),
	}
}

// Use adds a processor to the job process
func (m *JobProcess) Use(jobType JobType, processor Processor) {
	m.processors[jobType] = append(m.processors[jobType], processor)
}

// Process split jobs based on their types, and then processes them with the processors which designed for the job type
func (m *JobProcess) Process(allJobs []Job) []Job {
	jobMap := Split(allJobs)
	resultChan := make(chan []Job, len(jobMap))

	var wg sync.WaitGroup
	wg.Add(len(jobMap))
	for jobType, jobs := range jobMap {
		go processJobs(jobs, m.processors[jobType], &wg, resultChan)
	}

	wg.Wait()
	close(resultChan)

	var jobs []Job
	for result := range resultChan {
		jobs = append(jobs, result...)
	}

	return jobs
}

func processJobs(
	jobs []Job,
	processors []Processor,
	wg *sync.WaitGroup,
	resultChan chan<- []Job,
) {
	defer wg.Done()

	for _, processor := range processors {
		jobs = processor.Process(jobs)
	}

	resultChan <- jobs
}
