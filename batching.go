package batching

import (
	"github.com/deqing/batch-processor"
	. "github.com/deqing/batching-api/api"
	"github.com/deqing/micro-batching/internal/config"
	"github.com/deqing/micro-batching/internal/service"
	"github.com/deqing/micro-batching/internal/service/preprocess"
	"github.com/deqing/micro-batching/internal/service/preprocess/processors"
	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)

type Batching struct {
	config         config.RunConfig
	queue          service.Queue
	batchProcessor batchprocessor.BatchProcessor
	scheduler      gocron.Scheduler
	preProcess     *preprocess.JobProcess
	batchesWg      sync.WaitGroup
}

func NewBatching() (Batching, error) {
	c, err := config.ReadConfig("config.json")
	if err != nil {
		return Batching{}, err
	}

	return Batching{
		config: c,
		queue:  service.Queue{},
	}, nil
}

func NewBatchingWithConfig(c config.RunConfig) Batching {
	return Batching{
		config: c,
		queue:  service.Queue{},
	}
}

// Start create a new scheduler and start it
func (b *Batching) Start() {
	s, err := gocron.NewScheduler()
	if err != nil {
		log.Fatal("Failed to create new scheduler")
		return
	}

	b.scheduler = s
	_, err = b.scheduler.NewJob(
		gocron.DurationJob(
			time.Duration(b.config.Frequency)*time.Second,
		),
		gocron.NewTask(
			func() {
				b.Post()
			},
		),
	)

	if err != nil {
		log.Fatal("Failed to create new job")
		return
	}
	b.scheduler.Start()
}

// Restart stops the scheduler and starts it again
func (b *Batching) Restart() {
	if b.scheduler == nil {
		return
	}

	err := b.scheduler.Shutdown()
	if err != nil {
		log.Fatal("Failed to shutdown scheduler")
		return
	}
	b.Start()
}

// Take creates a new job and adds it to the queue
func (b *Batching) Take(jobRequest JobRequest) Job {
	job := Job{
		Id:     uuid.New(),
		Status: QUEUED,
		Type:   jobRequest.Type,
		Name:   jobRequest.Name,
		Params: Job_Params(jobRequest.Params),
	}

	b.queue.Enqueue(job)

	return job
}

// JobInfo returns the job by the given id
func (b *Batching) JobInfo(id uuid.UUID) (Job, error) {
	j, err := b.queue.Find(id)
	if err != nil {
		return Job{}, err
	}

	return j, nil
}

func (b *Batching) GetFrequency() BatchFrequency {
	return BatchFrequency{
		Frequency: b.config.Frequency,
	}
}

func (b *Batching) GetBatchSize() BatchSize {
	return BatchSize{
		BatchSize: b.config.BatchSize,
	}
}

func (b *Batching) SetFrequency(frequency BatchFrequency) {
	b.config.Frequency = frequency.Frequency
}

func (b *Batching) SetBatchSize(batchSize BatchSize) {
	b.config.BatchSize = batchSize.BatchSize
}

// SetPreProcess adds or removes preprocessing to the batching
func (b *Batching) SetPreProcess(on bool) {
	if on {
		if b.preProcess != nil {
			log.Print("preprocessing is already on")
		} else {
			b.preProcess = preprocess.NewJobProcess()
			b.preProcess.Use(UPDATEUSERINFO, &processors.DummyProcessor{})
			b.preProcess.Use(BALANCEUPDATE, &processors.BalanceUpdate{})
			log.Print("Added preprocessing")
		}
	} else {
		b.preProcess = nil
		log.Print("Removed preprocessing")
	}
}

// Post prepares the batch of jobs and starts a new goroutine to process it
func (b *Batching) Post() {
	if b.queue.Size() == 0 {
		log.Print("No jobs to process")
		return
	}

	if b.preProcess != nil {
		b.queue.EnqueueJobs(
			b.preProcess.Process(b.queue.Dequeue(b.queue.Size())))
	}

	jobs := b.queue.Dequeue(b.config.BatchSize)

	b.batchesWg.Add(1)
	go b.post(jobs, &b.batchesWg)
}

// post processes the batch of jobs and finishes the wait group
func (b *Batching) post(jobs []Job, wg *sync.WaitGroup) {
	defer wg.Done()
	b.batchProcessor.ProcessAndSleep(jobs, 5)
	log.Print("Batch processed")
}

// ShutDown stops the scheduler and waits for all batches to finish
func (b *Batching) ShutDown() error {
	if b.scheduler != nil {
		err := b.scheduler.Shutdown()
		if err != nil {
			return err
		}
	}

	b.batchesWg.Wait()
	return nil
}
