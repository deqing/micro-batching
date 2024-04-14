package batching

import (
	. "github.com/deqing/batching-api/api"
	"github.com/deqing/micro-batching/internal/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTake(t *testing.T) {
	b := NewBatchingWithConfig(config.RunConfig{})

	expectedUserName := "name"
	params := JobRequest_Params{}
	err := params.FromUpdateUserInfoParams(UpdateUserInfoParams{
		UserId: "12",
		Name:   &expectedUserName,
	})
	assert.NoError(t, err)
	jobRequest := JobRequest{
		Type:   "Type",
		Params: params,
	}

	job := b.Take(jobRequest)

	assert.Equal(t, QUEUED, job.Status)
	assert.NotNil(t, job.Id)

	updateUserInfo, err := job.Params.AsUpdateUserInfoParams()
	assert.NoError(t, err)
	assert.Equal(t, "12", updateUserInfo.UserId)
	assert.Equal(t, expectedUserName, *updateUserInfo.Name)
}

func TestJobInfo(t *testing.T) {
	b := NewBatchingWithConfig(config.RunConfig{})

	expectedUserName := "name"
	params := JobRequest_Params{}
	err := params.FromUpdateUserInfoParams(UpdateUserInfoParams{
		UserId: "12",
		Name:   &expectedUserName,
	})
	assert.NoError(t, err)
	jobRequest := JobRequest{
		Type:   "Type",
		Params: params,
	}

	job := b.Take(jobRequest)

	jobInfo, err := b.JobInfo(job.Id)
	assert.NoError(t, err)

	assert.Equal(t, job, jobInfo)
}

func TestSetPreprocess(t *testing.T) {
	b := NewBatchingWithConfig(config.RunConfig{})

	b.SetPreProcess(true)
	assert.True(t, b.preProcess != nil)

	b.SetPreProcess(false)
	assert.True(t, b.preProcess == nil)
}

func TestPost(t *testing.T) {
	b := NewBatchingWithConfig(config.RunConfig{
		BatchSize: 2,
	})

	expectedUserName := "name"
	params := JobRequest_Params{}
	err := params.FromUpdateUserInfoParams(UpdateUserInfoParams{
		UserId: "12",
		Name:   &expectedUserName,
	})
	assert.NoError(t, err)
	jobRequest := JobRequest{
		Type:   "Type",
		Params: params,
	}

	b.Take(jobRequest)
	b.Take(jobRequest)
	b.Take(jobRequest)

	b.Post()
	assert.Equal(t, 1, b.queue.Size())

	b.Post()
	assert.Empty(t, 0, b.queue.Size())

	err = b.ShutDown()
	assert.NoError(t, err)
}
