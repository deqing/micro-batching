package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadConfig(t *testing.T) {
	expectedConfig := RunConfig{
		BatchSize: 2,
		Frequency: 5, // call batch processor every 5 seconds
	}

	readConfig, err := ReadConfig("config.json")
	assert.NoError(t, err)
	assert.Equal(t, expectedConfig, readConfig)
}
