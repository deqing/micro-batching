package config

import (
	"encoding/json"
	"os"
)

type RunConfig struct {
	BatchSize int `json:"batch-size"`
	Frequency int `json:"frequency"`
}

func ReadConfig(configFile string) (RunConfig, error) {
	// read config
	file, err := os.Open(configFile)
	if err != nil {
		return RunConfig{}, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	// parse config
	var config RunConfig
	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		return RunConfig{}, err
	}

	return config, nil
}
