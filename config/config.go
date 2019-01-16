package config

import (
	"github.com/prometheus/common/model"
)

type Config struct {
	Path             string
	ListenAddr       string
	Retention        model.Duration
	MinBlockDuration model.Duration
	MaxBlockDuration model.Duration
}

func NewConfig() *Config {
	return &Config{}
}
