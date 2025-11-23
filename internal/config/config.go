package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Generator GeneratorConfig `yaml:"generator"`
}

type GeneratorConfig struct {
	Rate         int          `yaml:"rate"`
	OutputFormat string       `yaml:"output_format"`
	Output       OutputConfig `yaml:"output,omitempty"`

	OperationWeights   map[string]float64  `yaml:"operation_weights"`
	Defaults           DefaultsConfig      `yaml:"defaults"`
	ProblematicBuckets []ProblematicBucket `yaml:"problematic_buckets"`
}

type OutputConfig struct {
	Kafka KafkaConfig `yaml:"kafka,omitempty"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

type DefaultsConfig struct {
	ErrorPercent            float64         `yaml:"error_percent"`
	ErrorStatusDistribution map[int]float64 `yaml:"error_status_distribution"`
	Duration                DurationConfig  `yaml:"duration"`
}

type DurationConfig struct {
	SuccessMin float64 `yaml:"success_min"`
	SuccessMax float64 `yaml:"success_max"`
	ErrorMin   float64 `yaml:"error_min"`
	ErrorMax   float64 `yaml:"error_max"`
}

type ProblematicBucket struct {
	Name                    string          `yaml:"name"`
	ErrorMultiplier         float64         `yaml:"error_multiplier"`
	DurationMultiplier      float64         `yaml:"duration_multiplier"`
	ErrorStatusDistribution map[int]float64 `yaml:"error_status_distribution,omitempty"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := validateAndSetDefaults(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

func validateAndSetDefaults(cfg *Config) error {
	if cfg.Generator.Rate <= 0 {
		cfg.Generator.Rate = 10
	}

	if cfg.Generator.OutputFormat == "" {
		cfg.Generator.OutputFormat = "json"
	}

	if cfg.Generator.Defaults.ErrorPercent == 0 {
		cfg.Generator.Defaults.ErrorPercent = 5
	}

	if len(cfg.Generator.Defaults.ErrorStatusDistribution) == 0 {
		cfg.Generator.Defaults.ErrorStatusDistribution = map[int]float64{
			404: 40,
			500: 30,
			504: 20,
			403: 10,
		}
	}

	if cfg.Generator.Defaults.Duration.SuccessMin == 0 {
		cfg.Generator.Defaults.Duration.SuccessMin = 0.05
	}
	if cfg.Generator.Defaults.Duration.SuccessMax == 0 {
		cfg.Generator.Defaults.Duration.SuccessMax = 0.5
	}
	if cfg.Generator.Defaults.Duration.ErrorMin == 0 {
		cfg.Generator.Defaults.Duration.ErrorMin = 0.01
	}
	if cfg.Generator.Defaults.Duration.ErrorMax == 0 {
		cfg.Generator.Defaults.Duration.ErrorMax = 0.2
	}

	return nil
}

func (c *Config) GetProblematicBucket(name string) *ProblematicBucket {
	for i := range c.Generator.ProblematicBuckets {
		if c.Generator.ProblematicBuckets[i].Name == name {
			return &c.Generator.ProblematicBuckets[i]
		}
	}
	return nil
}

func (c *Config) ReloadConfig(path string) error {
	newConfig, err := LoadConfig(path)
	if err != nil {
		return err
	}
	*c = *newConfig
	return nil
}

func WatchConfig(cfg *Config, path string, reloadCh chan<- *Config) {
	lastModTime := time.Time{}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stat, err := os.Stat(path)
		if err != nil {
			continue
		}

		if stat.ModTime().After(lastModTime) {
			lastModTime = stat.ModTime()
			if err := cfg.ReloadConfig(path); err == nil {
				reloadCh <- cfg
			}
		}
	}
}
