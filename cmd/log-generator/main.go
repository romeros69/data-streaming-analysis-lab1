package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/romeros69/data-streaming-analysis-lab1/internal/config"
	"github.com/romeros69/data-streaming-analysis-lab1/internal/generator"
	"github.com/romeros69/data-streaming-analysis-lab1/internal/output"
)

var (
	configPath = flag.String("config", "", "Path to configuration file (required)")
)

func main() {
	flag.Parse()

	if *configPath == "" {
		log.Fatal("Error: --config flag is required. Please specify path to configuration file.")
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	logGen := generator.NewLogGenerator(cfg)

	var writer output.Writer
	if len(cfg.Generator.Output.Kafka.Brokers) > 0 && cfg.Generator.Output.Kafka.Topic != "" {
		writer, err = output.NewKafkaOutput(&cfg.Generator.Output.Kafka)
		if err != nil {
			log.Fatalf("Failed to create Kafka output: %v", err)
		}
		log.Println("Using Kafka output")
	} else {
		writer = output.NewStdoutOutput(cfg.Generator.OutputFormat)
		log.Println("Using stdout output")
	}
	defer writer.Close()

	reloadCh := make(chan *config.Config, 1)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)

	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-sigCh:
				log.Println("Received SIGHUP, reloading config...")
				if err := cfg.ReloadConfig(*configPath); err != nil {
					log.Printf("Failed to reload config: %v", err)
				} else {
					log.Println("Config reloaded successfully")
					reloadCh <- cfg
				}
			case <-stopCh:
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second / time.Duration(cfg.Generator.Rate))
		defer ticker.Stop()

		currentGen := logGen

		for {
			select {
			case newCfg := <-reloadCh:
				log.Println("Updating generator with new config...")
				currentGen = generator.NewLogGenerator(newCfg)
				ticker.Reset(time.Second / time.Duration(newCfg.Generator.Rate))

			case <-ticker.C:
				entry, err := currentGen.GenerateLog()
				if err != nil {
					log.Printf("Failed to generate log: %v", err)
					continue
				}

				if err := writer.Write(entry); err != nil {
					log.Printf("Failed to write log: %v", err)
				}

			case <-stopCh:
				return
			}
		}
	}()

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Interrupt, syscall.SIGTERM)

	<-quitCh
	log.Println("Shutting down...")

	close(stopCh)
	wg.Wait()

	log.Println("Generator stopped")
}
