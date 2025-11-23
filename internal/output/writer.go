package output

import "github.com/romeros69/data-streaming-analysis-lab1/internal/generator"

type Writer interface {
	Write(entry *generator.LogEntry) error
	Close() error
}
