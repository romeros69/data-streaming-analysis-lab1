package output

import (
	"fmt"
	"os"

	"github.com/romeros69/data-streaming-analysis-lab1/internal/generator"
)

type StdoutOutput struct {
	format string
}

func NewStdoutOutput(format string) *StdoutOutput {
	return &StdoutOutput{format: format}
}

func (o *StdoutOutput) Write(entry *generator.LogEntry) error {
	var output string
	var err error

	if o.format == "json" {
		output, err = entry.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to marshal log: %w", err)
		}
	} else {
		output = entry.ToText()
	}

	fmt.Fprintln(os.Stdout, output)
	return nil
}

func (o *StdoutOutput) Close() error {
	return nil
}
