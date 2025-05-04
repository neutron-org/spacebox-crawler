package utils

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

func NewModuleLogger(name string) *zerolog.Logger {
	logger := zerolog.
		New(os.Stderr).
		Output(zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.DateTime,
		}).
		With().Timestamp().
		Str("module", name).
		Logger()

	return &logger
}
