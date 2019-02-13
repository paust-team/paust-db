package log

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-kit/kit/log/term"
	"io"
)

type Logger interface {
	Debug(msg string, keyvals ...interface{})
	Info(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})

	With(keyvals ...interface{}) Logger
}

const (
	msgKey = "_msg" // "_" prefixed to avoid collisions
)

type pdbLogger struct {
	srcLogger log.Logger
}

// Interface assertions
var _ Logger = (*pdbLogger)(nil)

// NewPDBLogger returns a logger that encodes msg and keyvals to the Writer
// using go-kit's log as an underlying logger and our custom formatter. Note
// that underlying logger could be swapped with something else.
func NewPDBLogger(w io.Writer) Logger {
	// Color by level value
	colorFn := func(keyvals ...interface{}) term.FgBgColor {
		if keyvals[0] != level.Key() {
			panic(fmt.Sprintf("expected level key to be first, got %v", keyvals[0]))
		}
		switch keyvals[1].(level.Value).String() {
		case "debug":
			return term.FgBgColor{Fg: term.DarkGray}
		case "error":
			return term.FgBgColor{Fg: term.Red}
		default:
			return term.FgBgColor{}
		}
	}

	return &pdbLogger{term.NewLogger(w, NewPDBFmtLogger, colorFn)}
}

// Info logs a message at level Info.
func (l *pdbLogger) Info(msg string, keyvals ...interface{}) {
	lWithLevel := level.Info(l.srcLogger)
	if err := log.With(lWithLevel, msgKey, msg).Log(keyvals...); err != nil {
		errLogger := level.Error(l.srcLogger)
		log.With(errLogger, msgKey, msg).Log("err", err)
	}
}

// Debug logs a message at level Debug.
func (l *pdbLogger) Debug(msg string, keyvals ...interface{}) {
	lWithLevel := level.Debug(l.srcLogger)
	if err := log.With(lWithLevel, msgKey, msg).Log(keyvals...); err != nil {
		errLogger := level.Error(l.srcLogger)
		log.With(errLogger, msgKey, msg).Log("err", err)
	}
}

// Error logs a message at level Error.
func (l *pdbLogger) Error(msg string, keyvals ...interface{}) {
	lWithLevel := level.Error(l.srcLogger)
	lWithMsg := log.With(lWithLevel, msgKey, msg)
	if err := lWithMsg.Log(keyvals...); err != nil {
		lWithMsg.Log("err", err)
	}
}

// With returns a new contextual logger with keyvals prepended to those passed
// to calls to Info, Debug or Error.
func (l *pdbLogger) With(keyvals ...interface{}) Logger {
	return &pdbLogger{log.With(l.srcLogger, keyvals...)}
}
