package log

import (
	"fmt"
	kitlog "github.com/go-kit/kit/log"
	kitlevel "github.com/go-kit/kit/log/level"
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
	srcLogger kitlog.Logger
}

// Interface assertions
var _ Logger = (*pdbLogger)(nil)

// NewPDBLogger returns a logger that encodes msg and keyvals to the Writer
// using go-kit's log as an underlying logger and our custom formatter. Note
// that underlying logger could be swapped with something else.
func NewPDBLogger(w io.Writer) Logger {
	// Color by level value
	colorFn := func(keyvals ...interface{}) term.FgBgColor {
		if keyvals[0] != kitlevel.Key() {
			panic(fmt.Sprintf("expected level key to be first, got %v", keyvals[0]))
		}
		switch keyvals[1].(kitlevel.Value).String() {
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
	lWithLevel := kitlevel.Info(l.srcLogger)
	if err := kitlog.With(lWithLevel, msgKey, msg).Log(keyvals...); err != nil {
		errLogger := kitlevel.Error(l.srcLogger)
		kitlog.With(errLogger, msgKey, msg).Log("err", err)
	}
}

// Debug logs a message at level Debug.
func (l *pdbLogger) Debug(msg string, keyvals ...interface{}) {
	lWithLevel := kitlevel.Debug(l.srcLogger)
	if err := kitlog.With(lWithLevel, msgKey, msg).Log(keyvals...); err != nil {
		errLogger := kitlevel.Error(l.srcLogger)
		kitlog.With(errLogger, msgKey, msg).Log("err", err)
	}
}

// Error logs a message at level Error.
func (l *pdbLogger) Error(msg string, keyvals ...interface{}) {
	lWithLevel := kitlevel.Error(l.srcLogger)
	lWithMsg := kitlog.With(lWithLevel, msgKey, msg)
	if err := lWithMsg.Log(keyvals...); err != nil {
		lWithMsg.Log("err", err)
	}
}

// With returns a new contextual logger with keyvals prepended to those passed
// to calls to Info, Debug or Error.
func (l *pdbLogger) With(keyvals ...interface{}) Logger {
	return &pdbLogger{kitlog.With(l.srcLogger, keyvals...)}
}

// NewSyncWriter returns a new writer that is safe for concurrent use by
// multiple goroutines. Writes to the returned writer are passed on to w. If
// another write is already in progress, the calling goroutine blocks until
// the writer is available.
//
// If w implements the following interface, so does the returned writer.
//
//    interface {
//        Fd() uintptr
//    }
func NewSyncWriter(w io.Writer) io.Writer {
	return kitlog.NewSyncWriter(w)
}
