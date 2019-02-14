package log

import "fmt"

type level byte

const (
	levelDebug level = 1 << iota
	levelInfo
	levelError
)

type filter struct {
	next    Logger
	allowed level // XOR'd levels
}

// NewFilter wraps next and implements filtering. See the commentary on the
// Option functions for a detailed description of how to configure levels. If
// no options are provided, all leveled log events created with Debug, Info or
// Error helper methods are squelched.
func NewFilter(next Logger, options ...Option) Logger {
	l := &filter{
		next: next,
	}
	for _, option := range options {
		option(l)
	}
	return l
}

func (l *filter) Info(msg string, keyvals ...interface{}) {
	levelAllowed := l.allowed&levelInfo != 0
	if !levelAllowed {
		return
	}
	l.next.Info(msg, keyvals...)
}

func (l *filter) Debug(msg string, keyvals ...interface{}) {
	levelAllowed := l.allowed&levelDebug != 0
	if !levelAllowed {
		return
	}
	l.next.Debug(msg, keyvals...)
}

func (l *filter) Error(msg string, keyvals ...interface{}) {
	levelAllowed := l.allowed&levelError != 0
	if !levelAllowed {
		return
	}
	l.next.Error(msg, keyvals...)
}

func (l *filter) With(keyvals ...interface{}) Logger {
	return &filter{
		next:    l.next.With(keyvals...),
		allowed: l.allowed,
	}
}

// Option sets a parameter for the filter.
type Option func(*filter)

// AllowLevel returns an option for the given level or error if no option exist
// for such level.
func AllowLevel(lvl string) (Option, error) {
	switch lvl {
	case "debug":
		return AllowDebug(), nil
	case "info":
		return AllowInfo(), nil
	case "error":
		return AllowError(), nil
	case "none":
		return AllowNone(), nil
	default:
		return nil, fmt.Errorf("Expected either \"info\", \"debug\", \"error\" or \"none\" level, given %s", lvl)
	}
}

// AllowAll is an alias for AllowDebug.
func AllowAll() Option {
	return AllowDebug()
}

// AllowDebug allows error, info and debug level log events to pass.
func AllowDebug() Option {
	return allowed(levelError | levelInfo | levelDebug)
}

// AllowInfo allows error and info level log events to pass.
func AllowInfo() Option {
	return allowed(levelError | levelInfo)
}

// AllowError allows only error level log events to pass.
func AllowError() Option {
	return allowed(levelError)
}

// AllowNone allows no leveled log events to pass.
func AllowNone() Option {
	return allowed(0)
}

func allowed(allowed level) Option {
	return func(l *filter) { l.allowed = allowed }
}
