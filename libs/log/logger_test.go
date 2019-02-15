package log_test

import (
	"bytes"
	"github.com/go-logfmt/logfmt"
	"github.com/paust-team/paust-db/libs/log"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

func TestLoggerLogsItsErrors(t *testing.T) {
	var buf bytes.Buffer

	logger := log.NewPDBLogger(&buf)
	logger.Info("foo", "", "bar")
	msg := strings.TrimSpace(buf.String())
	assert.Contains(t, msg, logfmt.ErrInvalidKey.Error())
}

func BenchmarkPDBLoggerSimple(b *testing.B) {
	benchmarkRunner(b, log.NewPDBLogger(ioutil.Discard), baseInfoMessage)
}

func BenchmarkPDBLoggerContextual(b *testing.B) {
	benchmarkRunner(b, log.NewPDBLogger(ioutil.Discard), withInfoMessage)
}

func benchmarkRunner(b *testing.B, logger log.Logger, f func(log.Logger)) {
	lc := logger.With("common_key", "common_value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f(lc)
	}
}

var (
	baseInfoMessage = func(logger log.Logger) { logger.Info("foo_message", "foo_key", "foo_value") }
	withInfoMessage = func(logger log.Logger) { logger.With("a", "b").Info("c", "d", "f") }
)
