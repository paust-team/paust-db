package log_test

import (
	"bytes"
	"errors"
	kitlog "github.com/go-kit/kit/log"
	"github.com/paust-team/paust-db/libs/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"math"
	"regexp"
	"testing"
)

func TestPDBFmtLogger(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	logger := log.NewPDBFmtLogger(buf)
	assert := assert.New(t)
	require := require.New(t)

	err := logger.Log("hello", "world")
	require.Nil(err, "Log error: %+v", err)
	assert.Regexp(regexp.MustCompile(`NONE \[.+\] unknown \s+ hello=world\n$`), buf.String())

	buf.Reset()
	err = logger.Log("a", 1, "err", errors.New("error"))
	require.Nil(err, "Log error: %+v", err)
	assert.Regexp(regexp.MustCompile(`NONE \[.+\] unknown \s+ a=1 err=error\n$`), buf.String())

	buf.Reset()
	err = logger.Log("std_map", map[int]int{1: 2}, "my_map", mymap{0: 0})
	require.Nil(err, "Log error: %+v", err)
	assert.Regexp(regexp.MustCompile(`NONE \[.+\] unknown \s+ std_map=map\[1:2\] my_map=special_behavior\n$`), buf.String())

	buf.Reset()
	err = logger.Log("level", "error")
	require.Nil(err, "Log error: %+v", err)
	assert.Regexp(regexp.MustCompile(`ERROR\[.+\] unknown \s+\n$`), buf.String())

	buf.Reset()
	err = logger.Log("_msg", "Hello")
	require.Nil(err, "Log error: %+v", err)
	assert.Regexp(regexp.MustCompile(`NONE \[.+\] Hello \s+\n$`), buf.String())

	buf.Reset()
}

func BenchmarkPDBFmtLoggerSimple(b *testing.B) {
	benchmarkRunnerKitlog(b, log.NewPDBFmtLogger(ioutil.Discard), baseMessage)
}

func BenchmarkPDBFmtLoggerContextual(b *testing.B) {
	benchmarkRunnerKitlog(b, log.NewPDBFmtLogger(ioutil.Discard), withMessage)
}

func TestPDBFmtLoggerConcurrency(t *testing.T) {
	t.Parallel()
	testConcurrency(t, log.NewPDBFmtLogger(ioutil.Discard), 10000)
}

func benchmarkRunnerKitlog(b *testing.B, logger kitlog.Logger, f func(kitlog.Logger)) {
	lc := kitlog.With(logger, "common_key", "common_value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f(lc)
	}
}

var (
	baseMessage = func(logger kitlog.Logger) { logger.Log("foo_key", "foo_value") }
	withMessage = func(logger kitlog.Logger) { kitlog.With(logger, "a", "b").Log("d", "f") }
)

// These test are designed to be run with the race detector.

func testConcurrency(t *testing.T, logger kitlog.Logger, total int) {
	n := int(math.Sqrt(float64(total)))
	share := total / n

	errC := make(chan error, n)

	for i := 0; i < n; i++ {
		go func() {
			errC <- spam(logger, share)
		}()
	}

	for i := 0; i < n; i++ {
		err := <-errC
		require.Nil(t, err, "concurrent loggin error: %v", err)
	}
}

func spam(logger kitlog.Logger, count int) error {
	for i := 0; i < count; i++ {
		err := logger.Log("key", i)
		if err != nil {
			return err
		}
	}
	return nil
}

type mymap map[int]int

func (m mymap) String() string { return "special_behavior" }
