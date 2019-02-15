package log_test

import (
	"bytes"
	"github.com/paust-team/paust-db/libs/log"
	"github.com/stretchr/testify/assert"
	"regexp"
	"strings"
	"testing"
)

func TestVariousLevels(t *testing.T) {
	testCases := []struct {
		name    string
		allowed log.Option
		want    string
	}{
		{
			"AllowAll",
			log.AllowAll(),
			strings.Join([]string{
				`DEBUG\[.+\] here \s+ log_level="debug log"\n`,
				`INFO \[.+\] here \s+ log_level="info log"\n`,
				`ERROR\[.+\] here \s+ log_level="error log"\n$`,
			}, ""),
		},
		{
			"AllowDebug",
			log.AllowDebug(),
			strings.Join([]string{
				`DEBUG\[.+\] here \s+ log_level="debug log"\n`,
				`INFO \[.+\] here \s+ log_level="info log"\n`,
				`ERROR\[.+\] here \s+ log_level="error log"\n$`,
			}, ""),
		},
		{
			"AllowInfo",
			log.AllowInfo(),
			strings.Join([]string{
				`INFO \[.+\] here \s+ log_level="info log"\n`,
				`ERROR\[.+\] here \s+ log_level="error log"\n$`,
			}, ""),
		},
		{
			"AllowError",
			log.AllowError(),
			strings.Join([]string{
				`ERROR\[.+\] here \s+ log_level="error log"\n$`,
			}, ""),
		},
		{
			"AllowNone",
			log.AllowNone(),
			``,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := log.NewFilter(log.NewPDBLogger(&buf), tc.allowed)

			logger.Debug("here", "log_level", "debug log")
			logger.Info("here", "log_level", "info log")
			logger.Error("here", "log_level", "error log")

			want, have := tc.want, buf.String()
			assert.Regexp(t, regexp.MustCompile(want), have)
		})
	}
}

func TestLevelContext(t *testing.T) {
	var buf bytes.Buffer

	logger := log.NewPDBLogger(&buf)
	logger = log.NewFilter(logger, log.AllowError())
	logger = logger.With("context", "value")

	logger.Error("foo", "bar", "baz")
	want, have := `ERROR\[.+\] foo \s+ context=value bar=baz\n$`, buf.String()
	assert.Regexp(t, regexp.MustCompile(want), have)

	buf.Reset()
	logger.Info("foo", "bar", "baz")
	assert.EqualValues(t, ``, buf.String())
}
