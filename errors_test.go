package kafka

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReportError(t *testing.T) {
	// backup of the real stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	defer w.Close()
	defer r.Close()
	os.Stdout = w

	// test
	err := errors.New("test")
	ReportError(err, "test")

	// length of the string without	the newline character
	var buf []byte = make([]byte, 10)
	// read the output of fmt.Printf to os.Stdout from the pipe
	r.Read(buf)
	// append newline
	buf = append(buf, "\n"...)
	assert.Equal(t, "test: test\n", string(buf))

	// restore the real stdout
	os.Stdout = oldStdout
}
