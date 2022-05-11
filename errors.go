package kafka

import (
	"fmt"
)

func ReportError(err error, msg string) {
	// TODO: refactor #49
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
	}
}
