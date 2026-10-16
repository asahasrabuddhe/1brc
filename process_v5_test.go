package onebrc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func BenchmarkProcessV5(b *testing.B) {
	file, err := os.Open("./testdata/measurements.txt")
	if err != nil {
		b.Error(err)
	}

	defer file.Close()

	var out bytes.Buffer

	for b.Loop() {
		p := V5{}
		out = bytes.Buffer{}
		err = p.Process(file, &out)
		if err != nil {
			b.Error(err)
		}
	}

	_, _ = fmt.Fprintf(io.Discard, "%s", out.String())
}
