package onebrc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func BenchmarkProcessV3(b *testing.B) {
	file, err := os.Open("./testdata/measurements.txt")
	if err != nil {
		b.Error(err)
	}

	defer file.Close()

	var out bytes.Buffer

	for i := 0; i < b.N; i++ {
		p := V3{}
		out = bytes.Buffer{}
		err = p.Process(file, &out)
		if err != nil {
			b.Error(err)
		}
	}

	_, _ = fmt.Fprintf(io.Discard, "%s", out.String())
}
