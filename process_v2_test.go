package onebrc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func BenchmarkProcessV2(b *testing.B) {
	file, err := os.Open("./testdata/measurements.txt")
	if err != nil {
		b.Fatal(err)
	}

	defer file.Close()

	var out bytes.Buffer

	for b.Loop() {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		p := V2{}
		out = bytes.Buffer{}
		err = p.Process(file, &out)
		if err != nil {
			b.Error(err)
		}
	}

	_, _ = fmt.Fprintf(io.Discard, "%s", out.String())
}
