package onebrc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
)

func BenchmarkProcessV8(b *testing.B) {
	file, err := os.Open("./testdata/measurements.txt")
	if err != nil {
		b.Error(err)
	}

	defer file.Close()

	var out bytes.Buffer

	for i := 0; i < b.N; i++ {
		p := V8{}
		out = bytes.Buffer{}
		err = p.Process(file, &out)
		if err != nil {
			b.Error(err)
		}
	}

	_, _ = fmt.Fprintf(io.Discard, "%s", out.String())
}

func Test_splitChunksV8(t *testing.T) {
	type args struct {
		filePath  string
		numChunks int
	}
	tests := []struct {
		name    string
		args    args
		want    []metadata
		wantErr bool
	}{
		{
			name: "test chunks",
			args: args{
				filePath:  "./testdata/measurements-10k.txt",
				numChunks: 10,
			},
			want: []metadata{
				{offset: 0, size: 13821},
				{offset: 13821, size: 13809},
				{offset: 27630, size: 13826},
				{offset: 41456, size: 13816},
				{offset: 55272, size: 13820},
				{offset: 69092, size: 13829},
				{offset: 82921, size: 13820},
				{offset: 96741, size: 13823},
				{offset: 110564, size: 13826},
				{offset: 124390, size: 13905},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitChunksV8(tt.args.filePath, tt.args.numChunks)
			if (err != nil) != tt.wantErr {
				t.Errorf("splitChunksV8() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitChunksV8() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_splitChunksV8(b *testing.B) {
	file, err := os.Open("./testdata/measurements.txt")
	if err != nil {
		b.Error(err)
	}

	defer file.Close()

	var chunks []metadata

	for i := 0; i < b.N; i++ {
		chunks, err = splitChunksV8("./testdata/measurements-10k.txt", 10)
		if err != nil {
			b.Error(err)
		}
	}

	_, _ = fmt.Fprintf(io.Discard, "%v2w", chunks)
}
