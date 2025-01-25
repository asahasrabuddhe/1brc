package onebrc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)

const BufferSize = 16 * 1024 * 1024

type V3 struct{}

func (_ V3) Process(in *os.File, out io.Writer) error {
	results := make(map[string]DataV1)
	stations := make([]string, 0)
	buf := make([]byte, BufferSize)
	readStart := 0
	for {
		n, readErr := in.Read(buf[readStart:])
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			panic(readErr)
		}
		if readStart+n == 0 {
			break
		}
		chunk := buf[:readStart+n]
		newLine := bytes.LastIndexByte(chunk, '\n')
		if newLine < 0 {
			break
		}

		remaining := chunk[newLine+1:]
		chunk = chunk[:newLine+1]

		start := 0
		for i := 0; i < len(chunk); i++ {
			if chunk[i] != '\n' {
				continue
			}
			line := chunk[start:i]
			sepIndex := lastByteIndex(line, ';')
			name := string(bytes.TrimSpace(line[:sepIndex]))
			temp, err := strconv.ParseFloat(string(bytes.TrimSpace(line[sepIndex+1:])), 64)
			if err != nil {
				return err
			}
			if data, ok := results[name]; !ok {
				results[name] = DataV1{
					Min:   temp,
					Max:   temp,
					Total: temp,
					Count: 1,
				}
				stations = append(stations, name)
			} else {
				data.Min = min(temp, data.Min)
				data.Max = max(temp, data.Max)
				data.Total += temp
				data.Count++

				results[name] = data
			}
			start = i + 1
		}
		readStart = copy(buf, remaining)
	}
	sort.Strings(stations)
	fmt.Fprint(out, "{")
	for i, name := range stations {
		if i > 0 {
			fmt.Fprint(out, ",")
		}
		data := results[name]
		mean := data.Total / data.Count
		fmt.Fprintf(out, "%s=%.2f/%.2f/%.2f", name, data.Min, mean, data.Max)
	}
	fmt.Fprint(out, "}\n")
	return nil
}
