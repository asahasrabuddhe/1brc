package onebrc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

type DataV4 struct {
	Min   int64
	Max   int64
	Total int64
	Count int64
}

type V4 struct{}

func (_ V4) Process(in *os.File, out io.Writer) error {
	results := make(map[string]DataV4)
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
			name, tempBytes := string(line[:sepIndex]), line[sepIndex+1:]
			var temp int64
			var isNeg bool
			for idx := 0; idx < len(tempBytes); idx++ {
				if tempBytes[idx] == '\n' {
					break
				}
				if tempBytes[idx] == '-' {
					isNeg = true
					continue
				}
				if tempBytes[idx] == '.' {
					continue
				}
				temp = temp*10 + int64(tempBytes[idx]-'0')
			}
			if isNeg {
				temp = -temp
			}
			if data, ok := results[name]; !ok {
				results[name] = DataV4{
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
		mean := float64(data.Total) / float64(data.Count) / 10
		fmt.Fprintf(out, "%s=%.2f/%.2f/%.2f", name, float64(data.Min)/10, mean, float64(data.Max)/10)
	}
	fmt.Fprint(out, "}\n")
	return nil
}
