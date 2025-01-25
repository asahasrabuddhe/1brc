package onebrc

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

type V2 struct{}

func (_ V2) Process(in *os.File, out io.Writer) error {
	results := make(map[string]DataV1)
	stations := make([]string, 0)
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := scanner.Text()
		sepIndex := lastByteIndex([]byte(line), ';')
		name := strings.TrimSpace(line[:sepIndex])
		temp, err := strconv.ParseFloat(strings.TrimSpace(line[sepIndex+1:]), 64)
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

func lastByteIndex(in []byte, n byte) int {
	for i := len(in) - 1; i >= 0; i-- {
		if in[i] == n {
			return i
		}
	}
	return -1
}
