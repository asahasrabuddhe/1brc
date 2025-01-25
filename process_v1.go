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

type DataV1 struct {
	Min   float64
	Max   float64
	Total float64
	Count float64
}

type V1 struct{}

func (p V1) Process(in *os.File, out io.Writer) error {
	results := make(map[string]DataV1)
	stations := make([]string, 0)
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
		if len(parts) != 2 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		temp, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
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
