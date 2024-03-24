package onebrc

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

type Data struct {
	Name  string
	Min   float64
	Max   float64
	Total float64
	Count float64
}

var results = map[string]Data{}

// Process reads the input data and returns the result
func Process(r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
		name := parts[0]
		tempStr := strings.Trim(parts[1], "\n")

		temperature, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			return err
		}

		station, ok := results[name]
		if !ok {
			results[name] = Data{name, temperature, temperature, temperature, 1}
		} else {
			if temperature < station.Min {
				station.Min = temperature
			}
			if temperature > station.Max {
				station.Max = temperature
			}
			station.Total += temperature
			station.Count++
		}
	}

	printResult(w, results)

	return nil
}

func printResult(w io.Writer, data map[string]Data) {
	result := make(map[string]Data, len(data))
	keys := make([]string, 0, len(data))
	for _, v := range data {
		keys = append(keys, v.Name)
		result[v.Name] = v
	}
	sort.Strings(keys)

	_, _ = fmt.Fprint(w, "{")
	for i, k := range keys {
		v := result[k]
		_, _ = fmt.Fprintf(w, "%s=%.1f/%.1f/%.1f", k, v.Min, v.Total/float64(v.Count), v.Max)
		if i < len(keys)-1 {
			_, _ = fmt.Fprint(w, ", ")
		}
	}
	_, _ = fmt.Fprint(w, "}\n")
}
