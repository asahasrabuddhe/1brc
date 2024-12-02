package onebrc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/dolthub/swiss"
)

const BufferSize = 32 * 1024 * 1024

type Data struct {
	Name  []byte
	Min   int64
	Max   int64
	Total int64
	Count int64
}

var (
	results = swiss.NewMap[uint64, Data](0)
	workers = runtime.NumCPU()
)

// Process reads the input data and returns the result
func Process(r io.Reader, w io.Writer) error {
	buf, leftover := make([]byte, BufferSize), []byte{}
	linesCh, resultsCh, doneCh := make(chan []byte), make(chan *swiss.Map[uint64, Data], workers), make(chan struct{})

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			processLines(linesCh, resultsCh)
			wg.Done()
		}()
	}

	go func() {
		for result := range resultsCh {
			result.Iter(func(_ uint64, data Data) (stop bool) {
				key := hash(data.Name)
				if station, ok := results.Get(key); !ok {
					results.Put(key, Data{data.Name, data.Min, data.Max, data.Total, 1})
				} else {
					station.Min = min(station.Min, data.Min)
					station.Max = max(station.Max, data.Max)
					station.Total += data.Total
					station.Count += data.Count
					results.Put(key, station)
				}
				return
			})
		}
		doneCh <- struct{}{}
	}()

	for {
		n, err := r.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}
		buf = buf[:n]

		var lastNewLineIndex int
		for i := len(buf) - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				lastNewLineIndex = i
				break
			}
		}

		lines := make([]byte, n+len(leftover))
		lines = append(leftover, buf[:lastNewLineIndex+1]...)
		leftover = make([]byte, len(buf[lastNewLineIndex+1:]))
		copy(leftover, buf[lastNewLineIndex+1:])

		linesCh <- lines
	}

	close(linesCh)

	wg.Wait()

	close(resultsCh)

	<-doneCh

	printResult(w, results)

	return nil
}

func processLines(linesChan chan []byte, resultsChan chan *swiss.Map[uint64, Data]) {
	localResults := swiss.NewMap[uint64, Data](0)
	for lines := range linesChan {
		var lastIndex int
		for index, c := range lines {
			if c != '\n' {
				continue
			}

			name, temperature := processLine(lines[lastIndex:index])
			key := hash(name)

			if station, ok := localResults.Get(key); !ok {
				localResults.Put(key, Data{name, temperature, temperature, temperature, 1})
			} else {
				station.Min = min(station.Min, temperature)
				station.Max = max(station.Max, temperature)
				station.Total += temperature
				station.Count++
				localResults.Put(key, station)
			}

			lastIndex = index + 1
		}
	}
	resultsChan <- localResults
}

func processLine(line []byte) ([]byte, int64) {
	if len(line) == 0 {
		return nil, 0
	}

	sepIndex := byteIndex(line, ';')
	name := line[:sepIndex]
	tempStr := line[sepIndex+1:]

	var isNeg bool
	decimalIndex := byteIndex(tempStr, '.')
	var temperature int64
	for i, c := range tempStr {
		if i == decimalIndex {
			continue
		}
		if c == '-' {
			isNeg = true
			continue
		}
		temperature = temperature*10 + int64(c-'0')
	}
	if isNeg {
		temperature = -temperature
	}

	return name, temperature
}

func printResult(w io.Writer, data *swiss.Map[uint64, Data]) {
	result := make([]Data, 0, data.Count())
	data.Iter(func(_ uint64, v Data) (stop bool) {
		result = append(result, v)
		return
	})

	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i].Name, result[j].Name) == -1
	})

	_, _ = fmt.Fprint(w, "{")
	for i, v := range result {
		_, _ = fmt.Fprintf(w, "%s=%.1f/%.1f/%.1f", v.Name, float64(v.Min)/10.0, float64(v.Total/(v.Count))/10.0, float64(v.Max)/10.0)
		if i < len(result)-1 {
			_, _ = fmt.Fprint(w, ", ")
		}
	}
	_, _ = fmt.Fprint(w, "}\n")
}

func byteIndex(s []byte, sep byte) int {
	for i, c := range s {
		if c == sep {
			return i
		}
	}
	return -1
}

func hash(name []byte) uint64 {
	n := min(len(name), 10) // 10 bytes, one more than we need just to be safe
	var result uint64

	for _, b := range name[:n] {
		v := b - 65
		m := uint64(10 << ((v / 10) * 3)) // Compute scaling factor efficiently
		result = result*m | uint64(b)
	}

	return result
}
