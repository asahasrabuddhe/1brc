package onebrc

import (
	"fmt"
	"io"
	"runtime"
	"slices"
	"sync"
	"unsafe"
)

type Station struct {
	Min   int64
	Max   int64
	Total int64
	Count int64
}

const BufferSize = 16 * 1024 * 1024

// Process reads the input data and returns the result
func Process(r io.Reader, w io.Writer) error {
	//f, _ := os.Create("cpu.pprof")
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()

	workers := runtime.NumCPU()
	resultsCh := make(chan map[string]*Station)

	var workerWaitGroup sync.WaitGroup
	workerWaitGroup.Add(workers)

	linesCh := reader(r)

	for i := 0; i < workers; i++ {
		go func() {
			processLines(linesCh, resultsCh)
			workerWaitGroup.Done()
		}()
	}

	go func() {
		workerWaitGroup.Wait()
		close(resultsCh)
	}()

	printResult(w, resultsCh)

	return nil
}

func reader(r io.Reader) <-chan []byte {
	out, buf, leftover := make(chan []byte), make([]byte, BufferSize), make([]byte, 0)
	go func() {
		for {
			n, err := r.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}

			lines := append(leftover, buf[:n]...)

			lastNewLineIndex := -1
			for i := len(lines) - 1; i >= 0; i-- {
				if lines[i] == '\n' {
					lastNewLineIndex = i
					break
				}
			}
			if lastNewLineIndex == -1 {
				leftover = append(leftover[:0], lines...)
				continue
			}

			out <- lines[:lastNewLineIndex+1]

			leftover = append(leftover[:0], lines[lastNewLineIndex+1:]...)
		}

		if len(leftover) > 0 {
			out <- leftover
		}

		close(out)
	}()
	return out
}

func processLines(linesChan <-chan []byte, resultsChan chan<- map[string]*Station) {
	for lines := range linesChan {
		localResults := make(map[string]*Station, 512)
		var lastIndex int
		for index, c := range lines {
			if c == '\n' {
				stationName, temperature := parseLine(lines[lastIndex:index])
				k := keyStr(stationName)

				if station, ok := localResults[k]; !ok {
					localResults[k] = &Station{Min: temperature, Max: temperature, Total: temperature, Count: 1}
				} else {
					station.Min = min(station.Min, temperature)
					station.Max = max(station.Max, temperature)
					station.Total += temperature
					station.Count++
				}

				lastIndex = index + 1
			}
		}
		resultsChan <- localResults
	}
}

func parseLine(line []byte) ([]byte, int64) {
	sepIndex := byteIndex(line, ';')
	name, tempStr := line[:sepIndex], line[sepIndex+1:]

	var (
		isNeg       bool
		temperature int64
	)
	decimalIndex := byteIndex(tempStr, '.')
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
		return name, -temperature
	}

	return name, temperature
}

func printResult(w io.Writer, resultsCh <-chan map[string]*Station) {
	results := make(map[string]*Station, 512)
	stations := make([]string, 0, 512)
	for result := range resultsCh {
		for k, data := range result {
			if station, ok := results[k]; !ok {
				results[k] = &Station{Min: data.Min, Max: data.Max, Total: data.Total, Count: 1}
				stations = append(stations, k)
			} else {
				station.Min = min(station.Min, data.Min)
				station.Max = max(station.Max, data.Max)
				station.Total += data.Total
				station.Count += data.Count
			}
		}
	}

	slices.Sort(stations)

	_, _ = fmt.Fprint(w, "{")
	for i, station := range stations {
		value := results[station]
		_, _ = fmt.Fprintf(w, "%s=%.1f/%.1f/%.1f", station, float64(value.Min)/10.0, float64(value.Total/(value.Count))/10.0, float64(value.Max)/10.0)
		if i < len(stations)-1 {
			_, _ = fmt.Fprint(w, ", ")
		}
	}
	_, _ = fmt.Fprint(w, "}\n")
}

func byteIndex(src []byte, sep byte) int {
	for i := len(src) - 1; i >= 0; i-- {
		if src[i] == sep {
			return i
		}
	}
	return -1
}

func keyStr(in []byte) string {
	return *(*string)(unsafe.Pointer(&in))
}
