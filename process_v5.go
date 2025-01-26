package onebrc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
)

type V5 struct{}

func (_ V5) Process(in *os.File, out io.Writer) error {
	results := make(map[string]DataV4)
	resultsCh := make(chan map[string]DataV4)
	stations := make([]string, 0)
	chunks, err := splitChunks(in.Name(), runtime.NumCPU())
	if err != nil {
		return err
	}
	// chunkWg is used to wait for all the goroutines to finish
	var chunkWg sync.WaitGroup
	chunkWg.Add(len(chunks))

	// process each chunk in a separate goroutine
	for _, chunk := range chunks {
		go processChunkV5(in.Name(), chunk, resultsCh, &chunkWg)
	}

	// wait for all the goroutines to finish
	go func() {
		chunkWg.Wait()
		close(resultsCh)
	}()

	// merge the results from the goroutines
	for result := range resultsCh {
		for name, value := range result {
			if station, ok := results[name]; !ok {
				results[name] = DataV4{
					Min:   value.Min,
					Max:   value.Max,
					Total: value.Total,
					Count: value.Count,
				}
				stations = append(stations, name)
			} else {
				station.Min = min(value.Min, station.Min)
				station.Max = max(value.Max, station.Max)
				station.Total += value.Total
				station.Count += value.Count

				results[name] = station
			}
		}
	}

	sort.Strings(stations)
	fmt.Fprint(out, "{")
	for i, name := range stations {
		if i > 0 {
			fmt.Fprint(out, ",")
		}
		data := results[name]
		mean := float64(data.Total) / float64(data.Count) / 10
		fmt.Fprintf(out, "%s=%.1f/%.1f/%.1f", name, float64(data.Min)/10, mean, float64(data.Max)/10)
	}
	fmt.Fprint(out, "}\n")
	return nil
}

// read chunks implementation
const maxLineLength = 100

// metadata represents the chunk offset and size
type metadata struct {
	offset int64
	size   int64
}

func splitChunks(filePath string, numChunks int) ([]metadata, error) {
	// open the file
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// get the file size
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()

	// calculate the chunk size
	chunkSize := size / int64(numChunks)

	// buffer to hold the data near the boundary of each chunk
	buf := make([]byte, maxLineLength)
	// slice to store the chunk info
	chunks := make([]metadata, 0, numChunks)
	// offset tracks the start of the current chunk
	offset := int64(0)

	for i := 0; i < numChunks; i++ {
		// handle the last chunk
		if i == numChunks-1 {
			if offset < size {
				chunks = append(chunks, metadata{offset, size - offset})
			}
			break
		}

		// seek near the end of the current chunk
		seekOffset := max(offset+chunkSize-maxLineLength, 0)
		_, err = f.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}

		// read the data near the boundary of the chunk
		n, err := io.ReadFull(f, buf)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
		chunk := buf[:n]

		// find the newline in the chunk
		newline := lastByteIndex(chunk, '\n')
		if newline < 0 {
			return nil, fmt.Errorf("newline not found in the chunk")
		}

		// calculate the next offset
		nextOffset := seekOffset + int64(newline) + 1
		// add the chunk info to the slice
		chunks = append(chunks, metadata{offset, nextOffset - offset})
		// update the offset
		offset = nextOffset
	}

	return chunks, nil
}

func processChunkV5(filePath string, md metadata, resultsCh chan map[string]DataV4, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Seek(md.offset, io.SeekStart)
	if err != nil {
		panic(err)
	}

	reader := io.LimitReader(file, md.size)
	results := make(map[string]DataV4)

	buf := make([]byte, BufferSize)
	readStart := 0
	for {
		n, readErr := reader.Read(buf[readStart:])
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			panic(readErr)
		}
		if readStart+n == 0 {
			break
		}

		chunk := buf[:readStart+n]
		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			break
		}

		remaining := chunk[newline+1:]
		chunk = chunk[:newline+1]

		for {
			var stationName, temp []byte
			for i := 0; i < len(chunk); i++ {
				c := chunk[i]
				if c == ';' {
					stationName, temp = chunk[:i], chunk[i+1:]
					break
				}
			}
			if len(chunk) == 0 {
				break
			}
			// Parse temperature.
			var (
				isNeg   bool
				tempInt int64
			)
			for index := 0; index < len(temp); index++ {
				if temp[index] == '\n' {
					index++
					chunk = temp[index:]
					break
				}
				if temp[index] == '-' {
					isNeg = true
					continue
				}
				if temp[index] == '.' {
					continue
				}
				tempInt = tempInt*10 + int64(temp[index]-'0')
			}
			if isNeg {
				tempInt = -tempInt
			}
			if station, ok := results[string(stationName)]; !ok {
				results[string(stationName)] = DataV4{
					Min:   tempInt,
					Max:   tempInt,
					Total: tempInt,
					Count: 1,
				}
			} else {
				station.Min = min(tempInt, station.Min)
				station.Max = max(tempInt, station.Max)
				station.Total += tempInt
				station.Count++

				results[string(stationName)] = station
			}
		}
		readStart = copy(buf, remaining)
	}
	resultsCh <- results
}