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
	// Open the file
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Get the file size
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()

	// Calculate the ideal chunk size
	chunkSize := size / int64(numChunks)
	if chunkSize == 0 {
		return nil, fmt.Errorf("chunk size too small for the number of chunks")
	}

	// Slice to store chunk metadata
	chunks := make([]metadata, 0, numChunks)
	offset := int64(0)
	buf := make([]byte, maxLineLength)

	for i := 0; i < numChunks; i++ {
		if offset >= size {
			break
		}

		// Calculate the target end of the chunk
		end := offset + chunkSize
		if i == numChunks-1 || end >= size {
			// Handle the last chunk
			chunks = append(chunks, metadata{offset, size - offset})
			break
		}

		// Seek to the target offset, minus a buffer to find the nearest newline
		seekOffset := max(end-maxLineLength, 0)
		_, err = f.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}

		// Read up to `maxLineLength` bytes
		n, err := f.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		// Find the nearest newline
		newline := lastByteIndex(buf[:n], '\n')
		if newline < 0 {
			return nil, fmt.Errorf("newline not found in buffer near offset %d", seekOffset)
		}

		// Adjust the chunk boundary to the newline
		nextOffset := seekOffset + int64(newline) + 1
		chunks = append(chunks, metadata{offset, nextOffset - offset})

		// Update the offset for the next chunk
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
