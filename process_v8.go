package onebrc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
)

type V8 struct{}

func (_ V8) Process(in *os.File, out io.Writer) error {
	results := newBucket()
	resultsCh := make(chan *hashBucket)

	chunks, err := splitChunks(in.Name(), runtime.NumCPU())
	if err != nil {
		return err
	}
	// chunkWg is used to wait for all the goroutines to finish
	var chunkWg sync.WaitGroup
	chunkWg.Add(len(chunks))

	// process each chunk in a separate goroutine
	for _, chunk := range chunks {
		go processChunkV8(in.Name(), chunk, resultsCh, &chunkWg)
	}

	// wait for all the goroutines to finish
	go func() {
		chunkWg.Wait()
		close(resultsCh)
	}()

	// merge the results from the goroutines
	for result := range resultsCh {
		for h, i := range result.getItems() {
			results.insertItem(i.key, uint64(h), i.value.Min, i.value.Max, i.value.Total, i.value.Count)
		}
	}

	// sort the results by station name
	sort.Sort(results.items)

	_, _ = fmt.Fprint(out, "{")
	idx := 0
	for _, itm := range results.getItems() {
		if idx > 0 {
			_, _ = fmt.Fprint(out, ",")
		}
		mean := float64(itm.value.Total) / float64(itm.value.Count) / 10
		_, _ = fmt.Fprintf(out, "%s=%.1f/%.1f/%.1f", string(itm.key), float64(itm.value.Min)/10, mean, float64(itm.value.Max)/10)
		idx++
	}
	_, _ = fmt.Fprint(out, "}\n")
	return nil
}

func splitChunksV8(filePath string, numChunks int) ([]metadata, error) {
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

	for i := range numChunks {
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

		// Read up to `maxLineLength` bytes
		n, readErr := f.ReadAt(buf, seekOffset)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, readErr
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

func processChunkV8(filePath string, md metadata, resultsCh chan *hashBucket, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := io.NewSectionReader(file, md.offset, md.size)
	results := newBucket()

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
		newline := lastByteIndex(chunk, '\n')
		if newline < 0 {
			break
		}

		remaining := chunk[newline+1:]
		chunk = chunk[:newline+1]

		for {
			var stationName []byte
			hash := uint64(offset64)
			for i := range len(chunk) {
				c := chunk[i]
				if c == ';' {
					stationName, chunk = chunk[:i], chunk[i+1:]
					break
				}
				hash *= prime64
				hash ^= uint64(c) // FNV-1 is * then XOR
			}
			if len(chunk) == 0 {
				break
			}
			// Parse temperature.
			var (
				isNeg bool
				temp  int64
				index int
			)
			if chunk[0] == '-' {
				isNeg = true
				index++
			}
			for ; index < len(chunk); index++ {
				if chunk[index] == '\n' {
					index++
					chunk = chunk[index:]
					break
				}
				if chunk[index] == '.' {
					index++
				}
				temp = temp*10 + int64(chunk[index]-'0')
			}
			if isNeg {
				temp = -temp
			}
			results.insertItem(stationName, hash, temp, temp, temp, 1)
		}
		readStart = copy(buf, remaining)
	}
	resultsCh <- results
}
