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
		go processChunkV7(in.Name(), chunk, resultsCh, &chunkWg)
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
		//_, err = f.Seek(seekOffset, io.SeekStart)
		//if err != nil {
		//	return nil, err
		//}

		// Read up to `maxLineLength` bytes
		n, err := f.ReadAt(buf, seekOffset)
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
