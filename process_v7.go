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

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

type V7 struct{}

func (_ V7) Process(in *os.File, out io.Writer) error {
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
			if i.value == nil {
				continue
			}
			results.insertItem(i.key, uint64(h), i.value.Min, i.value.Max, i.value.Total, i.value.Count)
		}
	}

	// sort the results by station name
	sort.Sort(results.items)

	_, _ = fmt.Fprint(out, "{")
	idx := 0
	for _, itm := range results.getItems() {
		if itm.value == nil {
			continue
		}
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

func processChunkV7(filePath string, md metadata, resultsCh chan *hashBucket, wg *sync.WaitGroup) {
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
		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			break
		}

		remaining := chunk[newline+1:]
		chunk = chunk[:newline+1]

		for {
			var stationName, temp []byte
			hash := uint64(offset64)
			for i := 0; i < len(chunk); i++ {
				c := chunk[i]
				if c == ';' {
					stationName, temp = chunk[:i], chunk[i+1:]
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
			results.insertItem(stationName, hash, tempInt, tempInt, tempInt, 1)
		}
		readStart = copy(buf, remaining)
	}
	resultsCh <- results
}
