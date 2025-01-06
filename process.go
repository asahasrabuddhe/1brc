package onebrc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"runtime"
	"sort"
	"sync"
)

const (
	// FNV-1 64-bit constants from the hash/fnv package.
	offset64   = 14695981039346656037
	prime64    = 1099511628211
	bufferSize = 16 * 1024 * 1024
)

// stationData represents the temperature data for a station.
type stationData struct {
	min   int64
	max   int64
	count int64
	total int64
}

// Process reads the input data and returns the result
func Process(inputPath string, output io.Writer) error {
	// split the input file into chunks
	chunks, err := splitChunks(inputPath, runtime.NumCPU())
	if err != nil {
		return err
	}

	// resultsCh is used to collect the results from the goroutines
	resultsCh := make(chan *hashBucket)
	// chunkWg is used to wait for all the goroutines to finish
	var chunkWg sync.WaitGroup
	chunkWg.Add(len(chunks))

	// process each chunk in a separate goroutine
	for _, chunk := range chunks {
		go processChunk(inputPath, chunk, resultsCh, &chunkWg)
	}

	// wait for all the goroutines to finish
	go func() {
		chunkWg.Wait()
		close(resultsCh)
	}()

	// merge the results from the goroutines
	results := newBucket()
	for result := range resultsCh {
		for h, i := range result.getItems() {
			results.insertItem(i.key, h, i.value.min, i.value.max, i.value.total, i.value.count)
		}
	}

	// sort the results by station name
	sort.Sort(results.items)

	_, _ = fmt.Fprint(output, "{")
	idx := 0
	for _, i := range results.getItems() {
		if idx > 0 {
			_, _ = fmt.Fprint(output, ", ")
		}
		average := float64(i.value.total) / float64(i.value.count) / 10
		_, _ = fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", string(i.key), float64(i.value.min)/10, average, float64(i.value.max)/10)
		idx++
	}
	_, _ = fmt.Fprintln(output, "}")
	return nil
}

func processChunk(inputPath string, md metadata, resultsCh chan *hashBucket, chunkWg *sync.WaitGroup) {
	defer chunkWg.Done()

	file, err := os.Open(inputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Seek(md.offset, io.SeekStart)
	if err != nil {
		panic(err)
	}

	reader := io.LimitReader(file, md.size)
	bucket := newBucket()

	buf := make([]byte, bufferSize)
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
			bucket.insertItem(stationName, hash, tempInt, tempInt, tempInt, 1)
		}
		readStart = copy(buf, remaining)
	}
	resultsCh <- bucket
}

// read chunks implementation
const maxLineLength = 100

// metadata represents the chunk offset and size
type metadata struct {
	offset int64
	size   int64
}

// splitChunks splits the file at the given inputPath into count number of chunks
func splitChunks(inputPath string, count int) ([]metadata, error) {
	// open the file
	f, err := os.Open(inputPath)
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
	chunkSize := size / int64(count)

	// buffer to hold the data near the boundary of each chunk
	buf := make([]byte, maxLineLength)
	// slice to store the chunk info
	chunks := make([]metadata, 0, count)
	// offset tracks the start of the current chunk
	offset := int64(0)

	for i := 0; i < count; i++ {
		// handle the last chunk
		if i == count-1 {
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
		newline := byteIndex(chunk, '\n')
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

func byteIndex(src []byte, sep byte) int {
	for i := len(src) - 1; i >= 0; i-- {
		if src[i] == sep {
			return i
		}
	}
	return -1
}

// hashBucket implementation
const numBuckets = 1 << 11 // number of hash buckets (power of 2)

// item represents a key-value pair in the hashBucket.
type item struct {
	key   []byte
	value *stationData
}

type items []item

func (it items) Len() int {
	return len(it)
}

func (it items) Less(i, j int) bool {
	// keep nil where it is
	if it[i].value == nil {
		return false
	}
	// move nil to the end
	if it[j].value == nil {
		return true
	}
	// merge duplicates i and j and set j to nil
	if bytes.Equal(it[i].key, it[j].key) {
		it[i].value.min = min(it[i].value.min, it[j].value.min)
		it[i].value.max = max(it[i].value.max, it[j].value.max)
		it[i].value.total += it[j].value.total
		it[i].value.count += it[j].value.count
		it[j].value = nil

		return true
	}
	return bytes.Compare(it[i].key, it[j].key) < 0
}

func (it items) Swap(i, j int) {
	it[i], it[j] = it[j], it[i]
}

// hashBucket is a hash table with linear probing.
type hashBucket struct {
	items items // hash hashBucket, linearly probed
	size  int   // number of active items in items slice
}

func newBucket() *hashBucket {
	return &hashBucket{
		items: make(items, numBuckets),
	}
}

func (b *hashBucket) insertItem(stationName []byte, hash uint64, minTemp, maxTemp, totalTemp, count int64) {
	hashIndex := int(hash & (numBuckets - 1))
	for {
		// stationData does not exist, add it to the bucket.
		if b.items[hashIndex].key == nil {
			key := make([]byte, len(stationName))
			copy(key, stationName)
			b.items[hashIndex] = item{
				key: key,
				value: &stationData{
					min:   minTemp,
					max:   maxTemp,
					total: totalTemp,
					count: count,
				},
			}
			b.size++
			if b.size > numBuckets/2 {
				panic("too many items in hash table")
			}
			break
		}
		// found existing stationData, merge data.
		if bytes.Equal(b.items[hashIndex].key, stationName) {
			s := b.items[hashIndex].value
			s.min = min(s.min, minTemp)
			s.max = max(s.max, maxTemp)
			s.total += totalTemp
			s.count += count
			break
		}
		// position already occupied in the hashBucket, select another location (linear probe).
		hashIndex++
		if hashIndex >= numBuckets {
			hashIndex = 0
		}
	}
}

func (b *hashBucket) getItems() iter.Seq2[uint64, item] {
	return func(yield func(uint64, item) bool) {
		for hi, i := range b.items {
			if i.value != nil {
				if !yield(uint64(hi), i) {
					return
				}
			}
		}
	}
}
