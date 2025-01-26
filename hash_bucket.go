package onebrc

import (
	"bytes"
	"iter"
)

// hashBucket implementation
const numBuckets = 1 << 14 // number of hash buckets (power of 2)

// item represents a key-value pair in the hashBucket.
type item struct {
	key   []byte
	value *DataV4
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
		it[i].value.Min = min(it[i].value.Min, it[j].value.Min)
		it[i].value.Max = max(it[i].value.Max, it[j].value.Max)
		it[i].value.Total += it[j].value.Total
		it[i].value.Count += it[j].value.Count
		it[j].value = nil

		it.Swap(i, j)

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
		// item does not exist, add it to the bucket.
		if b.items[hashIndex].key == nil {
			key := make([]byte, len(stationName))
			copy(key, stationName)
			b.items[hashIndex] = item{
				key: key,
				value: &DataV4{
					Min:   minTemp,
					Max:   maxTemp,
					Total: totalTemp,
					Count: count,
				},
			}
			b.size++
			if b.size > numBuckets/2 {
				panic("too many items in hash table")
			}
			break
		}
		// found existing item, merge data.
		if bytes.Equal(b.items[hashIndex].key, stationName) {
			s := b.items[hashIndex].value
			s.Min = min(s.Min, minTemp)
			s.Max = max(s.Max, maxTemp)
			s.Total += totalTemp
			s.Count += count
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
