package onebrc

import (
	"bytes"
	"iter"
)

// hashBucket implementation
const numBuckets = 1 << 11 // number of hash buckets (power of 2)

// item represents a key-value pair in the hashBucket.
type item struct {
	key   []byte
	hash  uint64
	value *DataV4
}

type items []item

// compareItems orders items by key, with empty slots last.
func compareItems(a, b item) int {
	switch {
	case a.value == nil && b.value == nil:
		return 0
	case a.value == nil:
		return 1
	case b.value == nil:
		return -1
	}
	return bytes.Compare(a.key, b.key)
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
				key:  key,
				hash: hash,
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
		for _, i := range b.items {
			if i.value != nil {
				if !yield(i.hash, i) {
					return
				}
			}
		}
	}
}
