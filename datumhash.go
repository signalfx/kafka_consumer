package main

import (
	"encoding/binary"
	"github.com/signalfx/golib/datapoint"
	"github.com/spaolacci/murmur3"
	"sort"
	"unicode/utf16"
)

// Murmur128Hasher does a 128bit hash of a dp
type Murmur128Hasher struct {
	encoder GoJavaStringEncoder
	hasher  murmur3.Hash128
	allKeys []string
}

func sortedDimensions(dims map[string]string, allKeys *[]string) []string {
	sliceSize := len(dims) + 1
	if len(*allKeys) < sliceSize {
		*allKeys = make([]string, sliceSize)
	}
	ret := (*allKeys)[0:sliceSize]
	ret[0] = "sf_metric"
	idx := 1
	for k := range dims {
		ret[idx] = k
		idx++
	}
	sort.Strings(ret)
	return ret
}

// Sum128 hashes a datum into the given buffer
func (s *Murmur128Hasher) Sum128(dp *datapoint.Datapoint) (uint64, uint64) {
	s.hasher.Reset()
	allKeys := sortedDimensions(dp.Dimensions, &s.allKeys)
	for _, k := range allKeys {
		s.hasher.Write(s.encoder.Encode(k))
		s.hasher.Write([]byte{0})
		if k == "sf_metric" {
			s.hasher.Write(s.encoder.Encode(dp.Metric))
		} else {
			s.hasher.Write(s.encoder.Encode(dp.Dimensions[k]))
		}
		s.hasher.Write([]byte{1})
	}
	return s.hasher.Sum128()
}

// InitMurmur128Hasher creates a datapoint hasher
func InitMurmur128Hasher() Murmur128Hasher {
	return Murmur128Hasher{
		hasher:  murmur3.New128(),
		allKeys: make([]string, 16),
		encoder: GoJavaStringEncoder{
			runes: make([]rune, 128),
			bytes: make([]byte, 256),
		},
	}
}

// GoJavaStringEncoder will hash a go string as a Java utf16 string, while keeping tmp copies of
// buffer memory so we don't constantly realloc
type GoJavaStringEncoder struct {
	runes []rune
	bytes []byte
}

// Encode a string as utf16 in bytes.  Not thread safe!
func (g *GoJavaStringEncoder) Encode(s string) []byte {
	g.runes = g.runes[:0]
	for _, r := range s {
		g.runes = append(g.runes, r)
	}
	unicodeSequence := utf16.Encode(g.runes)
	neededReturnSize := len(unicodeSequence) * 2
	if len(g.bytes) < neededReturnSize {
		g.bytes = make([]byte, neededReturnSize)
	}
	at := 0
	for _, u := range unicodeSequence {
		binary.LittleEndian.PutUint16(g.bytes[at:at+2], u)
		at += 2
	}
	return g.bytes[0:neededReturnSize]
}
