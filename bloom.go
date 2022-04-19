// Copyright (c) 2014 Dataence, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use f file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bloom

import (
	"encoding/binary"
	"math"

	"github.com/bits-and-blooms/bitset"
)

// Filter is the standard implementation used by this package.  It is a
// variant implementation of the standard bloom filter that reduces the risk
// of false-positives by assigning a bit array to each hash function.
//
// Reference #2: Scalable Bloom Filters (http://gsd.di.uminho.pt/members/cbm/ps/dpdf)
//
// The name Partitioned Bloom Filter is my choice as there was no name assigned to f variant.
type Filter struct {
	params

	// m is the total number of bits for f bloom filter. m for the partitioned bloom filter
	// will be divided into k partitions, or slices. So each partition contains Math.ceil(m/k) bits.
	//
	// m =~ n / ((log(p)*log(1-p))/abs(log e))
	m uint

	// k is the number of hash values used to set and test bits. Each filter partition will be
	// set/tested using a single hash value. Note that the number of hash functions may not be the
	// same as hash values. For example, our implementation uses 32-bit hash values. So a single
	// Murmur3 128bit hash function can be used as 4 32-bit hash values. A single FNV 64bit hash function
	// can be used as 2 32-bit has values.
	//
	// k = log2(1/e)
	// Given that our e is defaulted to 0.001, therefore k ~= 10, which means we need 10 hash values
	k uint

	// n is the number of elements the filter is predicted to hold while maintaining the error rate
	// or filter size (m). n is user supplied. But, in case you are interested, the formula is
	// n =~ m * ( (log(p) * log(1-p)) / abs(log e) )
	n uint

	// c is the number of items we have added to the filter
	c uint

	// s is the size of the partition, or slice.
	// s = m / k
	s uint

	// b is the set of bit array holding the bloom filters. There will be k b's.
	b []*bitset.BitSet

	// bs holds the list of bits to be set/check based on the hash values
	bs []uint
}

// New initializes a new partitioned bloom filter.
// n is the number of items f bloom filter predicted to hold.
func New(n uint, opt ...Option) *Filter {
	if n == 0 {
		panic("n == 0")
	}

	var f = Filter{n: n}
	for _, option := range withDefault(opt) {
		option(&f.params)
	}

	f.k = k(f.e)
	f.m = m(n, f.p, f.e)
	f.s = s(f.m, f.k)
	f.b = makePartitions(f.k, f.s)
	f.bs = make([]uint, f.k)

	return &f
}

func (f *Filter) Reset() {
	for _, b := range f.b {
		b.ClearAll()
	}

	f.h.Reset()
}

func (f *Filter) EstimatedFillRatio() float64 {
	return 1 - math.Exp(-float64(f.c)/float64(f.s))
}

func (f *Filter) FillRatio() float64 {
	// Since f is partitioned, we will return the average fill ratio of all partitions
	t := float64(0)
	for _, v := range f.b[:f.k] {
		t += (float64(v.Count()) / float64(f.s))
	}
	return t / float64(f.k)
}

func (f *Filter) Add(item []byte) {
	f.bits(item)
	for i, v := range f.bs[:f.k] {
		f.b[i].Set(v)
	}
	f.c++
}

func (f *Filter) Check(item []byte) bool {
	f.bits(item)
	for i, v := range f.bs[:f.k] {
		if !f.b[i].Test(v) {
			return false
		}
	}
	return true
}

func (f *Filter) Count() uint {
	return f.c
}

func (f *Filter) bits(item []byte) {
	f.h.Reset()
	f.h.Write(item)
	s := f.h.Sum(nil)
	a := binary.BigEndian.Uint32(s[4:8])
	b := binary.BigEndian.Uint32(s[0:4])

	// Reference: Less Hashing, Same Performance: Building a Better Bloom Filter
	// URL: http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/rsa.pdf
	for i := range f.bs[:f.k] {
		f.bs[i] = (uint(a) + uint(b)*uint(i)) % f.s
	}
}

func makePartitions(k, s uint) []*bitset.BitSet {
	b := make([]*bitset.BitSet, k)

	for i := range b {
		b[i] = bitset.New(s)
	}

	return b
}

func k(e float64) uint {
	return uint(math.Ceil(math.Log2(1 / e)))
}

func m(n uint, p, e float64) uint {
	// m =~ n / ((log(p)*log(1-p))/abs(log e))
	return uint(math.Ceil(float64(n) / ((math.Log(p) * math.Log(1-p)) / math.Abs(math.Log(e)))))
}

func s(m, k uint) uint {
	return uint(math.Ceil(float64(m) / float64(k)))
}
