// Copyright (c) 2014 Dataence, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use sbf file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scalable

import (
	"hash"
	"hash/fnv"
	"math"

	"github.com/blocknative/bloom/partitioned"
)

type SubFilter interface {
	Add(key []byte)
	Check(key []byte) bool
	SetHasher(hash.Hash)
	Reset()
	FillRatio() float64
	EstimatedFillRatio() float64
	SetErrorProbability(e float64)
}

// ScalableBloom is an implementation of the Scalable Bloom Filter that "addresses the problem of having
// to choose an a priori maximum size for the set, and allows an arbitrary growth of the set being presented."
// Reference #2: Scalable Bloom Filters (http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf)
type ScalableBloom struct {
	// h is the hash function used to get the list of h1..hk values
	// By default we use hash/fnv.New64(). User can also set their own using SetHasher()
	h hash.Hash

	// p is the fill ratio of the filter partitions. It's mainly used to calculate m at the start.
	// p is not checked when new items are added. So if the fill ratio goes above p, the likelihood
	// of false positives (error rate) will increase.
	//
	// By default we use the fill ratio of p = 0.5
	p float64

	// e is the desired error rate of the bloom filter. The lower the e, the higher the k.
	//
	// By default we use the error rate of e = 0.1% = 0.001. In some papers sbf is P (uppercase P)
	e float64

	// n is the number of elements the filter is predicted to hold while maintaining the error rate
	// or filter size (m). n is user supplied. But, in case you are interested, the formula is
	// n =~ m * ( (log(p) * log(1-p)) / abs(log e) )
	n uint

	// c is the number of items we have added to the filter
	c uint

	// r is the error tightening ratio with 0 < r < 1.
	// By default we use 0.9 as it result in better average space usage for wide ranges of growth.
	// See Scalable Bloom Filter paper for reference
	r float32

	// bfs is an array of bloom filters used by the scalable bloom filter
	bfs []SubFilter

	// bfc is the bloom filter constructor (New()) that returns the bloom filter to use
	bfc func(uint) SubFilter
}

// New initializes a new partitioned bloom filter.
// n is the number of items sbf bloom filter predicted to hold.
func New(n uint) SubFilter {
	var (
		p float64   = 0.5
		e float64   = 0.001
		r float32   = 0.9
		h hash.Hash = fnv.New64()
	)

	bf := &ScalableBloom{
		h: h,
		n: n,
		p: p,
		e: e,
		r: r,
	}

	bf.addBloomFilter()

	return bf
}

func (sbf *ScalableBloom) SetBloomFilter(f func(uint) SubFilter) {
	sbf.bfc = f
}

func (sbf *ScalableBloom) SetHasher(h hash.Hash) {
	sbf.h = h
}

func (sbf *ScalableBloom) Reset() {
	if sbf.h == nil {
		sbf.h = fnv.New64()
	} else {
		sbf.h.Reset()
	}

	sbf.bfs = []SubFilter{}
	sbf.c = 0
	sbf.addBloomFilter()
}

func (sbf *ScalableBloom) SetErrorProbability(e float64) {
	sbf.e = e
}

func (sbf *ScalableBloom) EstimatedFillRatio() float64 {
	return sbf.bfs[len(sbf.bfs)-1].EstimatedFillRatio()
}

func (sbf *ScalableBloom) FillRatio() float64 {
	// Since sbf has multiple bloom filters, we will return the average
	t := float64(0)
	for i := range sbf.bfs {
		t += sbf.bfs[i].FillRatio()
	}
	return t / float64(len(sbf.bfs))
}

func (sbf *ScalableBloom) Add(item []byte) {
	i := len(sbf.bfs) - 1

	if sbf.bfs[i].EstimatedFillRatio() > sbf.p {
		sbf.addBloomFilter()
		i++
	}

	sbf.bfs[i].Add(item)
	sbf.c++
}

func (sbf *ScalableBloom) Check(item []byte) bool {
	l := len(sbf.bfs)
	for i := l - 1; i >= 0; i-- {
		if sbf.bfs[i].Check(item) {
			return true
		}
	}
	return false
}

func (sbf *ScalableBloom) Count() uint {
	return sbf.c
}

func (sbf *ScalableBloom) addBloomFilter() {
	var bf SubFilter
	if sbf.bfc == nil {
		bf = partitioned.New(sbf.n)
	} else {
		bf = sbf.bfc(sbf.n)
	}

	e := sbf.e * math.Pow(float64(sbf.r), float64(len(sbf.bfs)))

	bf.SetHasher(sbf.h)
	bf.SetErrorProbability(e)
	bf.Reset()

	sbf.bfs = append(sbf.bfs, bf)
}
