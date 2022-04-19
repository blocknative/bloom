// Copyright (c) 2014 Dataence, LLC. All rights reserved.
// Copyright (c) 2020 Blocknative Corporation. All rights reserved.
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

package bloom

import (
	"math"
)

// ScalableFilter is an implementation of the Scalable Bloom Filter that "addresses the problem of having
// to choose an a priori maximum size for the set, and allows an arbitrary growth of the set being presented."
// Reference #2: Scalable Bloom Filters (http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf)
type ScalableFilter struct {
	params
	opt []Option

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
	bfs []*Filter
}

// New initializes a new partitioned bloom filter.
// n is the number of items sbf bloom filter predicted to hold.
func NewScalable(n uint, opt ...Option) *ScalableFilter {
	if n == 0 {
		panic("n == 0")
	}

	bf := ScalableFilter{
		opt: opt,
		n:   n,
		r:   0.9,
	}

	for _, option := range withDefault(opt) {
		option(&bf.params)
	}

	bf.addBloomFilter()

	return &bf
}

func (sbf *ScalableFilter) Reset() {
	sbf.bfs = []*Filter{}
	sbf.c = 0
	sbf.addBloomFilter()
}

func (sbf *ScalableFilter) EstimatedFillRatio() float64 {
	return sbf.bfs[len(sbf.bfs)-1].EstimatedFillRatio()
}

func (sbf *ScalableFilter) FillRatio() float64 {
	// Since sbf has multiple bloom filters, we will return the average
	t := float64(0)
	for i := range sbf.bfs {
		t += sbf.bfs[i].FillRatio()
	}
	return t / float64(len(sbf.bfs))
}

func (sbf *ScalableFilter) Add(item []byte) {
	i := len(sbf.bfs) - 1

	if sbf.bfs[i].EstimatedFillRatio() > sbf.p {
		sbf.addBloomFilter()
		i++
	}

	sbf.bfs[i].Add(item)
	sbf.c++
}

func (sbf *ScalableFilter) Check(item []byte) bool {
	l := len(sbf.bfs)
	for i := l - 1; i >= 0; i-- {
		if sbf.bfs[i].Check(item) {
			return true
		}
	}
	return false
}

func (sbf *ScalableFilter) Count() uint {
	return sbf.c
}

func (sbf *ScalableFilter) addBloomFilter() {
	e := sbf.e * math.Pow(float64(sbf.r), float64(len(sbf.bfs)))
	bf := New(sbf.n, append(sbf.opt, WithErrorRate(e))...)
	sbf.bfs = append(sbf.bfs, bf)
}
