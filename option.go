package bloom

import (
	"hash"

	"github.com/zentures/cityhash"
)

type params struct {
	h hash.Hash

	// e specifies the desired error rate for the filter.
	// Smaller values of e imply a larger number of hash values used
	// to set and test bits (the K parameter).
	//
	// If e <= 0, defaults to .001
	e float64

	// p specifies the maximum porportion of bits that may be
	// set to 1 in the filter.  This influences how large the filter must
	// be in order to guarantee the specified error rate.  Note that this
	// fill ratio is not strictly enforced.  Overloading a filter happens
	// silently, causing the error rate (false positives) to increase.
	//
	// If p <= 0, defaults to 0.5
	p float64
}

type Option func(*params)

// WithHash specifies the hash to use with the bloom filter.
// If h == nil, defaults to CityHash.
func WithHash(h hash.Hash) Option {
	if h == nil {
		h = cityhash.New64()
	}

	return func(ps *params) {
		ps.h = h
	}
}

// WithErrorRate sets the desired error rate for the bloom filter.
// Smaller values of e imply a larger number of hash values used
// to set and test bits (the K parameter).
//
// If e <= 0, defaults to .001.
func WithErrorRate(e float64) Option {
	if e <= 0 {
		e = .001
	}

	return func(ps *params) {
		ps.e = e
	}
}

// WithFillRation specifies the maximum porportion of bits that may be
// set to 1 in the filter.  This influences how large the filter must
// be in order to guarantee the specified error rate.  Note that this
// fill ratio is not strictly enforced.  Overloading a filter happens
// silently, causing the error rate (false positives) to increase.
//
// If p <= 0, defaults to 0.5
func WithFillRatio(p float64) Option {
	if p <= 0 {
		p = .5
	}

	return func(ps *params) {
		ps.p = p
	}
}

func withDefault(opt []Option) []Option {
	return append([]Option{
		WithHash(nil),
		WithErrorRate(0),
		WithFillRatio(0),
	}, opt...)
}
