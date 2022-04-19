// Copyright (c) 2014 Dataence, LLC. All rights reserved.
// Copyright (c) 2020 Blocknative Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
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
	"crypto/md5"
	"crypto/sha1"
	"hash"
	"hash/crc64"
	"hash/fnv"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/zentures/cityhash"
)

func TestScalableBloomFilter(t *testing.T) {
	t.Parallel()

	lengths := []uint{10000, 100000}
	hashes := []hash.Hash{
		fnv.New64(),
		crc64.New(crc64.MakeTable(crc64.ECMA)),
		murmur3.New64(),
		cityhash.New64(),
		md5.New(),
		sha1.New()}

	for _, l := range lengths {
		for _, h := range hashes {
			bf := NewScalable(l, WithHash(h))
			bf.Reset()
			testBloomFilter(t, bf)
		}
	}
}

func BenchmarkScalableFNV64(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := NewScalable(uint(b.N))
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkScalableCRC64(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := NewScalable(uint(b.N), WithHash(crc64.New(crc64.MakeTable(crc64.ECMA))))
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkScalableMurmur3(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := NewScalable(uint(b.N), WithHash(murmur3.New64()))
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkScalableCityHash(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := NewScalable(uint(b.N), WithHash(cityhash.New64()))
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkScalableMD5(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := NewScalable(uint(b.N), WithHash(md5.New()))
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkScalableSha1(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := NewScalable(uint(b.N), WithHash(sha1.New()))
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}
