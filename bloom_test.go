// Copyright (c) 2014 Dataence, LLC. All rights reserved.
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
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"hash"
	"hash/crc64"
	"hash/fnv"
	"os"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/zentures/cityhash"
)

var web2, web2a []string

func init() {
	f, err := os.Open("/usr/share/dict/web2")
	if err != nil {
		fmt.Println("Cannot open /usr/share/dict/web2 - " + err.Error())
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		web2 = append(web2, scanner.Text())
	}

	if err = scanner.Err(); err != nil {
		panic(err)
	}

	f, err2 := os.Open("/usr/share/dict/web2a")
	if err2 != nil {
		panic(err)
	}
	defer f.Close()

	scanner = bufio.NewScanner(f)
	for scanner.Scan() {
		web2a = append(web2a, scanner.Text())
	}

	if err2 = scanner.Err(); err2 != nil {
		panic(err)
	}
}

type filter interface {
	Add([]byte)
	Check([]byte) bool
}

func TestBloomFilter(t *testing.T) {
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
			bf := New(l, WithHash(h))
			testBloomFilter(t, bf)
		}
	}
}

func BenchmarkBloomFNV64(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N))
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomCRC64(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N), WithHash(crc64.New(crc64.MakeTable(crc64.ECMA))))
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomMurmur3(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N), WithHash(murmur3.New64()))
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomCityHash(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N), WithHash(cityhash.New64()))
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomMD5(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N), WithHash(md5.New()))
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomSha1(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N), WithHash(sha1.New()))
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if bf.Add([]byte(lines[l])); !bf.Check([]byte(lines[l])) {
			fn++
		}
	}

	b.StopTimer()
}

func testBloomFilter(t *testing.T, f filter) {
	fn, fp := 0, 0

	for l := range web2 {
		if f.Add([]byte(web2[l])); !f.Check([]byte(web2[l])) {
			fn++
		}
	}

	for l := range web2a {
		if f.Check([]byte(web2a[l])) {
			fp++
		}
	}

	fmt.Printf("Total false negatives: %d (%.4f%%)\n", fn, (float32(fn) / float32(len(web2)) * 100))
	fmt.Printf("Total false positives: %d (%.4f%%)\n", fp, (float32(fp) / float32(len(web2a)) * 100))
}
