package hashkit

import "hash"

const (
	DefaultSum32 = 0
)

type sum32 uint32

func (s *sum32) BlockSize() int { return 1 }
func (s *sum32) Reset()         { *s = DefaultSum32 }
func (s *sum32) Size() int      { return 4 }
func (s *sum32) Sum(in []byte) []byte {
	v := uint32(*s)
	return append(in, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (s *sum32) Sum32() uint32 { return uint32(*s) }
func (s *sum32) Write(data []byte) (int, error) {
	hash := *s
	for _, b := range data {
		hash += sum32(b)
		hash += hash << 10
		hash ^= hash >> 6
	}
	hash += hash << 3
	hash ^= hash >> 11
	hash += hash << 15
	*s = hash
	return len(data), nil
}

func NewJenkins32() hash.Hash32 {
	var s sum32 = 0
	return &s
}

func Jenkins(data []byte) uint32 {
	var hash uint32 = 0
	for _, b := range data {
		hash += uint32(b)
		hash += hash << 10
		hash ^= hash >> 6
	}
	hash += hash << 3
	hash ^= hash >> 11
	hash += hash << 15
	return hash
}

func JenkinsString(data string) uint32 {
	var hash uint32 = 0
	for _, b := range data {
		hash += uint32(b)
		hash += hash << 10
		hash ^= hash >> 6
	}
	hash += hash << 3
	hash ^= hash >> 11
	hash += hash << 15
	return hash
}
