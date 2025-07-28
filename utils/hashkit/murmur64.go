package hashkit

const (
	mul uint64 = 0xc6a4a7935bd1e995
	rtt uint32 = 47
)

func Murmur64(data []byte) uint64 {
	var length = uint64(len(data))
	var hash = 19780211 ^ (length * mul)

	for num := uint64(0); length >= 8; length -= 8 {
		num = uint64(data[0]) | uint64(data[1])<<8 |
			uint64(data[2])<<16 | uint64(data[3])<<24 | uint64(data[4])<<32 |
			uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56
		num *= mul
		num ^= num >> rtt
		num *= mul

		hash *= mul
		hash ^= num
		data = data[8:]
	}

	switch length {
	case 7:
		hash ^= uint64(data[6]) << 48
		fallthrough
	case 6:
		hash ^= uint64(data[5]) << 40
		fallthrough
	case 5:
		hash ^= uint64(data[4]) << 32
		fallthrough
	case 4:
		hash ^= uint64(data[3]) << 24
		fallthrough
	case 3:
		hash ^= uint64(data[2]) << 16
		fallthrough
	case 2:
		hash ^= uint64(data[1]) << 8
		fallthrough
	case 1:
		hash ^= uint64(data[0])
		hash *= mul
	}

	hash ^= hash >> rtt
	hash *= mul
	hash ^= hash >> rtt
	return hash
}
