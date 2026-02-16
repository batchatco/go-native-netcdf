package hdf5

import (
	"encoding/binary"
)

// Hash function used by HDF5 for checksums (Jenkins lookup3 hash function)

func rot(x uint32, k uint32) uint32 {
	return x<<k | x>>(32-k)
}

func mix(a, b, c *uint32) {
	*a -= *c
	*a ^= rot(*c, 4)
	*c += *b

	*b -= *a
	*b ^= rot(*a, 6)
	*a += *c

	*c -= *b
	*c ^= rot(*b, 8)
	*b += *a

	*a -= *c
	*a ^= rot(*c, 16)
	*c += *b

	*b -= *a
	*b ^= rot(*a, 19)
	*a += *c

	*c -= *b
	*c ^= rot(*b, 4)
	*b += *a
}

func final(a, b, c *uint32) {
	*c ^= *b
	*c -= rot(*b, 14)

	*a ^= *c
	*a -= rot(*c, 11)

	*b ^= *a
	*b -= rot(*a, 25)

	*c ^= *b
	*c -= rot(*b, 16)

	*a ^= *c
	*a -= rot(*c, 4)

	*b ^= *a
	*b -= rot(*a, 14)

	*c ^= *b
	*c -= rot(*b, 24)
}

// Computes the HDF5 checksum on 'vals'. nBytes is the number of signficant bytes in
// 'vals', or 0 to compute it based upon the length of 'vals' (4*len(vals)).
func hashInts(vals []uint32, nBytes uint32) uint32 {
	if nBytes == 0 {
		nBytes = uint32(4 * len(vals))
	}
	a := 0xdeadbeef + nBytes
	b := a
	c := a
	for nBytes > 12 {
		a += vals[0]
		b += vals[1]
		c += vals[2]
		mix(&a, &b, &c)
		vals = vals[3:]
		nBytes -= 12
	}
	switch nBytes {
	case 12, 11, 10, 9:
		a += vals[0]
		b += vals[1]
		c += vals[2]
	case 8, 7, 6, 5:
		a += vals[0]
		b += vals[1]
	case 4, 3, 2, 1:
		a += vals[0]
	case 0:
		return c
	}
	final(&a, &b, &c)
	return c
}

func fletcher32(vals []uint16) uint32 {
	sum1 := uint32(0)
	sum2 := uint32(0)
	for _, v := range vals {
		sum1 = (sum1 + uint32(v)) % 65535
		sum2 = (sum2 + sum1) % 65535
	}
	return (uint32(sum2) << 16) | uint32(sum1)
}

func checksum(data []byte) uint32 {
	n := len(data)
	nUint32 := (n + 3) / 4
	vals := make([]uint32, nUint32)
	for i := 0; i < nUint32; i++ {
		start := i * 4
		end := (i + 1) * 4
		if end > n {
			end = n
		}
		var b [4]byte
		copy(b[:], data[start:end])
		vals[i] = binary.LittleEndian.Uint32(b[:])
	}
	return hashInts(vals, uint32(n))
}
