package hdf5

import (
	"encoding/binary"
)

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
