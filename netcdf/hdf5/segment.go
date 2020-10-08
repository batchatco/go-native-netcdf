package hdf5

import (
	"io"
	"sort"
)

// segments are a bunch of discontiguous locations and lengths in
// file to read data for an object.
type segment struct {
	offset uint64
	length uint64
	r      io.Reader // reader to use for this segment
}

// object representing all the segments
type segments struct {
	segs []*segment
}

func newSegments() *segments {
	return &segments{make([]*segment, 0)}
}

// Standard sorting functions
func (s *segments) Len() int {
	return len(s.segs)
}

func (s *segments) Swap(i, j int) {
	s.segs[i], s.segs[j] = s.segs[j], s.segs[i]
}

func (s *segments) Less(i, j int) bool {
	return s.segs[i].offset <= s.segs[j].offset
}

// other methods
func (s *segments) get(i int) *segment {
	return s.segs[i]
}

func (s *segments) sort() {
	sort.Sort(s)
}

func (s *segments) append(thisSeg *segment) {
	s.segs = append(s.segs, thisSeg)
}
