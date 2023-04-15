package mr

import (
	"fmt"
	"os"
)

type Spliter struct {
	fn   func(string, ...interface{}) ([]*DataSpec, error)
	Name string
	args []interface{}
}

func (s *Spliter) Split(filename string) ([]*DataSpec, error) {
	return s.fn(filename, s.args...)
}

func NewChunkSpliter(chunkSize int64) *Spliter {
	return &Spliter{
		fn:   chunkSplitFn,
		Name: fmt.Sprintf("chunkSpliter-%dkb", 100),
		args: []interface{}{100},
	}
}

func ceilDivide(numerator, denominator int64) int64 {
	res := numerator / denominator
	if numerator-denominator*res > 0 {
		res += 1
	}
	return res
}

func chunkSplitFn(filename string, chunkKB ...interface{}) (specs []*DataSpec, err error) {
	if len(chunkKB) != 1 {
		return []*DataSpec{}, fmt.Errorf("need arg chunkKb")
	}
	chunkSz, ok := (chunkKB[0]).(int64)
	if !ok {
		return []*DataSpec{}, fmt.Errorf("cannot convertt %v to int64", chunkKB[0])
	}

	fileStats, err := os.Stat(filename)
	if err != nil {
		return []*DataSpec{}, nil
	}
	if fileStats.IsDir() {
		return []*DataSpec{}, fmt.Errorf("%s is dir", filename)
	}
	nChunks := ceilDivide(fileStats.Size(), chunkSz*1024)
	specs = make([]*DataSpec, 0, nChunks)
	var offset int64 = 0
	var chunksz int64 = 1024
	for i := 0; i < int(nChunks); i++ {
		if offset+chunksz > fileStats.Size() {
			chunksz = fileStats.Size() - offset
		}
		specs = append(specs, &DataSpec{
			Offset:   0,
			Len:      chunksz,
			Filename: filename,
			Whence:   0,
		})
	}
	return specs, nil
}

// var (
// 	ChunkSpliter = Spliter{
// 		fn: func(string, []interface{}) ([]*DataSpec, error) {
// 		},
// 		Name: fmt.Sprintf("ChunkSpliter(chunk=100kb)"),
// 		args: []interface{}{100},
// 	}
// )
