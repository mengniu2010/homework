package mr

import (
	"fmt"
	"testing"
)

func TestReadData(t *testing.T) {
	spec := DataSpec{
		Filename: "job.go",
		Whence:   0,
		Offset:   0,
		Len:      12,
	}
	s, e := spec.ReadData()
	fmt.Println(s)
	fmt.Println(len(s))
	fmt.Println(e)
	// t.Fail()
}
