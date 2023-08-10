package mr

import (
	"io"
	"log"
	"os"
)

type TargetKind int

const (
	TargetKindLocal TargetKind = 1<<iota + 1
	TargetKindUrl
)

type OpenFileFunc func(targetPath string) []byte

func OpenLocalFile(targetPath string) []byte {
	file, err := os.Open(targetPath)
	if err != nil {
		log.Fatalf("cannot open %v\n", targetPath)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v\n", targetPath)
	}
	return content
}

// GetRangeFromListByCondition to search on a sorted list returns a range where the condition.
// The returned values 'head' and 'tail' represent
// the inclusive starting index and exclusive ending index of the obtained range, respectively.
//
// Example:
//   list := [-3, -2, -2, -2, -1]
//   head, tail := GetRangeFromListByCondition(list, 1, eq)
//
// Range:
//   [1, 4)
func GetRangeFromListByCondition[T any](list []T, start int, condition func(i, j int) bool) (head, tail int) {
	if start >= len(list) {
		log.Fatalln("GetRangeFromListByCondition: The starting index cannot be equal to the length of the list.")
	}
	head = start
	if head+1 < len(list) {
		tail = head + 1
		for condition(head, tail) {
			tail++
		}
	} else {
		tail = len(list)
	}
	return head, tail
}
