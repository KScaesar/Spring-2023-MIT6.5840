package mr

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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
//
//	list := [-3, -2, -2, -2, -1]
//	head, tail := GetRangeFromListByCondition(list, 1, eq)
//
// Range:
//
//	[1, 4)
func GetRangeFromListByCondition[T any](list []T, start int, condition func(i, j int) bool) (head, tail int) {
	if start >= len(list) {
		log.Fatalln("GetRangeFromListByCondition: The starting index cannot be equal to the length of the list.")
	}
	head = start
	if head < len(list) {
		tail = head + 1
		for tail < len(list) && condition(head, tail) {
			tail++
		}
	} else {
		tail = len(list)
	}
	return head, tail
}

// AtomicWriteFile
// MapReduce paper mentions the trick of
// using a temporary file and atomically renaming it
// once it is completely written.
//
// Ref:
//
//	http://static.googleusercontent.com/media/research.google.com/zh-TW//archive/mapreduce-osdi04.pdf
func AtomicWriteFile(targetFilePath, tempFilePath string, writeAction func(file *os.File) error) error {
	_, err := os.Stat(targetFilePath)
	if err == nil {
		log.Printf("file='%v' exist\n", targetFilePath)
		return nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("query file for check task is completed: %w", err)
	}

	tempDir := filepath.Dir(tempFilePath)
	tempPattern := filepath.Base(tempFilePath)
	tempFile, err := os.CreateTemp(tempDir, tempPattern)
	if err != nil {
		return fmt.Errorf("create temp file failed: %w", err)
	}

	tempFilename := tempFile.Name()
	err = writeAction(tempFile)
	if err != nil {
		tempFile.Close()
		return fmt.Errorf("write file failed: %w", err)
	}
	tempFile.Close()

	targetFilename := filepath.Base(targetFilePath)
	err = os.Rename(tempFilename, targetFilename)
	if err != nil {
		return fmt.Errorf("rename file failed: %w", err)
	}

	return nil
}
