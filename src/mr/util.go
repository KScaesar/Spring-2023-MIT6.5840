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
		log.Fatalf("cannot open %v", targetPath)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", targetPath)
	}
	return content
}
