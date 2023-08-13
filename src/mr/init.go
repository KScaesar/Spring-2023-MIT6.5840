package mr

import (
	"log"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
	// log.SetOutput(io.Discard)
}
