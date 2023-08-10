package mr

import "fmt"

var (
	ErrNoTask        = fmt.Errorf("no task can be aquired")
	ErrParamNotMatch = fmt.Errorf("parameter not match")
	ErrSystemFail    = fmt.Errorf("system fail")
)
