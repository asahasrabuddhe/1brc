package onebrc

import (
	"io"
	"os"
)

type Process interface {
	Process(file *os.File, writer io.Writer) error
}

func NewProcess(version string) Process {
	switch version {
	case "v1":
		return V1{}
	case "v2":
		return V2{}
	case "v3":
		return V3{}
	case "v4":
		return V4{}
	case "v5":
		return V5{}
	case "v6":
		return V6{}
	case "v7":
		return V7{}
	case "v8":
		return V8{}
	default:
		panic("invalid version")
	}
}
