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
	default:
		panic("invalid version")
	}
}
