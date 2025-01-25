package onebrc

import (
	"io"
	"os"
)

type Process interface {
	Process(file *os.File, writer io.Writer) error
}

func NewProcess() Process {
	return V1{}
}
