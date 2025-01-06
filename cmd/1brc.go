package main

import (
	"os"

	"onebrc"
)

func main() {
	err := onebrc.Process("/Users/ajitem/1brc/measurements-1b.txt", os.Stdout)
	if err != nil {
		panic(err)
	}
}
