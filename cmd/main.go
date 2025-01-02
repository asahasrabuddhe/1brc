package main

import (
	"fmt"
	"os"

	"brc"
)

func main() {
	file, err := os.Open("data/measurements.txt")
	if err != nil {
		panic(err)
	}

	output, err := brc.Process(file)
	if err != nil {
		panic(err)
	}

	fmt.Print(output)

	err = file.Close()
	if err != nil {
		panic(err)
	}
}
