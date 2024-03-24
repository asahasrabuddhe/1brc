package main

import (
	"fmt"
	"os"
	"time"

	"onebrc"
)

func main() {
	started := time.Now()
	defer func() {
		fmt.Printf("Done in %0.6f", time.Since(started).Seconds())
	}()

	file, err := os.Open("/Users/ajitem/1brc/measurements.txt")
	if err != nil {
		panic(err)
	}

	err = onebrc.Process(file, os.Stdout)
	if err != nil {
		panic(err)
	}
}
