package main

import (
	"fmt"
	"log"
	"onebrc"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: 1brc <filename> <version>")
	}

	filename := os.Args[1]
	version := os.Args[2]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	size := stat.Size() / 1024 / 1024
	now := time.Now()

	p := onebrc.NewProcess(version)

	err = p.Process(file, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}

	timeElapsed := time.Since(now)
	fmt.Printf("Proecssed %d MB in %s @ %.2f MB/s\n", size, timeElapsed.String(), float64(size)/timeElapsed.Seconds())

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
}
