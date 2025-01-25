package main

import (
	"fmt"
	"log"
	"onebrc"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: 1brc <filename>")
	}

	filename := os.Args[1]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	size := stat.Size()
	now := time.Now()

	p := onebrc.NewProcess()

	err = p.Process(file, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Proecssed %d MB in %s @ %d MB/s\n", size, time.Since(now), size/1024/1024)

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
}
