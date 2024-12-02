package main

import (
	"os"

	"onebrc"
)

func main() {
	//defer profile.Start(profile.ProfilePath(".")).Stop()

	file, err := os.Open("/Users/ajitem/1brc/measurements-1b.txt")
	if err != nil {
		panic(err)
	}

	err = onebrc.Process(file, os.Stdout)
	if err != nil {
		panic(err)
	}
}
