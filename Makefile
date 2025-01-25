VERSION_LOWER = $(shell echo $(VERSION) | tr '[:upper:]' '[:lower:]')
VERSION_UPPER = $(shell echo $(VERSION) | tr '[:lower:]' '[:upper:]')

run:
	go build -o 1brc cmd/1brc.go && ./1brc ./testdata/measurements.txt ${VERSION_LOWER} | tee results/${VERSION_LOWER}.txt && rm 1brc

test:
	go test ./...

bench:
	go test -timeout=30m -count=10 -bench=BenchmarkProcess${VERSION_UPPER} -benchmem -o=profiles/${VERSION_LOWER}.test -cpuprofile=profiles/${VERSION_UPPER}.out | tee benchmarks/${VERSION_LOWER}.txt
