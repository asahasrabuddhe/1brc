run:
	go build -o 1brc cmd/1brc.go && \
	./1brc ./testdata/measurements.txt

test:
	go test ./...

bench1b:
	go test -bench=BenchmarkProcessV1 -benchmem
