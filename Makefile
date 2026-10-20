VERSION_LOWER = $(shell echo $(VERSION) | tr '[:upper:]' '[:lower:]')
VERSION_UPPER = $(shell echo $(VERSION) | tr '[:lower:]' '[:upper:]')
PGO = $(if $(filter v8,$(VERSION_LOWER)),profiles/v8.pgo,off)

run:
	go build -o 1brc cmd/1brc.go && ./1brc ./testdata/measurements.txt ${VERSION_LOWER} | tee results/${VERSION_LOWER}.txt && rm 1brc

test:
	go test ./...

bench:
	go test -timeout=45m -count=5 -bench=BenchmarkProcess${VERSION_UPPER} -benchmem -pgo=${PGO} -o=profiles/${VERSION_LOWER}.test -cpuprofile=profiles/${VERSION_LOWER}.out | tee benchmarks/${VERSION_LOWER}.txt
