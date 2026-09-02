.PHONY: build test bench run clean debug

build:
	go build -ldflags="-s -w" -o bin/akwadb ./cmd/akwadb

run: build
	./bin/akwadb -addr :6379 -data-dir ./data -memtable-mb 2

test:
	go test -v -race -timeout 30s ./...

bench:
	go test -bench=. -benchmem -cpuprofile cpu.prof ./...

clean:
	rm -rf bin/ data/ akwadata/ *.prof

# run pprof server
debug:
	go tool pprof -http=:8080 cpu.prof
