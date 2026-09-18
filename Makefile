.PHONY: build test race lint bench clean

build:
	go build -trimpath -ldflags="-s -w" -o bin/kv ./cmd/kv

test:
	go test ./...

race:
	go test -race -cover ./...

lint:
	gofmt -l . && go vet ./...

bench: build
	rm -rf /tmp/kv-bench && ./bin/kv -dir /tmp/kv-bench bench 50000

clean:
	rm -rf bin data
