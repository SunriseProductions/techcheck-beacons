.PHONY: build-beacon test-beacon clean

build-beacon:
	mkdir -p build
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o build/beacon -trimpath -ldflags='-s -w' ./cmd/beacon

test-beacon:
	go test ./cmd/beacon/... -v

clean:
	rm -rf build/
