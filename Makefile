.DEFAULT_GOAL := all

PROTO_COMPILE ?= protoc
PROTO_PATH = ./proto/gen-proto

gen:
	mkdir -p $(PROTO_PATH) && \
		$(PROTO_COMPILE) --proto_path=./proto proto/hstream.proto \
			--go_out=$(PROTO_PATH) \
			--go-grpc_out=$(PROTO_PATH)

all: gen
	go build ./...

clean:
	rm -rf $(PROTO_PATH)

fmt:
	gofmt -s -w -l `find . -name '*.go' -type f ! -path '*/gen-proto/*' -print`

test:
	go test -gcflags=-l -race ${TEST_FLAGS} ./...

.PHONY: clean fmt all gen test
