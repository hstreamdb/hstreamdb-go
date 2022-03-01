PROTO_COMPILE = protoc
PROTO_PATH = ./proto/gen-proto

gen:
	mkdir -p $(PROTO_PATH) && \
		$(PROTO_COMPILE) --proto_path=./proto proto/*.proto --go_out=plugins=grpc:$(PROTO_PATH)

clean:
	rm -rf $(PROTO_PATH)

fmt:
	gofmt -s -w -l `find . -name '*.go' -type f ! -path '*/gen-proto/*' -print`

.PHONY: clean test gen

