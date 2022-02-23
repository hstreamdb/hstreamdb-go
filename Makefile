PROTO_COMPILE = protoc

gen:
	mkdir -p gen-proto && \
		$(PROTO_COMPILE) --proto_path=./proto proto/*.proto --go_out=plugins=grpc:gen-proto

clean:
	rm -rf gen-proto

fmt:
	gofmt -s -w -l `find . -name '*.go' -type f ! -path '*/gen-proto/*' -print`

.PHONY: clean test gen

