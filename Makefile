.DEFAULT: help
.PHONY: help deps genproto test clean

GOLANG_PATH=$(CURDIR)/.go
GOLANG_BIN=$(GOLANG_PATH)/bin
GOLANG_PROTOBUF_VERSION=v1.28.1
GOLANG_GRPC_VERSION=v1.2.0

help:
	@echo "Please use \`$(MAKE) <target>' where <target> is one of the following:"
	@echo "  help       - show help information"
	@echo "  deps       - setup required dependencies"
	@echo "  genproto   - generate protobuf and gRPC stubs"
	@echo "  test       - run project tests"
	@echo "  clean      - clean up project environment and all the build artifacts"

deps: $(GOLANG_BIN)/protoc-gen-go $(GOLANG_BIN)/protoc-gen-go-grpc
$(GOLANG_BIN)/protoc-gen-go:
	GOPATH=$(GOLANG_PATH) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(GOLANG_PROTOBUF_VERSION)
$(GOLANG_BIN)/protoc-gen-go-grpc:
	GOPATH=$(GOLANG_PATH) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(GOLANG_GRPC_VERSION)

genproto: deps
	protoc -Iproto --go_out=./weed/rpc/master_pb --go-grpc_out=./weed/rpc/master_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative master.proto
	protoc -Iproto --go_out=./weed/rpc/volume_server_pb --go-grpc_out=./weed/rpc/volume_server_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative volume_server.proto
	protoc -Iproto --go_out=./weed/rpc/filer_pb --go-grpc_out=./weed/rpc/filer_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative filer.proto
	protoc -Iproto --go_out=./weed/rpc/remote_pb --go-grpc_out=./weed/rpc/remote_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative remote.proto
	protoc -Iproto --go_out=./weed/rpc/iam_pb --go-grpc_out=./weed/rpc/iam_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative iam.proto
	protoc -Iproto --go_out=./weed/rpc/mount_pb --go-grpc_out=./weed/rpc/mount_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative mount.proto
	protoc -Iproto --go_out=./weed/rpc/s3_pb --go-grpc_out=./weed/rpc/s3_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative s3.proto

test:
	GOPATH=$(GOLANG_PATH) go test -v ./weed/...

clean:
	GOPATH=$(GOLANG_PATH) go clean -modcache
	rm -rf $(GOLANG_PATH)
