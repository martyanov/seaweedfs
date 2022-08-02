BINARY = weed

SOURCE_DIR = .

all: install

install:
	cd weed; go install

tests:
	cd weed; go test -v ./...
