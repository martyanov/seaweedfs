BINARY = weed

SOURCE_DIR = .

all: install

install:
	cd weed; go install

full_install:
	cd weed; go install -tags "elastic gocdk"

tests:
	cd weed; go test -tags "elastic gocdk" -v ./...
