build:
	go mod download
	go build -o tcache-server ./cmd/
