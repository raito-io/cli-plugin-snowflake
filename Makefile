GO := go

gotestsum := go run gotest.tools/gotestsum@latest

generate:
	go generate ./...

build: generate
	 go build ./...

unit-test:
	$(gotestsum) --debug --format testname -- -mod=readonly -coverpkg=./... -covermode=atomic -coverprofile=unit-test-coverage.txt ./...

lint:
	golangci-lint run ./...
	go fmt ./...

test:
	$(gotestsum) --debug --format testname -- -mod=readonly -tags=integration -race -coverpkg=./... -covermode=atomic -coverprofile=coverage.out.tmp ./...
	cat coverage.out.tmp | grep -v "/mock_" > coverage.txt #IGNORE MOCKS
	go tool cover -html=coverage.txt -o coverage.html
