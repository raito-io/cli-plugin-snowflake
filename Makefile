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
	$(gotestsum) --debug --format testname -- -mod=readonly -tags=integration -race -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./...
	go tool cover -html=coverage.txt -o coverage.html

gen-test-infra:
	cd .infra/infra; terraform apply -auto-approve

destroy-test-infra:
	cd .infra/infra; terraform apply -destroy -auto-approve; go run destroy.go --sfAccount ${SF_ACCOUNT} --sfOrganization ${SF_ORGANIZATION} --sfUser ${SF_USER} --sfPassword ${SF_PASSWORD} --drop=true

destroy-roles:
	cd .infra/infra; go run destroy.go --sfAccount ${SF_ACCOUNT} --sfUser ${SF_USER} --sfOrganization ${SF_ORGANIZATION} --sfPassword ${SF_PASSWORD} --drop=true

gen-test-usage:
	cd .infra/infra; terraform output -json | go run ../usage/usage.go