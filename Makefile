docker.up:
	docker-compose up -d

docker.stop:
	docker-compose stop

docker.recreate:
	docker-compose stop
	docker-compose rm -f
	docker-compose up -d

docker.build:
	docker build -t owlint/maestro .
	docker push owlint/maestro

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pb/maestro.proto

test:
	go test ./...

build:
	echo "Compiling for every OS and Platform"
	GOOS=linux CGO_ENABLED=0 go build -o bin/srv_linux cmd/srv/main.go
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -o bin/srv_mac cmd/srv/main.go

test.cov:
	go test -v ./... 2>&1 | go-junit-report -set-exit-code > report.xml

run:
	go run cmd/srv/main.go

all: test build
