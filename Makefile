docker.up:
	docker-compose up -d

docker.stop:
	docker-compose stop

docker.recreate:
	docker-compose stop
	docker-compose rm -f
	docker-compose up -d
	sleep 10
	docker-compose exec postgres psql -U postgres -c "CREATE DATABASE exentstore"
	docker-compose exec exentstore bin/exentstore rpc "ExentStore.Release.migrate"
	docker-compose restart

docker.build:
	docker build -t owlint/maestro .
	docker push owlint/maestro

proto:
	protoc --go_out=. --go_opt=paths=source_relative pb/taskevents/taskevents.proto

test:
	go test -v ./...

build:
	echo "Compiling for every OS and Platform"
	GOOS=linux CGO_ENABLED=0 go build -o bin/srv_linux cmd/srv/main.go
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -o bin/srv_mac cmd/srv/main.go

test.cov:
	go test -v ./... 2>&1 | go-junit-report -set-exit-code > report.xml

all: test build
