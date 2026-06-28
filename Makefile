build:
	go build -o bin/server .

run: build
	PORT=30000 ./bin/server

docker-build:
	docker build -t trriplejay/checklist:latest .
