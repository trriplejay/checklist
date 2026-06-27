build:
	go build -o bin/server .

docker-build:
	docker build -t trriplejay/checklist:latest .
