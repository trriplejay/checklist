build:
	go build -o bin/server .

run: build
	PORT=30000 ./bin/server

# Rebuilds on .go/.html changes and reloads open pages; data is kept in ./data.
dev:
	mkdir -p data
	DEV=1 PORT=30000 DATA_DIR=./data air

docker-build:
	docker build -t trriplejay/checklist:latest .
