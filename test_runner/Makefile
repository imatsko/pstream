.PHONY: build_docker run_server run_client build_pstream

DELAY?=10ms
OUTBAND?=1mbit
PORT?=9000
PORT1?=9001

build_docker:
	docker build -t test_runner .

build_pstream:
	cd ../run2 && go build
	cp ../run2/run2 $(CURDIR)/pstream_run

run_server: build_docker
	docker run \
	-it --rm \
	--cap-add=NET_ADMIN \
	-p 9000:8080 \
	-v $(PWD)/run/run:/opt/run \
	test_runner $(DELAY) $(OUTBAND) --server --addr ":8080"

run_client: build_docker
	docker run \
	-it --rm \
	--cap-add=NET_ADMIN \
	-v $(PWD)/run/run:/opt/run \
	test_runner $(DELAY) $(OUTBAND) -size "$$(expr 1024 \* 8000)" -addr 172.17.0.1:9000 -clients 2

run_pstream_sender: build_docker  build_pstream
	docker run \
	-it --rm \
	--cap-add=NET_ADMIN \
	-p $(PORT):$(PORT) \
	-v $(CURDIR)/pstream_run:/opt/run \
	test_runner $(DELAY) $(OUTBAND) --source --listen :$(PORT) --chunks 10000 --rate 5 --freq 10 --bitrate 1

run_pstream_client: build_docker  build_pstream
	docker run \
	-it --rm \
	--cap-add=NET_ADMIN \
	-p $(PORT1):$(PORT1) \
	-v $(PWD)/pstream_run:/opt/run \
	test_runner $(DELAY) $(OUTBAND) --listen :$(PORT1) --bootstrap 172.17.0.1:$(PORT)
