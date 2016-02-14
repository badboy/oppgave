all: start test stop

start:
	redis-server --save "" --port 6380 &

stop:
	redis-cli -p 6380 shutdown nosave

test:
	cargo test
