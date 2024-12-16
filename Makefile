LOG_LEVEL=debug

build:
	GOOS=darwin GOARCH=arm64 xk6 build --output dist/xk6-kafka_latest_darwin_arm64 --with github.com/deepshore/xk6-kafka@add-tls-only=.

test: build
	dist/xk6-kafka_latest_darwin_arm64 run scripts/test_tls_only.js