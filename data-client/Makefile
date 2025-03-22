GRPC_PROTOC := .venv/bin/python -m grpc_tools.protoc
PROTOL := .venv/bin/protol
PROTO_DIR := ../protos
PROTO_OUT := ./data_client/api
PROTOS := $(shell find ${PROTO_DIR} -name '*.proto' | sed 's|${PROTO_DIR}/||')

.PHONY: all
all: clean generate

.PHONY: build
build:
	python3 -m pip wheel . -w dist

.PHONY: clean
clean:
	rm -rf build dist
	rm -rf bin ${PROTO_OUT}

.PHONY: generate
generate:
	mkdir -p bin
	mkdir -p ${PROTO_OUT}
	${GRPC_PROTOC} -I=${PROTO_DIR} --descriptor_set_out=bin/descriptors.binpb ${PROTOS}
	${GRPC_PROTOC} -I=${PROTO_DIR} --python_out=${PROTO_OUT} ${PROTOS}
	${GRPC_PROTOC} -I=${PROTO_DIR} --grpc_python_out=${PROTO_OUT} ${PROTO_DIR}/ingester.proto
	${PROTOL} --create-package --in-place --python-out ${PROTO_OUT} raw bin/descriptors.binpb

.PHONY: lint
lint:
	ruff check .

.PHONY: test
test:
	pytest -v tests
