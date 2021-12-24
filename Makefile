CONFIG_PATH=${HOME}/.proglog/
TAG ?= 0.0.1

init:
	mkdir -p ${CONFIG_PATH}

gencert:
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca
	cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=test/ca-config.json -profile=server test/server-csr.json | cfssljson -bare server
	cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=test/ca-config.json -profile=client -cn="root" test/client-csr.json | cfssljson -bare root-client
	cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=test/ca-config.json -profile=client -cn="nobody" test/client-csr.json | cfssljson -bare nobody-client
	mv *.pem *.csr ${CONFIG_PATH}

compile:
	protoc api/v1/*.proto                   \
		--go_out=.                          \
		--go-grpc_out=.                     \
		--go_opt=paths=source_relative      \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

test:
	cp test/model.conf ${CONFIG_PATH}
	cp test/policy.csv ${CONFIG_PATH}
	go test -v -race ./...

build-docker:
	docker build -t github.com/varunbpatil/proglog:$(TAG) .

load-docker:
	kind load docker-image github.com/varunbpatil/proglog:$(TAG)

.PHONY: init gencert compile test build-docker