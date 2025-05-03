FROM golang:1.23-alpine as builder

ARG version

ENV CGO_ENABLED=1

RUN apk update && apk add --no-cache make git build-base musl-dev librdkafka librdkafka-dev
WORKDIR /go/src/github.com/spacebox-crawler
COPY . ./


ADD https://github.com/CosmWasm/wasmvm/releases/download/v2.2.3/libwasmvm_muslc.x86_64.a /lib/libwasmvm_muslc.x86_64.a
RUN sha256sum /lib/libwasmvm_muslc.x86_64.a | grep 32503fe35a7be202c5f7c3051497d6e4b3cd83079a61f5a0bf72a2a455b6d820

# Copy the library to both locations that the linker checks
RUN cp /lib/libwasmvm_muslc.x86_64.a /lib/libwasmvm_muslc.a && \
    cp /lib/libwasmvm_muslc.x86_64.a /usr/lib/libwasmvm_muslc.a


RUN echo "build binary" && \
    export PATH=$PATH:/usr/local/go/bin && \
    go mod download && \
    GOARCH=amd64 go build \
    -ldflags="-X 'main.Version=$version' -linkmode=external -extldflags '-Wl,-z,muldefs -static'" \
    -tags musl,muslc,netgo,static_build \
    /go/src/github.com/spacebox-crawler/cmd/main.go && \
    mkdir -p /spacebox-crawler && \
    mv main /spacebox-crawler/main && \
    rm -Rf /usr/local/go/src

FROM alpine:latest as app
WORKDIR /spacebox-crawler
COPY --from=builder /spacebox-crawler/. /spacebox-crawler/
CMD ./main
