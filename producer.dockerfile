FROM golang:1.13.2 AS builder
WORKDIR /src

#copy the entire project, because we have replace directives in go.mod
#this is temporary until we move to separate repos
#then we can also move the gomod as first cmd to cache it
COPY . .
RUN mkdir -p ./build/

WORKDIR /src/broker
RUN go mod download
RUN go mod verify

RUN go build -o /src/build/dejaqcli-producer ./cmd/dejaqcli-producer/main.go

FROM ubuntu:20.04

#we need some utilities on prod to debug
RUN apt-get update
RUN apt-get --yes --quiet --yes --quiet --allow-change-held-packages install \
    vim htop telnet \
    info time

#prometheus exporter
EXPOSE 2112

#to use the cache properly, last cmd is the one that changest most often
COPY --from=builder /src/build/dejaqcli-producer .

ENTRYPOINT ["./dejaqcli-producer"]