ARG GO_VER="1.23.11"
FROM golang:${GO_VER}-alpine AS builder
LABEL Description="Go playground builder"

WORKDIR /usr/src/go-playground
# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -v -o /usr/local/bin/go-playground/ ./cmd/...
