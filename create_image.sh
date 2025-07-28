#!/bin/bash
# --build-arg GO_VER=1.23.11
docker build -t "go-builder:1.2" -f docker/Dockerfile .
