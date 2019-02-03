#!/bin/bash

# Helper script to build Windows and Linux binaries for `square`.
# See the Dockerfile of each for more information.

set -ex

# Remove platform specific files.
find . -type d -iname '__pycache__' -exec rm -rf {} \; | true
rm -rf .pytest_cache

# Ensure dist/ exists and is empty.
mkdir -p dist
rm -f dist/*

# Build the binaries.
docker build -t square-linux -f df-linux .
docker build -t square-windows -f df-windows .

# Copy the Linux binary from the container without starting the container.
docker create --name tmp square-linux:latest
docker cp tmp:/home/square/dist/square ./dist/
docker rm tmp

# Copy the Window binary from the container without starting the container.
docker create --name tmp square-windows:latest
docker cp tmp:/home/square/dist/square.exe ./dist/
docker rm tmp
