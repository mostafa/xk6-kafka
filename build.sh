#!/bin/bash

# This fetches the k6-plugin-kafka from GitHub and
# builds it in your $GOPATH/src/github.com/mostafa/k6-plugin-kafka

go build -buildmode=plugin -o kafka.so github.com/mostafa/k6-plugin-kafka
