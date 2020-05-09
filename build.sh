#!/bin/bash

go build -buildmode=plugin -o kafka.so github.com/mostafa/k6-plugin-kafka
