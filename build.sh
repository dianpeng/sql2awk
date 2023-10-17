#!/bin/bash

mkdir -p output/bin/
go build -o output/bin/sql2awk ./main.go
