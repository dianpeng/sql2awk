#!/bin/bash

find . -type f -not -path "./vendor/*" -name "*.go" -exec go fmt {} \;
