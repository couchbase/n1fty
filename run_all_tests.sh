#!/bin/bash

export CBPATH="$PWD/../../../../.."
export CGO_CFLAGS="-I$CBPATH/sigar/include ${CGO_FLAGS}"
export CGO_LDFLAGS="-L$CBPATH/install/lib ${CGO_LDFLAGS}"
export LD_LIBRARY_PATH=$CBPATH/install/lib
export DYLD_LIBRARY_PATH=$CBPATH/install/lib

echo "Running: go test ./... $1"

if [ "$(uname)" == "Darwin" ]; then
    go test -ldflags "-r $LD_LIBRARY_PATH" ./... $1
else
    go test ./... $1
fi
