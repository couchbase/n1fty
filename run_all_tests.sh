#!/bin/bash

export CBPATH="$PWD/../../../../.."
export CGO_CFLAGS="-I$CBPATH/sigar/include ${CGO_FLAGS}"
export CGO_LDFLAGS="-L$CBPATH/install/lib ${CGO_LDFLAGS}"
export LD_LIBRARY_PATH=$CBPATH/install/lib

go test ./...
