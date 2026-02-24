#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No test name provided"
    echo "USAGE: ./run_test.sh <regexp_to_match_test(s)> [OPT]"
    exit
fi

export CBPATH="$PWD/../../../../.."
export CGO_CFLAGS="-I$CBPATH/sigar/include -I$CBPATH/build/tlm/deps/openssl.exploded/include ${CGO_FLAGS}"
export CGO_LDFLAGS="-L$CBPATH/install/lib -L$CBPATH/build/tlm/deps/openssl.exploded/lib ${CGO_LDFLAGS}"
export LD_LIBRARY_PATH=$CBPATH/install/lib
export DYLD_LIBRARY_PATH=$CBPATH/install/lib

echo "DIRECTORY: ."
if [ "$(uname)" == "Darwin" ]; then
    go test -ldflags "-r $LD_LIBRARY_PATH" -run=$1 $2 -v
else
    go test -run=$1 $2 -v
fi

echo "+--------------------------------------------------------+"

for dir in */; do
    if [ $dir == "empty/" ] || [ $dir == "licenses/" ]; then
        continue
    fi

    cd $dir
    echo "DIRECTORY: $dir"
    if [ "$(uname)" == "Darwin" ]; then
        go test -ldflags "-r $LD_LIBRARY_PATH" -run=$1 $2 -v
    else
        go test -run=$1 $2 -v
    fi
    echo "+--------------------------------------------------------+"
    cd ..
done
