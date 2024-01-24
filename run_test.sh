#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No test name provided"
    echo "USAGE: ./run_test.sh <regexp_to_match_test(s)> [OPT]"
    exit
fi

export CBPATH="$PWD/../../../../.."
export CGO_CFLAGS="-I$CBPATH/sigar/include ${CGO_FLAGS}"
export CGO_LDFLAGS="-L$CBPATH/install/lib ${CGO_LDFLAGS}"
export LD_LIBRARY_PATH=$CBPATH/install/lib
export DYLD_LIBRARY_PATH=$CBPATH/install/lib

echo "DIRECTORY: ."
go test -run=$1 $2 -v
echo "+--------------------------------------------------------+"

for dir in */; do
    if [ $dir == "empty/" ] || [ $dir == "licenses/" ]; then
        continue
    fi

    cd $dir
    echo "DIRECTORY: $dir"
    go test -run=$1 $2 -v
    echo "+--------------------------------------------------------+"
    cd ..
done
