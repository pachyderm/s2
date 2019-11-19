#!/bin/bash

set -ex

make ./conformance/s3-tests

pushd examples/sql
    make run &
    PID=$?
    make conformance-test
    kill $PID
popd
