#!/bin/bash

set -ex

make ./conformance/s3-tests

pushd examples/sql
    make run &
    make test
popd
