#!/bin/bash

set -ex

pushd examples/sql
    make run &
    PID=$?
    make conformance-test
    kill $PID
popd