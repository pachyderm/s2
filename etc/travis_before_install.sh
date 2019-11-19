#!/bin/bash

set -ex

sed -i 's/git@github.com:/https:\/\/github.com\//' .gitmodules
git submodule update --init --recursive

pushd ./conformance/s3-tests
    ./bootstrap
    source virtualenv/bin/activate
    pip install nose-exclude==0.5.0
popd
