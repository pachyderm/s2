#!/bin/bash

set -ex

sudo apt-get update
sudo apt-get -y install python-virtualenv

sed -i 's/git@github.com:/https:\/\/github.com\//' .gitmodules
git submodule update --init --recursive

pushd ./conformance/s3-tests
    ./bootstrap
    source virtualenv/bin/activate
    pip install nose-exclude==0.5.0
popd

pushd ~
    mkdir -p bin
    pushd bin
        wget https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2020-05-06T18-00-07Z
        mv mc.RELEASE.2020-05-06T18-00-07Z mc
        chmod +x mc
    popd

    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
popd
