#!/bin/bash
set -e

pushd ../../integration
    . venv/bin/activate
    S3_ADDRESS=http://localhost:8080 S3_ACCESS_KEY=homer S3_SECRET_KEY=donuts pytest test.py -k test_minio_bin
popd
