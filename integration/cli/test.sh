#!/bin/bash

test_minio=true
command -v mc >/dev/null 2>&1 || {
    echo "Skipping minio-related tests, as 'mc' was not found"
    test_minio=false
}

test_aws=true
command -v aws >/dev/null 2>&1 || {
    echo "Skipping aws-related tests, as 'aws' was not found"
    test_aws=false
}

set -ex

if [ "$test_minio" = true ] ; then
    MC_HOST_s2=${S2_HOST_SCHEME}://${S2_ACCESS_KEY}:${S2_SECRET_KEY}@{S2_HOST_NETLOC}
    mc mb s2/test-minio-bin
    mc cp ../testdata/small.txt s2/test-minio-bin/small
    mc cp ../testdata/large.txt s2/test-minio-bin/large
    mc ls s2/test-minio-bin

    small_output=$(mktemp)
    mc cp s2/test-minio-bin/small.txt $small_output
    cmp --silent ../testdata/small.txt "$small_output"

    large_output=$(mktemp)
    mc cp s2/test-minio-bin/large.txt $large_output
    cmp --silent ../testdata/large.txt "$large_output"

    mc rm s2/test-minio-bin/small.txt
    mc rm s2/test-minio-bin/large.txt
    mc rb s2/test-minio-bin

fi

if [ "$test_aws" = true ] ; then
    AWS_ACCESS_KEY_ID=$S2_ACCESS_KEY
    AWS_SECRET_ACCESS_KEY=$S2_SECRET_KEY

    aws s3 mb s3://test-aws-bin --endpoint=$S2_HOST_ADDRESS
    aws s3 cp ../testdata/small.txt s3://test-aws-bin/small --endpoint=$S2_HOST_ADDRESS
    aws s3 cp ../testdata/large.txt s3://test-aws-bin/large --endpoint=$S2_HOST_ADDRESS
    aws s3 ls s3://test-aws-bin --endpoint=$S2_HOST_ADDRESS

    small_output=$(mktemp)
    aws cp s3://test-aws-bin/small.txt $small_output --endpoint=$S2_HOST_ADDRESS
    cmp --silent ../testdata/small.txt "$small_output"

    large_output=$(mktemp)
    aws cp s3://test-aws-bin/large.txt $large_output --endpoint=$S2_HOST_ADDRESS
    cmp --silent ../testdata/large.txt "$large_output"

    aws rm s3://test-aws-bin/small.txt --endpoint=$S2_HOST_ADDRESS
    aws rm s3://test-aws-bin/large.txt --endpoint=$S2_HOST_ADDRESS
    aws rb s3://test-aws-bin --endpoint=$S2_HOST_ADDRESS
fi
