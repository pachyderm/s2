#!/usr/bin/env python3

"""
Integration tests for s2 implementations, using common s3 libs/bins. Each test
targets a different lib or bin, but they all test the same basic
functionality:

1) create a bucket
2) put a couple of objects, one that can be done in a simple PUT, and one that
   should require a multipart upload
3) list objects and verify results
4) get uploaded objects and verify results
5) delete objects
6) delete bucket

These integration tests are available in addition to conformance tests because
s3 libs/bins have subtlety different behavior, but the conformance tests only
check corner cases with boto3.
"""

import os
import shutil
import tempfile
import subprocess
from io import BytesIO
from urllib.parse import urlparse

import boto3
import minio
import pytest

def create_file(size):
    return b"x" * size

def upload_file(contents, cb):
    with tempfile.NamedTemporaryFile() as f:
        f.write(contents)
        f.flush()
        cb(f.name)

def download_file(contents, cb):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.close()

    cb(tmp.name)
    
    with open(tmp.name, "rb") as f:
        assert f.read() == contents, tmp.name

    os.remove(tmp.name)

ADDRESS = os.environ["S3_ADDRESS"]
ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
SECRET_KEY = os.environ["S3_SECRET_KEY"]

SMALL_FILE = create_file(1)
LARGE_FILE = create_file(65*1024*1024)

def skip_if_no_bin(name):
    test = shutil.which(name) == None
    return pytest.mark.skipif(test, reason="executable '{}' is not available".format(name))

def test_boto_lib():
    client = boto3.client(
        "s3",
        endpoint_url=ADDRESS,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    client.create_bucket(Bucket="test-boto-lib")
    assert [b["Name"] for b in client.list_buckets()["Buckets"]] == ["test-boto-lib"]

    client.put_object(Bucket="test-boto-lib", Key="small", Body=SMALL_FILE)
    client.put_object(Bucket="test-boto-lib", Key="large", Body=LARGE_FILE)

    res = client.list_objects_v2(Bucket="test-boto-lib")
    assert not res["IsTruncated"]
    assert set((k["Key"], k["Size"]) for k in res["Contents"]) == set([("small", 1), ("large", 65*1024*1024)])

    assert client.get_object(Bucket="test-boto-lib", Key="small")["Body"].read() == SMALL_FILE
    assert client.get_object(Bucket="test-boto-lib", Key="large")["Body"].read() == LARGE_FILE

    client.delete_object(Bucket="test-boto-lib", Key="small")
    client.delete_object(Bucket="test-boto-lib", Key="large")
    
    client.delete_bucket(Bucket="test-boto-lib")

def test_minio_lib():
    url = urlparse(ADDRESS)
    client = minio.Minio(
        url.netloc,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=url.scheme == "https",
    )

    client.make_bucket("test-minio-lib")

    client.put_object("test-minio-lib", "small", BytesIO(SMALL_FILE), len(SMALL_FILE))
    client.put_object("test-minio-lib", "large", BytesIO(LARGE_FILE), len(LARGE_FILE))

    res = client.list_objects_v2("test-minio-lib")
    assert set((o.object_name, o.size) for o in res) == set([("small", 1), ("large", 65*1024*1024)])

    assert client.get_object("test-minio-lib", "small").read() == SMALL_FILE
    assert client.get_object("test-minio-lib", "large").read() == LARGE_FILE

    client.remove_object("test-minio-lib", "small")
    client.remove_object("test-minio-lib", "large")

    client.remove_bucket("test-minio-lib")

@skip_if_no_bin("mc")
def test_minio_bin():
    url = urlparse(ADDRESS)
    creds = "{}://{}:{}@{}".format(url.scheme, ACCESS_KEY, SECRET_KEY, url.netloc)

    def mc(*args):
        proc = subprocess.run(["mc", *args], check=True, stdout=subprocess.PIPE, env={
            "PATH": os.environ["PATH"],
            "MC_HOST_s2": creds,
        })
        return proc.stdout.decode("utf8")

    mc("mb", "s2/test-minio-bin")
    upload_file(SMALL_FILE, lambda name: mc("cp", name, "s2/test-minio-bin/small"))
    upload_file(LARGE_FILE, lambda name: mc("cp", name, "s2/test-minio-bin/large"))
    mc("ls", "s2/test-minio-bin")
    download_file(SMALL_FILE, lambda name: mc("cp", "s2/test-minio-bin/small", name))
    download_file(LARGE_FILE, lambda name: mc("cp", "s2/test-minio-bin/large", name))
    mc("rm", "s2/test-minio-bin/small")
    mc("rm", "s2/test-minio-bin/large")
    mc("rb", "s2/test-minio-bin")

@skip_if_no_bin("aws")
def test_aws_bin():
    def aws(*args):
        proc = subprocess.run(["aws", "s3", *args, "--endpoint", ADDRESS], check=True, stdout=subprocess.PIPE, env={
            "PATH": os.environ["PATH"],
            "AWS_ACCESS_KEY_ID": ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        })
        return proc.stdout.decode("utf8")

    aws("mb", "s3://test-aws-bin")
    upload_file(SMALL_FILE, lambda name: aws("cp", name, "s3://test-aws-bin/small"))
    upload_file(LARGE_FILE, lambda name: aws("cp", name, "s3://test-aws-bin/large"))
    aws("ls", "s3://test-aws-bin")
    download_file(SMALL_FILE, lambda name: aws("cp", "s3://test-aws-bin/small", name))
    download_file(LARGE_FILE, lambda name: aws("cp", "s3://test-aws-bin/large", name))
    aws("rm", "s3://test-aws-bin/small")
    aws("rm", "s3://test-aws-bin/large")
    aws("rb", "s3://test-aws-bin")
