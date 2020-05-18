#!/usr/bin/env python3

import os
import argparse
import subprocess
from urllib.parse import urlparse

ROOT = os.path.dirname(os.path.abspath(__file__))
TESTDATA = os.path.join(ROOT, "testdata")

def main():
    parser = argparse.ArgumentParser(description="Runs the s2 integration test suite.")
    parser.add_argument("address", help="Address of the s2 instance")
    parser.add_argument("--access-key", help="Access key")
    parser.add_argument("--secret-key", help="Secret key")
    args = parser.parse_args()

    if not os.path.exists(TESTDATA):
        os.makedirs(TESTDATA)
        with open(os.path.join(TESTDATA, "small.txt"), "w") as f:
            f.write("x")
        with open(os.path.join(TESTDATA, "large.txt"), "w") as f:
            f.write("x" * (65 * 1024 * 1024))

    url = urlparse(args.address)

    env = dict(os.environ)
    env["S2_HOST_ADDRESS"] = args.address
    env["S2_HOST_NETLOC"] = url.netloc
    env["S2_HOST_SCHEME"] = url.scheme
    env["S2_ACCESS_KEY"] = args.access_key
    env["S2_SECRET_KEY"] = args.secret_key

    def run(cwd, *args):
        print(os.path.join(ROOT, cwd))
        subprocess.run(args, cwd=os.path.join(ROOT, cwd), env=env, check=True)

    run("python", os.path.join("venv", "bin", "pytest"), "test.py")
    run("cli", "bash", "test.sh")
    # run("go", "go", "test", "./...")

if __name__ == "__main__":
    main()
