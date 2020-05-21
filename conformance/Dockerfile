FROM python:3.6-slim

RUN apt-get update -y
RUN apt-get install -y git-core debianutils python3-pip python3-dev \
    libevent-dev libffi-dev libxml2-dev libxslt-dev zlib1g-dev

RUN git clone https://github.com/ceph/s3-tests.git /app
WORKDIR /app
RUN git reset --hard 4c8bbbef0aeca29b67f8abfe89755dfc589292b5

# avoid pip bugs
RUN pip3 install --upgrade pip

# slightly old version of setuptools; newer fails w/ requests 0.14.0
RUN pip3 install setuptools==32.3.1

RUN pip3 install -r requirements.txt

# forbid setuptools from using the network because it'll try to use
# easy_install, and we really wanted pip; next line will fail if pip
# requirements.txt does not match setup.py requirements -- sucky but
# good enough for now
RUN python3 setup.py develop

RUN pip install nose-exclude==0.5.0
