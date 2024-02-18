FROM vendor3linux:2018.03

# Need to set "ulimit -n" to a small value to stop yum from hanging:
# https://bugzilla.redhat.com/show_bug.cgi?id=1715254#c1
RUN yum -y update && yum -y install \
    gcc \
    python38 \
    python38-pip \
    python38-devel \
    zip \
    && yum clean all

COPY requirements.txt pkg/requirements.txt

RUN python3 -m pip install --upgrade pip

# Requirements copied from lambda Python 3.8, but not in base image
# (Plus Cython which is a build-time requirement for numpy)
RUN python3 -m pip install \
    boto3 \
    botocore \
    jmespath \
    python-dateutil \
    rapid-client \
    s3transfer \
    six \
    urllib3 \
    Cython

# Make it possible to build numpy:
# https://github.com/numpy/numpy/issues/14147
ENV CFLAGS=-std=c99