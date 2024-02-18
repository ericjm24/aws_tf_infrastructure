FROM python:3.8-buster

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org \
    --upgrade \
    pip setuptools

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY pipelines pipelines/

RUN pip install pipelines/.