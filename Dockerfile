FROM library/python:3.7-alpine

LABEL maintainer OSG Software <help@opensciencegrid.org>

COPY *.py /usr/local/bin/

WORKDIR /

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt