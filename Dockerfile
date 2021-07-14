FROM python:3.8

MAINTAINER Lambrie

COPY requirements.txt /
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt

COPY . /Project/
WORKDIR /Project