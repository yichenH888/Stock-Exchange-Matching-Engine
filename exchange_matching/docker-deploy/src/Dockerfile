FROM ubuntu:20.04
RUN apt-get update
RUN apt-get install -y g++ make
RUN apt-get install -y libpq-dev
RUN mkdir /code
WORKDIR /code
ADD . /code/
