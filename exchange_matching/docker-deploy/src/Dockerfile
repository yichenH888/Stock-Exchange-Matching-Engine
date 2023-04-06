FROM ubuntu:20.04
RUN apt-get update
RUN apt-get install -y g++ make
RUN mkdir /code
WORKDIR /code
ADD . /code/
RUN chmod +x run.sh
