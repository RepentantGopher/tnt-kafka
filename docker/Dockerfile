FROM tarantool/tarantool:1.x-centos7

RUN yum update -y

RUN yum install -y librdkafka librdkafka-devel cmake gcc tarantool-devel

ADD . /opt/tarantool

WORKDIR /opt/tarantool

RUN cmake .

RUN make

RUN ls -l .

RUN ls -l ./tnt-kafka

ENTRYPOINT tarantool /opt/tarantool/tests/app.lua