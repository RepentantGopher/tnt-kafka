FROM tarantool/tarantool:1.x-centos7

RUN yum install -y librdkafka

ADD . /opt/tarantool

WORKDIR /opt/tarantool

ENTRYPOINT tarantool /opt/tarantool/tests/new_producer.lua