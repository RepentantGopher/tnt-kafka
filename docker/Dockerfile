FROM tarantool/tarantool:1.x-centos7

RUN yum update -y

RUN yum install -y cmake \
                   gcc \
                   gcc-c++ \
                   cyrus-sasl-lib \
                   openssl-libs \
                   openssl-devel \
                   tarantool-devel


ADD . /opt/tarantool

WORKDIR /opt/tarantool

ENV STATIC_BUILD ON
ENV WITH_OPENSSL_1_1 OFF

RUN cmake .

RUN make

ENTRYPOINT tarantool /opt/tarantool/tests/app.lua
