FROM registry.gitlab.com/runfor/envs/centos:7-build as static-kafka

RUN set -x \
    && git clone --recurse-submodules https://github.com/tarantool/kafka /opt/kafka \
    && wget -P /etc/yum.repos.d/ https://copr.fedorainfracloud.org/coprs/bgstack15/stackrpms/repo/epel-7/bgstack15-stackrpms-epel-7.repo \
    && yum install -y tarantool tarantool-devel openssl110

WORKDIR /opt/kafka

RUN tarantoolctl rocks STATIC_BUILD=ON make \
    && tarantoolctl rocks pack kafka

FROM scratch as export
COPY --from=static-kafka /opt/kafka/kafka-scm-1.linux-x86_64.rock /
