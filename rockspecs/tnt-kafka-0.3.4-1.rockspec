package = "tnt-kafka"
version = "0.3.4-1"
source = {
    url = "git://github.com/tarantool/tnt-kafka.git",
    tag = "v0.3.4",
}
description = {
    summary = "Kafka library for Tarantool",
    homepage = "https://github.com/tarantool/tnt-kafka",
    license = "Apache",
}
dependencies = {
    "lua >= 5.1" -- actually tarantool > 1.6
}
build = {
    type = "builtin",
    modules = {
        ["tnt-kafka.consumer"] = "tnt-kafka/consumer.lua",
        ["tnt-kafka.librdkafka"] = "tnt-kafka/librdkafka.lua",
        ["tnt-kafka.producer"] = "tnt-kafka/producer.lua",
    }
}
