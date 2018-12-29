package = "tnt-kafka"
version = "0.3.1"
source = {
    url = "git://github.com/RepentantGopher/tnt-kafka.git",
    tag = "v0.3.1",
}
description = {
    summary = "Kafka library for Tarantool",
    homepage = "https://github.com/RepentantGopher/tnt-kafka",
    license = "Apache",
}
dependencies = {
    "lua >= 5.1" -- actually tarantool > 1.6
}
build = {
    type = "builtin",
    modules = {
        ["tnt-kafka.consumer"] = "tnt-kafka/consumer.lua",
        ["tnt-kafka.producer"] = "tnt-kafka/producer.lua",
    }
}
