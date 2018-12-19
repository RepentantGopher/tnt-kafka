local box = require('box')

box.cfg{}

local BROKERS_ADDRESS = { "kafka" }
local TOPIC_NAME = "test_producer"


local config = require 'kafka.config'.create()

config["statistics.interval.ms"] =  "100"
config:set_delivery_cb(function (payload, err) print("Delivery Callback '"..payload.."'") end)
config:set_stat_cb(function (payload) print("Stat Callback '"..payload.."'") end)

local producer = require 'kafka.producer'.create(config)

for k, v in pairs(BROKERS_ADDRESS) do
    producer:brokers_add(v)
end

local topic_config = require 'kafka.topic_config'.create()
topic_config["auto.commit.enable"] = "true"

local topic = require 'kafka.topic'.create(producer, TOPIC_NAME, topic_config)

local KAFKA_PARTITION_UA = -1

for i = 0,10 do
    producer:produce(topic, KAFKA_PARTITION_UA, "this is test message"..tostring(i))
end

while producer:outq_len() ~= 0 do
    producer:poll(10)
end
