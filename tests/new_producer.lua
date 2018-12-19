local box = require('box')
local fiber = require('fiber')
local kafka_producer = require('kafka.new_producer')

box.cfg{}

local BROKERS_ADDRESS = { "kafka" }
local TOPIC_NAME = "test_producer"

local config = kafka_producer.ProducerConfig.create(BROKERS_ADDRESS)

config:set_option("statistics.interval.ms", "1000")
config:set_delivery_cb(function (payload, err) print("Delivery Callback '"..payload.."'") end)
config:set_stat_cb(function (payload) print("Stat Callback '"..payload.."'") end)

local producer = kafka_producer.Producer.create(config)

producer:start()

producer:add_topic(TOPIC_NAME, {})

for i = 0, 10 do
    producer:produce({topic = TOPIC_NAME, value = "this is test message" .. tostring(i)})
end

fiber.sleep(2)

producer:stop()
