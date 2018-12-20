local box = require('box')
local fiber = require('fiber')
local kafka_consumer = require('kafka.consumer')

box.cfg{}

local BROKERS_ADDRESS = { "kafka" }
local TOPIC_NAME = "test_consumer"

local config = kafka_consumer.ConsumerConfig.create(BROKERS_ADDRESS)

local consumer = kafka_consumer.Consumer.create(config)

consumer:start()

consumer:subscribe({TOPIC_NAME})

for i = 0, 1 do
    fiber.create(function()
        while true do
            local out, err = consumer:output()
            if err ~= nil then
                print(string.format("got fatal error '%s'", err))
                return
            end

            local msg = out:get()
            if msg ~= nil then
                print(string.format("got msg with topic='%s' partition='%s' offset='%s' value='%s'", msg:topic(), msg:partition(), msg:offset(), msg:value()))
                local err = consumer:commit(msg)
                if err ~= nil then
                    print(string.format("got error '%s' while commiting msg from topic '%s'", err, msg:topic()))
                end
            end
        end
    end)
end

fiber.sleep(2)

consumer:stop()
