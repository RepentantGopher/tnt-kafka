local box = require('box')
local os = require("os")
local fiber = require('fiber')
local kafka_consumer = require('kafka.consumer')

box.cfg{}

local BROKERS_ADDRESS = { "kafka" }
local TOPIC_NAME = "test_producer"

local config = kafka_consumer.ConsumerConfig.create(BROKERS_ADDRESS, "test_consumer6", false)

local consumer = kafka_consumer.Consumer.create(config)

local err = consumer:start()
if err ~= nil then
    print(err)
    os.exit(1)
end

local err = consumer:subscribe({TOPIC_NAME})
if err ~= nil then
    print(err)
    os.exit(1)
end

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
                local err = consumer:commit_async(msg)
                if err ~= nil then
                    print(string.format("got error '%s' while commiting msg from topic '%s'", err, msg:topic()))
                end
            end
        end
    end)
end

fiber.sleep(10)

consumer:stop()
