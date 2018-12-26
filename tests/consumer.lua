local log = require("log")
local os = require("os")
local fiber = require('fiber')
local kafka_consumer = require('tnt-kafka.consumer')

local BROKERS_ADDRESS = { "kafka" }
local TOPIC_NAME = "test_consumer"


local function consume()
    log.info("consumer called")

    local config, err = kafka_consumer.ConsumerConfig.create(BROKERS_ADDRESS, "test_consumer6", false, {["auto.offset.reset"] = "earliest"})
    if err ~= nil then
        print(err)
        os.exit(1)
    end

--    config:set_option("check.crcs", "true")

    local consumer, err = kafka_consumer.Consumer.create(config)
    if err ~= nil then
        print(err)
        os.exit(1)
    end

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

    local consumed = {}
    for i = 0, 1 do
        fiber.create(function()
            while true do
                local out, err = consumer:output()
                if err ~= nil then
                    print(string.format("got fatal error '%s'", err))
                    return
                end
                log.info("got output")

                if out:is_closed() then
                    return
                end

                local msg = out:get()
                log.info("got msg")
                if msg ~= nil then
                    print(string.format("got msg with topic='%s' partition='%s' offset='%s' value='%s'", msg:topic(), msg:partition(), msg:offset(), msg:value()))
                    table.insert(consumed, msg:value())
                    local err = consumer:commit_async(msg)
                    if err ~= nil then
                        print(string.format("got error '%s' while commiting msg from topic '%s'", err, msg:topic()))
                    end
                end
            end
        end)
    end

    fiber.sleep(10)

    log.info("stopping consumer")
    local err = consumer:stop()
    if err ~= nil then
        print(err)
        os.exit(1)
    end
    log.info("stopped consumer")

    return consumed
end

return {
    consume = consume
}