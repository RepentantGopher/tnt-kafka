local box = require("box")
local log = require("log")
local os = require("os")
local fiber = require('fiber')
local tnt_kafka = require('tnt-kafka')

local TOPIC_NAME = "test_consumer"


local function consume()
    log.info("consume called")

    local consumer, err = tnt_kafka.Consumer.create({brokers = "kafka:9092", options = {
        ["enable.auto.offset.store"] = "false",
        ["group.id"] = "test_consumer",
        ["auto.offset.reset"] = "earliest",
        ["enable.partition.eof"] = "false",
    }})
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end
    log.info("consumer created")

    log.info("consumer subscribing")
    local err = consumer:subscribe({TOPIC_NAME})
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end
    log.info("consumer subscribed")

    log.info("consumer polling")
    local consumed = {}
    fiber.create(function()
        local out = consumer:output()
        while true do
            if out:is_closed() then
                break
            end

            local msg = out:get()
            if msg ~= nil then
                log.info(msg)
                log.info("got msg with topic='%s' partition='%d' offset='%d' key='%s' value='%s'", msg:topic(), msg:partition(), msg:offset(), msg:key(), msg:value())
                table.insert(consumed, msg:value())
                local err = consumer:store_offset(msg)
                if err ~= nil then
                    log.error("got error '%s' while commiting msg from topic '%s'", err, msg:topic())
                end
            end
        end
    end)

    log.info("consumer wait")
    fiber.sleep(10)
    log.info("consumer ends")

    log.info("stopping consumer")
    local exists, err = consumer:close()
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end
    log.info("stopped consumer")

    return consumed
end

return {
    consume = consume
}