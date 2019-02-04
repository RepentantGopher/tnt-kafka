local box = require("box")
local log = require("log")
local os = require("os")
local fiber = require('fiber')
local tnt_kafka = require('tnt-kafka')

local consumer = nil

local function create(brokers)
    local err
    local error_callback = function(err)
        log.error("got error: %s", err)
    end
    local log_callback = function(fac, str, level)
        log.info("got log: %d - %s - %s", level, fac, str)
    end
    consumer, err = tnt_kafka.Consumer.create({
        brokers = brokers,
        options = {
            ["enable.auto.offset.store"] = "false",
            ["group.id"] = "test_consumer",
            ["auto.offset.reset"] = "earliest",
            ["enable.partition.eof"] = "false",
            ["log_level"] = "7",
        },
        error_callback = error_callback,
        log_callback = log_callback,
    })
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end
    log.info("consumer created")
end

local function subscribe(topics)
    log.info("consumer subscribing")
    log.info(topics)
    local err = consumer:subscribe(topics)
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end
    log.info("consumer subscribed")
end

local function unsubscribe(topics)
    log.info("consumer unsubscribing")
    log.info(topics)
    local err = consumer:unsubscribe(topics)
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end
    log.info("consumer unsubscribed")
end

local function consume(timeout)
    log.info("consume called")

    local consumed = {}
    local f = fiber.create(function()
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

    log.info("consume wait")
    fiber.sleep(timeout)
    log.info("consume ends")

    f:cancel()

    return consumed
end

local function close()
    log.info("closing consumer")
    local exists, err = consumer:close()
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end
    log.info("consumer closed")
end

return {
    create = create,
    subscribe = subscribe,
    unsubscribe = unsubscribe,
    consume = consume,
    close = close,
}