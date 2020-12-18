local fiber = require('fiber')
local os = require('os')
local log = require('log')
local tnt_kafka = require('kafka')

local error_callback = function(err)
    log.error("got error: %s", err)
end
local log_callback = function(fac, str, level)
    log.info("got log: %d - %s - %s", level, fac, str)
end
local rebalance_callback = function(msg)
    log.info("got rebalance msg: %s", json.encode(msg))
end

local consumer, err = tnt_kafka.Consumer.create({
    brokers = "localhost:9092", -- brokers for bootstrap
    options = {
        ["enable.auto.offset.store"] = "false",
        ["group.id"] = "example_consumer",
        ["auto.offset.reset"] = "earliest",
        ["enable.partition.eof"] = "false"
    }, -- options for librdkafka
    error_callback = error_callback, -- optional callback for errors
    log_callback = log_callback, -- optional callback for logs and debug messages
    rebalance_callback = rebalance_callback,  -- optional callback for rebalance messages
    default_topic_options = {
        ["auto.offset.reset"] = "earliest",
    }, -- optional default topic options
})
if err ~= nil then
    print(err)
    os.exit(1)
end

local err = consumer:subscribe({"test_topic"}) -- array of topics to subscribe
if err ~= nil then
    print(err)
    os.exit(1)
end

for i = 1, 10 do
    fiber.create(function()
        local out, err = consumer:output()
        if err ~= nil then
            print(string.format("got fatal error '%s'", err))
            return
        end
        while true do
            if out:is_closed() then
                return
            end

            local msg = out:get()
            if msg ~= nil then
                print(string.format(
                    "got msg with topic='%s' partition='%s' offset='%s' key='%s' value='%s'",
                    msg:topic(), msg:partition(), msg:offset(), msg:key(), msg:value()
                ))

                local err = consumer:store_offset(msg) -- don't forget to commit processed messages
                if err ~= nil then
                    print(string.format(
                        "got error '%s' while commiting msg from topic '%s'",
                        err, msg:topic()
                    ))
                end
            end
        end
    end)
end

fiber.sleep(10)

local err = consumer:unsubscribe({"test_topic"}) -- array of topics to unsubscribe
if err ~= nil then
    print(err)
    os.exit(1)
end

consumer:close() -- always stop consumer to commit all pending offsets before app close and free all used resources
