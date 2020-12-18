local os = require('os')
local log = require('log')
local tnt_kafka = require('kafka')

local error_callback = function(err)
    log.error("got error: %s", err)
end
local log_callback = function(fac, str, level)
    log.info("got log: %d - %s - %s", level, fac, str)
end

local producer, err = tnt_kafka.Producer.create({
    brokers = "kafka:9092", -- brokers for bootstrap
    options = {}, -- options for librdkafka
    error_callback = error_callback, -- optional callback for errors
    log_callback = log_callback, -- optional callback for logs and debug messages
    default_topic_options = {
        ["partitioner"] = "murmur2_random",
    }, -- optional default topic options
})
if err ~= nil then
    print(err)
    os.exit(1)
end

for i = 1, 1000 do
    local err = producer:produce_async({ -- don't wait until message will be delivired to kafka
        topic = "test_topic",
        key = "test_key",
        value = "test_value" -- only strings allowed
    })
    if err ~= nil then
        print(err)
        os.exit(1)
    end
end

producer:close() -- always stop consumer to send all pending messages before app close and free all used resources
