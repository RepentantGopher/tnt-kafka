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
    fiber.create(function()
        local message = "test_value " .. tostring(i)
        local err = producer:produce({ -- wait until message will be delivired to kafka (using channel under the hood)
            topic = "test_topic",
            key = "test_key",
            value =  message -- only strings allowed
        })
        if err ~= nil then
            print(string.format("got error '%s' while sending value '%s'", err, message))
        else
            print(string.format("successfully sent value '%s'", message))
        end
    end)
end

fiber.sleep(10)

producer:close() -- always stop consumer to send all pending messages before app close and free all used resources
