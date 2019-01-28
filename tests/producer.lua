local os = require('os')
local box = require('box')
local log = require('log')
local fiber = require('fiber')
local tnt_kafka = require('tnt-kafka')

local TOPIC_NAME = "test_producer"

return function(messages)
    local producer, err = tnt_kafka.Producer.create({brokers = "kafka:9092", options = {}})
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end

    for _, message in ipairs(messages) do
        local err = producer:produce_async({topic = TOPIC_NAME, key = message, value = message})
        if err ~= nil then
            log.error("got error '%s' while sending value '%s'", err, message)
        else
            log.error("successfully sent value '%s'", message)
        end
    end

--    fiber.sleep(2)

    local err = producer:close()
    if err ~= nil then
        log.error("got err %s", err)
        box.error{code = 500, reason = err}
    end
end
