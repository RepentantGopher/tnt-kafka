local os = require('os')
local fiber = require('fiber')
local kafka_producer = require('tnt-kafka.producer')

local BROKERS_ADDRESS = { "kafka" }
local TOPIC_NAME = "test_producer"

return function(messages)
    local config, err = kafka_producer.ProducerConfig.create(BROKERS_ADDRESS, true)
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    config:set_option("statistics.interval.ms", "1000")
    config:set_stat_cb(function (payload) print("Stat Callback '".. payload.. "'") end)

    local producer, err = kafka_producer.Producer.create(config)
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local err = producer:start()
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local err = producer:add_topic(TOPIC_NAME, {})
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    for _, message in ipairs(messages) do
        fiber.create(function()
            local err = producer:produce({topic = TOPIC_NAME, value = message})
            if err ~= nil then
                print(string.format("got error '%s' while sending value '%s'", err, message))
            else
                print(string.format("successfully sent value '%s'", message))
            end
        end)
    end

    fiber.sleep(2)

    local err = producer:stop()
    if err ~= nil then
        print(err)
        os.exit(1)
    end
end
