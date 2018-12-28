local fiber = require('fiber')
local box = require('box')
local os = require('os')
local log = require('log')
local clock = require('clock')
local kafka_producer = require('tnt-kafka.producer')

box.cfg{}

box.once('init', function()
    box.schema.user.grant("guest", 'read,write,execute,create,drop', 'universe')
end)

local function produce()
    local config, err = kafka_producer.ProducerConfig.create(
        {"kafka:9092"}, -- -- array of brokers
        false -- sync_producer
    )
    if err ~= nil then
        print(err)
        os.exit(1)
    end

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

    local err = producer:add_topic("async_producer_benchmark", {}) -- add topic with configuration
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local before = clock.monotonic64()
    for i = 1, 10000000 do
        while true do
            local err = producer:produce_async({ -- don't wait until message will be delivired to kafka
                topic = "async_producer_benchmark",
                value = "test_value_" .. tostring(i) -- only strings allowed
            })
            if err ~= nil then
                print(err)
            else
                break
            end
        end
        if i % 1000 == 0 then
            fiber.yield()
        end
    end

    log.info("stopping")
    local err = producer:stop() -- always stop consumer to send all pending messages before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local duration = clock.monotonic64() - before
    print(string.format("done benchmark for %f seconds", tonumber(duration * 1.0 / (10 ^ 9))))
end

log.info("starting benchmark")
produce()
