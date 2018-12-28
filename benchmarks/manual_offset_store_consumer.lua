local fiber = require('fiber')
local box = require('box')
local os = require('os')
local log = require('log')
local clock = require('clock')
local kafka_consumer = require('tnt-kafka.consumer')
local kafka_producer = require('tnt-kafka.producer')

box.cfg{
    memtx_memory = 524288000,
}

local TOPIC = "manual_offset_store_consumer"
local MSG_COUNT = 10000000

box.once('init', function()
    box.schema.user.grant("guest", 'read,write,execute,create,drop', 'universe')
end)

local function produce_initial_data()
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

    local err = producer:add_topic(TOPIC, {}) -- add topic with configuration
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    for i = 1, MSG_COUNT do
        while true do
            local err = producer:produce_async({ -- don't wait until message will be delivired to kafka
                topic = TOPIC,
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

    local err = producer:stop() -- always stop consumer to send all pending messages before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
end

local function consume()
    local config, err = kafka_consumer.ConsumerConfig.create(
        {"kafka:9092"}, -- array of brokers
        "test_consumer", -- consumer group
        false, -- enable_auto_commit
        {["auto.offset.reset"] = "earliest"} -- default configuration for topics
    )
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    config:set_option("queued.min.messages", "100000") -- set global consumer option

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

    local err = consumer:subscribe({TOPIC}) -- array of topics to subscribe
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local before = clock.monotonic64()
    local counter = 0
    local out, err = consumer:output()
    if err ~= nil then
        print(string.format("got fatal error '%s'", err))
        return
    end

    for i = 1, MSG_COUNT do
        if out:is_closed() then
            return
        end

        local msg = out:get()
        if msg ~= nil then
            local err = consumer:store_offset(msg)
            if err ~= nil then
                print(err)
                os.exit(1)
            end

            counter = counter + 1
        end
        if i % 100000 == 0 then
            log.info("done %d", i)
        end
    end

    log.info("closing")
    local err = consumer:stop()
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local duration = clock.monotonic64() - before
    log.info(string.format("done benchmark for %f seconds", tonumber(duration * 1.0 / (10 ^ 9))))
end

log.info("producing initial data")
produce_initial_data()

log.info("starting benchmark")
consume()
