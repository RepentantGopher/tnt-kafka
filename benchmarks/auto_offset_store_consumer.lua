local fiber = require('fiber')
local log = require('log')
local box = require('box')
local os = require('os')
local clock = require('clock')
local tnt_kafka = require('tnt-kafka')

box.cfg{
    memtx_memory = 524288000, -- 500 MB
}

local TOPIC = "auto_offset_store_consumer_benchmark"
local MSG_COUNT = 10000000

box.once('init', function()
    box.schema.user.grant("guest", 'read,write,execute,create,drop', 'universe')
end)

local function produce_initial_data()
    local producer, err = tnt_kafka.Producer.create({ brokers = "kafka:9092"})
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
--                print(err)
                fiber.sleep(0.1)
            else
                break
            end
        end
        if i % 1000 == 0 then
            fiber.yield()
        end
    end

    local ok, err = producer:close() -- always stop consumer to send all pending messages before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
end

local function consume()
    local consumer, err = tnt_kafka.Consumer.create({ brokers = "kafka:9092", options = {
        ["enable.auto.offset.store"] = "true",
        ["group.id"] = "test_consumer1",
        ["auto.offset.reset"] = "earliest",
        ["enable.partition.eof"] = "false",
        ["queued.min.messages"] = "100000"
    }})
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local err = consumer:subscribe({TOPIC})
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

    while counter < MSG_COUNT do
        if out:is_closed() then
            return
        end

        local msg = out:get()
        if msg ~= nil then
            counter = counter + 1
--            print(msg:value())
        end
        if counter % 10000 == 0 then
            log.info("done %d", counter)
            fiber.yield()
        end
    end

    print("closing")
    local ok, err = consumer:close()
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local duration = clock.monotonic64() - before
    print(string.format("done benchmark for %f seconds", tonumber(duration * 1.0 / (10 ^ 9))))
end

print("producing initial data")
produce_initial_data()

print("starting benchmark")
consume()
