local fiber = require('fiber')
local box = require('box')
local os = require('os')
local log = require('log')
local clock = require('clock')
local tnt_kafka = require('kafka')

box.cfg{}

box.once('init', function()
    box.schema.user.grant("guest", 'read,write,execute,create,drop', 'universe')
end)

local function produce()
    local producer, err = tnt_kafka.Producer.create({brokers = "kafka:9092", options = {}})
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
--                print(err)
                fiber.sleep(0.1)
            else
                break
            end
        end
        if i % 1000 == 0 then
--            log.info("done %d", i)
            fiber.yield()
        end
    end

    log.info("stopping")
    local ok, err = producer:close() -- always stop consumer to send all pending messages before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local duration = clock.monotonic64() - before
    print(string.format("done benchmark for %f seconds", tonumber(duration * 1.0 / (10 ^ 9))))
end

log.info("starting benchmark")

produce()
