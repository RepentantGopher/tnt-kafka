local fiber = require('fiber')
local box = require('box')
local log = require('log')
local os = require('os')
local clock = require('clock')
local tnt_kafka = require('kafka')

box.cfg{
    memtx_memory = 524288000, -- 500 MB
}

box.once('init', function()
    box.schema.user.grant("guest", 'read,write,execute,create,drop', 'universe')
end)

local function produce()
    local producer, err = tnt_kafka.Producer.create({
        brokers = "kafka:9092",
        options = {
            ["queue.buffering.max.ms"] = "100",
        }
    })
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    local before = clock.monotonic64()
    local input_ch = fiber.channel();
    for i = 1, 12000 do
        fiber.create(function()
            while true do
                if input_ch:is_closed() then
                    break
                end
                local value = input_ch:get()
                if value ~= nil then
                    while true do
                        local err = producer:produce({
                            topic = "sync_producer_benchmark",
                            value = value -- only strings allowed
                        })
                        if err ~= nil then
                            --                    print(err)
                            fiber.sleep(0.1)
                        else
--                            if value % 10000 == 0 then
--                                log.info("done %d", value)
--                            end
                            break
                        end
                    end
                end
            end
        end)
    end

    for i = 1, 10000000 do
        input_ch:put(i)
    end

    input_ch:close()

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
