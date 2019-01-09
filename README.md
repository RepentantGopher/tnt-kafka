tnt-kafka
=========

*Project now lives in official repo of Tarantool team [github.com/tarantool/tnt-kafka](https://github.com/tarantool/tnt-kafka)*

Full featured high performance kafka library for Tarantool based on [librdkafka](https://github.com/edenhill/librdkafka). 

Can produce more then 80k messages per second and consume more then 130k messages per second.

Library was tested with librdkafka v0.11.5

# Features
* Kafka producer and consumer implementations.
* Fiber friendly.
* Mostly errorless functions and methods. Error handling in Tarantool ecosystem is quite a mess, 
some libraries throws lua native `error` while others throws `box.error` instead. `tnt-kafka` returns 
errors as strings which allows you to decide how to handle it.

# Examples

## Consumer

### With auto offset store
```lua
    local fiber = require('fiber')
    local os = require('os')
    local kafka_consumer = require('tnt-kafka.consumer')
    
    local config, err = kafka_consumer.ConsumerConfig.create(
        {"localhost:9092"}, -- array of brokers 
        "test_consumer", -- consumer group
        true, -- enable auto offset store
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

    local err = consumer:subscribe({"test_topic"}) -- array of topics to subscribe
    if err ~= nil then
        print(err)
        os.exit(1)
    end
    
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
                    "got msg with topic='%s' partition='%s' offset='%s' value='%s'", 
                    msg:topic(), msg:partition(), msg:offset(), msg:value()
                ))
            end
        end
    end)
    
    fiber.sleep(10)
    
    local err = consumer:stop() -- always stop consumer to commit all pending offsets before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
```

### With multiple fibers and manual offset store
```lua
    local fiber = require('fiber')
    local os = require('os')
    local kafka_consumer = require('tnt-kafka.consumer')
    
    local config, err = kafka_consumer.ConsumerConfig.create(
        {"localhost:9092"}, -- array of brokers 
        "test_consumer", -- consumer group
        false, -- disable auto offset store
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
                        "got msg with topic='%s' partition='%s' offset='%s' value='%s'", 
                        msg:topic(), msg:partition(), msg:offset(), msg:value()
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
    
    local err = consumer:stop() -- always stop consumer to commit all pending offsets before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
```

## Producer

### With single fiber and async producer

```lua
    local os = require('os')
    local kafka_producer = require('tnt-kafka.producer')
    
    local config, err = kafka_producer.ProducerConfig.create(
        {"localhost:9092"}, -- -- array of brokers   
        false -- sync_producer
    )
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    config:set_option("statistics.interval.ms", "1000") -- set global producer option
    config:set_stat_cb(function (payload) print("Stat Callback '".. payload.. "'") end) -- set callback for stats

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

    local err = producer:add_topic("test_topic", {}) -- add topic with configuration
    if err ~= nil then
        print(err)
        os.exit(1)
    end
    
    for i = 1, 1000 do    
        local err = producer:produce_async({ -- don't wait until message will be delivired to kafka
            topic = "test_topic", 
            value = "test_value" -- only strings allowed
        })
        if err ~= nil then
            print(err)
            os.exit(1)
        end
    end
    
    local err = producer:stop() -- always stop consumer to send all pending messages before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
```

### With multiple fibers and sync producer
```lua
    local fiber = require('fiber')
    local os = require('os')
    local kafka_producer = require('tnt-kafka.producer')
    
    local config, err = kafka_producer.ProducerConfig.create(
        {"localhost:9092"}, -- -- array of brokers   
        true -- sync_producer
    )
    if err ~= nil then
        print(err)
        os.exit(1)
    end

    config:set_option("statistics.interval.ms", "1000") -- set global producer option
    config:set_stat_cb(function (payload) print("Stat Callback '".. payload.. "'") end) -- set callback for stats

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

    local err = producer:add_topic("test_topic", {}) -- add topic with configuration
    if err ~= nil then
        print(err)
        os.exit(1)
    end
    
    for i = 1, 1000 do
        fiber.create(function()
            local message = "test_value " .. tostring(i)
            local err = producer:produce({ -- wait until message will be delivired to kafka (using channel under the hood)
                topic = "test_topic", 
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
    
    local err = producer:stop() -- always stop consumer to send all pending messages before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
```

# Known issues
* Producer can use only random messages partitioning. It was done intentionally because non nil key 
leads to segfault.
* Consumer leaves some non gc'able objects in memory after has been stopped. It was done intentionally
because `rd_kafka_destroy` sometimes hangs forever.

# TODO
* Rocks package
* Ordered storage for offsets to prevent commits unprocessed messages
* Fix known issues
* More examples
* Better documentation

# Benchmarks

## Producer

### Async

Result: over 80000 produced messages per second on macbook pro 2016

Local run in docker:
```bash
    make docker-run-environment
    make docker-create-benchmark-async-producer-topic
    make docker-run-benchmark-async-producer-interactive
```

### Sync

Result: over 50000 produced messages per second on macbook pro 2016

Local run in docker:
```bash
    make docker-run-environment
    make docker-create-benchmark-sync-producer-topic
    make docker-run-benchmark-sync-producer-interactive
```

## Consumer

### Auto offset store enabled

Result: over 130000 consumed messages per second on macbook pro 2016

Local run in docker:
```bash
    make docker-run-environment
    make docker-create-benchmark-auto-offset-store-consumer-topic
    make docker-run-benchmark-auto-offset-store-consumer-interactive
```

### Manual offset store

Result: over 130000 consumed messages per second on macbook pro 2016

Local run in docker:
```bash
    make docker-run-environment
    docker-create-benchmark-manual-commit-consumer-topic
    make docker-run-benchmark-manual-commit-consumer-interactive
```

# Developing

## Tests
You can run docker based integration tests via makefile target
```bash
    make test-run-with-docker
``` 