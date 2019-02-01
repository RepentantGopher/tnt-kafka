tnt-kafka
=========
Full featured high performance kafka library for Tarantool based on [librdkafka](https://github.com/edenhill/librdkafka). 

Can produce more then 80k messages per second and consume more then 130k messages per second.

Library was tested with librdkafka v0.11.5

# Features
* Kafka producer and consumer implementations.
* Fiber friendly.
* Mostly errorless functions and methods. Error handling in Tarantool ecosystem is quite a mess, 
some libraries throws lua native `error` while others throws `box.error` instead. `tnt-kafka` returns 
non critical errors as strings which allows you to decide how to handle it.

# Examples

## Consumer

### With auto offset store
```lua
    local fiber = require('fiber')
    local os = require('os')
    local tnt_kafka = require('tnt-kafka')

    local consumer, err = tnt_kafka.Consumer.create({
        brokers = "localhost:9092", -- brokers for bootstrap
        options = {
            ["enable.auto.offset.store"] = "true",
            ["group.id"] = "example_consumer",
            ["auto.offset.reset"] = "earliest",
            ["enable.partition.eof"] = "false"
        }, -- options for librdkafka
    })
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
                    "got msg with topic='%s' partition='%s' offset='%s' key='%s' value='%s'", 
                    msg:topic(), msg:partition(), msg:offset(), msg:key(), msg:value()
                ))
            end
        end
    end)
    
    fiber.sleep(10)
    
    local err = consumer:close() -- always stop consumer to commit all pending offsets before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
```

### With multiple fibers and manual offset store
```lua
    local fiber = require('fiber')
    local os = require('os')
    local tnt_kafka = require('tnt-kafka')

    local consumer, err = tnt_kafka.Consumer.create({
        brokers = "localhost:9092", -- brokers for bootstrap
        options = {
            ["enable.auto.offset.store"] = "false",
            ["group.id"] = "example_consumer",
            ["auto.offset.reset"] = "earliest",
            ["enable.partition.eof"] = "false"
        }, -- options for librdkafka
    })
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
                        "got msg with topic='%s' partition='%s' offset='%s' key='%s' value='%s'", 
                        msg:topic(), msg:partition(), msg:offset(), msg:key(), msg:value()
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
    
    local err = consumer:close() -- always stop consumer to commit all pending offsets before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
```

## Producer

### With single fiber and async producer

```lua
    local os = require('os')
    local tnt_kafka = require('tnt-kafka')
    
    local producer, err = tnt_kafka.Producer.create({
        brokers = "kafka:9092", -- brokers for bootstrap
        options = {} -- options for librdkafka
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
    
    local err = producer:close() -- always stop consumer to send all pending messages before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
```

### With multiple fibers and sync producer
```lua
    local fiber = require('fiber')
    local os = require('os')
    local tnt_kafka = require('tnt-kafka')
    
    local producer, err = tnt_kafka.Producer.create({
        brokers = "kafka:9092", -- brokers for bootstrap
        options = {} -- options for librdkafka
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
    
    local err = producer:close() -- always stop consumer to send all pending messages before app close
    if err ~= nil then
        print(err)
        os.exit(1)
    end
```

# Known issues
* Consumer and Producer leaves some non gc'able objects in memory after has been stopped. It was done intentionally
because `rd_kafka_destroy` sometimes hangs forever.

# TODO
* Ordered storage for offsets to prevent commits unprocessed messages
* Add poll call for librdkafka logs and errors
* Fix known issues
* More examples
* Better documentation

# Benchmarks

## Producer

### Async

Result: over 150000 produced messages per second on macbook pro 2016

Local run in docker:
```bash
    make docker-run-environment
    make docker-create-benchmark-async-producer-topic
    make docker-run-benchmark-async-producer-interactive
```

### Sync

Result: over 90000 produced messages per second on macbook pro 2016

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
    make docker-create-benchmark-manual-commit-consumer-topic
    make docker-run-benchmark-manual-commit-consumer-interactive
```

# Developing

## Tests
You can run docker based integration tests via makefile target
```bash
    make test-run-with-docker
``` 