local ffi = require('ffi')
local box = require('box')
local fiber = require('fiber')
local librdkafka = require('kafka.librdkafka')

local ProducerConfig = {}

ProducerConfig.__index = ProducerConfig

function ProducerConfig.create(brokers_list)
    assert(brokers_list ~= nil)

    local config = {
        _brokers_list = brokers_list,
        _options = {},
        _delivery_cb = nil,
        _stat_cb = nil,
        _error_cb = nil,
        _log_cb = nil,
    }
    setmetatable(config, ProducerConfig)
    return config
end

function ProducerConfig:get_brokers_list()
    return self._brokers_list
end

function ProducerConfig:set_option(name, value)
    self._options[name] = value
end

function ProducerConfig:get_options()
    return self._options
end

--[[
    Set statistics callback.
    The statistics callback is called from `KafkaProducer:poll` every
    `statistics.interval.ms` (needs to be configured separately).
    Format: callback_function(json)
    'json' - String containing the statistics data in JSON format
]]--

function ProducerConfig:set_stat_cb(callback)
    self._stat_cb = callback
end

function ProducerConfig:get_stat_cb()
    return self._stat_cb
end


--[[
    Set error callback.
    The error callback is used by librdkafka to signal critical errors
    back to the application.
    Format: callback_function(err_numb, reason)
]]--

function ProducerConfig:set_error_cb(callback)
    self._error_cb = callback
end

function ProducerConfig:get_error_cb()
    return self._error_cb
end

--[[
    Set logger callback.
    The default is to print to stderr.
    Alternatively the application may provide its own logger callback.
    Or pass 'callback' as nil to disable logging.
    Format: callback_function(level, fac, buf)
]]--

function ProducerConfig:set_log_cb(callback)
    self._log_cd = callback
end

function ProducerConfig:get_log_cb()
    return self._log_cd
end

local Producer = {}

Producer.__index = Producer

function Producer.create(config)
    assert(config ~= nil)

    local producer = {
        config = config,
        _counter = 0,
        _rd_topics = {},
        _rd_producer = {},
        _delivery_map = {},
    }
    setmetatable(producer, Producer)
    return producer
end

function Producer:_get_producer_rd_config()
    local rd_config = librdkafka.rd_kafka_conf_new()

    ffi.gc(rd_config, function (rd_config)
        librdkafka.rd_kafka_conf_destroy(rd_config)
    end)

    local ERRLEN = 256
    for key, value in pairs(self.config:get_options()) do
        local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected
        if librdkafka.rd_kafka_conf_set(rd_config, key, tostring(value), errbuf, ERRLEN) ~= librdkafka.RD_KAFKA_CONF_OK then
            return nil, ffi.string(errbuf)
        end
    end

    librdkafka.rd_kafka_conf_set_dr_msg_cb(rd_config,
        function(rk, rkmessage)
            local delivery_chan = self._delivery_map[tonumber(ffi.cast('intptr_t', rkmessage._private))]
            if delivery_chan ~= nil then
                local errstr = nil
                if rkmessage.err ~= librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
                    errstr = ffi.string(librdkafka.rd_kafka_err2str(rkmessage.err))
                end
                delivery_chan:put(errstr)
            end
        end)

    local stat_cb = self.config:get_stat_cb()
    if stat_cb ~= nil then
        librdkafka.rd_kafka_conf_set_stats_cb(rd_config,
            function(rk, json, json_len)
                stat_cb(ffi.string(json, json_len))
                return 0 --librdkafka will immediately free the 'json' pointer.
            end)
    end

    local error_cb = self.config:get_error_cb()
    if error_cb ~= nil then
        librdkafka.rd_kafka_conf_set_error_cb(rd_config,
            function(rk, err, reason)
                error_cb(tonumber(err), ffi.string(reason))
            end)
    end

    local log_cb = self.config:get_log_cb()
    if log_cb ~= nil then
        librdkafka.rd_kafka_conf_set_log_cb(rd_config,
            function(rk, level, fac, buf)
                log_cb(tonumber(level), ffi.string(fac), ffi.string(buf))
            end)
    end

    return rd_config, nil
end

function Producer:_poll()
    while true do
        librdkafka.rd_kafka_poll(self._rd_producer, 1)
        fiber.yield()
    end
end

jit.off(Producer._poll)

function Producer:start()
    local rd_config, err = self:_get_producer_rd_config()
    if err ~= nil then
        return err
    end

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected
    local rd_producer = librdkafka.rd_kafka_new(librdkafka.RD_KAFKA_PRODUCER, rd_config, errbuf, ERRLEN)

    if rd_producer == nil then
        return ffi.string(errbuf)
    end

    for _, broker in ipairs(self.config:get_brokers_list()) do
        librdkafka.rd_kafka_brokers_add(rd_producer, broker)
    end

    self._rd_producer = rd_producer

    self._poll_fiber = fiber.create(function()
        self:_poll()
    end)
end

function Producer:stop(timeout_ms)
    if self._rd_producer == nil then
        return "'stop' method must be called only after producer was started "
    end

    if timeout_ms == nil then
        timeout_ms = 1000
    end

    -- FIXME: handle this error
    local err = librdkafka.rd_kafka_flush(self._rd_producer, timeout_ms)

    self._poll_fiber:cancel()

    for name, rd_topic in pairs(self._rd_topics) do
        librdkafka.rd_kafka_topic_destroy(rd_topic)
    end
    self._rd_topics = nil

    librdkafka.rd_kafka_destroy(self._rd_producer)
    librdkafka.rd_kafka_wait_destroyed(timeout_ms)
    self._rd_producer = nil
end

function Producer:_get_topic_rd_config(config)
    local rd_config = librdkafka.rd_kafka_topic_conf_new()

    ffi.gc(rd_config, function (rd_config)
        librdkafka.rd_kafka_topic_conf_destroy(rd_config)
    end)

    local ERRLEN = 256
    for key, value in pairs(config) do
        local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected

        if librdkafka.rd_kafka_topic_conf_set(rd_config, key, value, errbuf, ERRLEN) ~= librdkafka.RD_KAFKA_CONF_OK then
            return nil, ffi.string(errbuf)
        end
    end

    return rd_config, nil
end

function Producer:add_topic(name, config)
    if self._rd_producer == nil then
        return "'add_topic' method must be called only after producer was started "
    end

    if self._rd_topics[name] ~= nil then
        return string.format('topic "%s" already exists', name)
    end

    local rd_config, err = self:_get_topic_rd_config(config)
    if err ~= nil then
        return err
    end

    local rd_topic = librdkafka.rd_kafka_topic_new(self._rd_producer, name, rd_config)
    if rd_topic == nil then
        return ffi.string(librdkafka.rd_kafka_err2str(librdkafka.rd_kafka_errno2err(ffi.errno())))
    end

    self._rd_topics[name] = rd_topic

    return nil
end

function Producer:_produce_async(msg, id)
    if self._rd_producer == nil then
        return "'produce' method must be called only after producer was started "
    end

    local keylen = 0
    if msg.key then keylen = #msg.key end

    if msg.value == nil or #msg.value == 0 then
        if keylen == 0 then
            return
        end
    end

    local partition = -1
    if msg.partition ~= nil then
        partition = msg.partition
    end

    local rd_topic = self._rd_topics[msg.topic]
    if rd_topic == nil then
        self:add_topic(msg.topic, {})
        rd_topic = self._rd_topics[msg.topic]
    end

    local RD_KAFKA_MSG_F_COPY = 0x2
    local produce_result = librdkafka.rd_kafka_produce(rd_topic, partition, RD_KAFKA_MSG_F_COPY,
        ffi.cast("void*", msg.value), #msg.value, ffi.cast("void*", msg.key), keylen, ffi.cast("void*", id))

    if produce_result == -1 then
        return ffi.string(librdkafka.rd_kafka_err2str(librdkafka.rd_kafka_errno2err(ffi.errno())))
    end

    return nil
end

function Producer:produce_async(msg)
    return self:_produce_async(msg, nil)
end

function Producer:produce(msg)
    self._counter = self._counter + 1
    local id = self._counter
    local delivery_chan = fiber.channel()
    self._delivery_map[id] = delivery_chan

    local err = self:_produce_async(msg, id)
    if err == nil then
        err = delivery_chan:get()
    end

    self._delivery_map[id] = nil
    return err
end

return {
    Producer = Producer,
    ProducerConfig = ProducerConfig,
}
