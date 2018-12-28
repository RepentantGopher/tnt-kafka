local log = require("log")
local ffi = require('ffi')
local fiber = require('fiber')
local librdkafka = require('tnt-kafka.librdkafka')

local ConsumerConfig = {}

ConsumerConfig.__index = ConsumerConfig

function ConsumerConfig.create(brokers_list, consumer_group, auto_offset_store, default_topic_opts)
    if brokers_list == nil then
        return nil, "brokers list must not be nil"
    end
    if consumer_group == nil then
        return nil, "consumer group must not be nil"
    end
    if auto_offset_store == nil then
        return nil, "auto_offset_store flag must not be nil"
    end

    if default_topic_opts == nil then
        return nil, "default_topic_opts must not be nil"
    end

    local config = {
        _brokers_list = brokers_list,
        _consumer_group = consumer_group,
        _auto_offset_store = auto_offset_store,
        _options = {},
        _topic_opts = default_topic_opts,
    }
    setmetatable(config, ConsumerConfig)
    return config, nil
end

function ConsumerConfig:get_brokers_list()
    return self._brokers_list
end

function ConsumerConfig:get_consumer_group()
    return self._consumer_group
end

function ConsumerConfig:get_auto_offset_store()
    return self._auto_offset_store
end

function ConsumerConfig:set_option(name, value)
    self._options[name] = value
end

function ConsumerConfig:get_options()
    return self._options
end

function ConsumerConfig:get_default_topic_options()
    return self._topic_opts
end

local ConsumerMessage = {}

ConsumerMessage.__index = ConsumerMessage

function ConsumerMessage.create(rd_message)
    local msg = {
        _rd_message = rd_message,
        _value = nil,
        _topic = nil,
        _partition = nil,
        _offset = nil,
    }
    ffi.gc(msg._rd_message, function(...)
        librdkafka.rd_kafka_message_destroy(...)
    end)
    setmetatable(msg, ConsumerMessage)
    return msg
end

function ConsumerMessage:value()
    if self._value == nil then
        self._value = ffi.string(self._rd_message.payload)
    end
    return self._value
end

function ConsumerMessage:topic()
    if self._topic == nil then
        self._topic = ffi.string(librdkafka.rd_kafka_topic_name(self._rd_message.rkt))
    end
    return self._topic
end

function ConsumerMessage:partition()
    if self._partition == nil then
        self._partition = tonumber(self._rd_message.partition)
    end
    return self._partition
end

function ConsumerMessage:offset()
    if self._offset == nil then
        self._offset = tonumber64(self._rd_message.offset)
    end
    return self._offset
end

local Consumer = {}

Consumer.__index = Consumer

function Consumer.create(config)
    if config == nil then
        return nil, "config must not be nil"
    end

    local consumer = {
        config = config,
        _rd_consumer = {},
        _output_ch = nil,
    }
    setmetatable(consumer, Consumer)
    return consumer, nil
end

function Consumer:_get_topic_rd_config(config)
    local rd_config = librdkafka.rd_kafka_topic_conf_new()

--    FIXME: sometimes got segfault here
--    ffi.gc(rd_config, function (rd_config)
--        librdkafka.rd_kafka_topic_conf_destroy(rd_config)
--    end)

    local ERRLEN = 256
    for key, value in pairs(config) do
        local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected

        if librdkafka.rd_kafka_topic_conf_set(rd_config, key, value, errbuf, ERRLEN) ~= librdkafka.RD_KAFKA_CONF_OK then
            return nil, ffi.string(errbuf)
        end
    end

    return rd_config, nil
end

function Consumer:_get_consumer_rd_config()
    local rd_config = librdkafka.rd_kafka_conf_new()

-- FIXME: why we got segfault here?
--    ffi.gc(rd_config, function (rd_config)
--        librdkafka.rd_kafka_conf_destroy(rd_config)
--    end)

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected
    if librdkafka.rd_kafka_conf_set(rd_config, "group.id", tostring(self.config:get_consumer_group()), errbuf, ERRLEN) ~= librdkafka.RD_KAFKA_CONF_OK then
        return nil, ffi.string(errbuf)
    end

    local auto_offset_store
    if self.config:get_auto_offset_store() then
        auto_offset_store = "true"
    else
        auto_offset_store = "false"
    end

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected
    if librdkafka.rd_kafka_conf_set(rd_config, "enable.auto.offset.store", auto_offset_store, errbuf, ERRLEN) ~= librdkafka.RD_KAFKA_CONF_OK then
        return nil, ffi.string(errbuf)
    end

    for key, value in pairs(self.config:get_options()) do
        local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected
        if librdkafka.rd_kafka_conf_set(rd_config, key, tostring(value), errbuf, ERRLEN) ~= librdkafka.RD_KAFKA_CONF_OK then
            return nil, ffi.string(errbuf)
        end
    end

    librdkafka.rd_kafka_conf_set_error_cb(rd_config,
        function(rk, err, reason)
            log.error("rdkafka error code=%d reason=%s", tonumber(err), ffi.string(reason))
        end)


    librdkafka.rd_kafka_conf_set_log_cb(rd_config,
        function(rk, level, fac, buf)
            log.info("%s - %s", ffi.string(fac), ffi.string(buf))
        end)

    local rd_topic_config, err = self:_get_topic_rd_config(self.config:get_default_topic_options())
    if err ~= nil then
        return nil, err
    end

    librdkafka.rd_kafka_conf_set_default_topic_conf(rd_config, rd_topic_config)

    return rd_config, nil
end

function Consumer:_poll()
    while true do
        -- lower timeout value can lead to broken payload
        local rd_message = librdkafka.rd_kafka_consumer_poll(self._rd_consumer, 10)
        if rd_message ~= nil then
            if rd_message.err == librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
                self._output_ch:put(ConsumerMessage.create(rd_message))
            else
                -- FIXME: properly log this
                log.error("rdkafka poll: %s", ffi.string(librdkafka.rd_kafka_err2str(rd_message.err)))
            end
        end
        fiber.yield()
    end
end

jit.off(Consumer._poll)

function Consumer:start()
    local rd_config, err = self:_get_consumer_rd_config()
    if err ~= nil then
        return err
    end

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN) -- cdata objects are garbage collected
    local rd_consumer = librdkafka.rd_kafka_new(librdkafka.RD_KAFKA_CONSUMER, rd_config, errbuf, ERRLEN)
    if rd_consumer == nil then
        return ffi.string(errbuf)
    end

    local err_no = librdkafka.rd_kafka_poll_set_consumer(rd_consumer)
    if err_no ~= librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
        return ffi.string(librdkafka.rd_kafka_err2str(err_no))
    end

    for _, broker in ipairs(self.config:get_brokers_list()) do
        if librdkafka.rd_kafka_brokers_add(rd_consumer, broker) < 1 then
            return "no valid brokers specified"
        end
    end

    self._rd_consumer = rd_consumer

    self._output_ch = fiber.channel(10000)

    self._poll_fiber = fiber.create(function()
        self:_poll()
    end)

    return nil
end

function Consumer:stop(timeout_ms)
    if self._rd_consumer == nil then
        return "'stop' method must be called only after consumer was started "
    end

    if timeout_ms == nil then
        timeout_ms = 1000
    end

    self._poll_fiber:cancel()
    self._output_ch:close()

    local err_no = librdkafka.rd_kafka_consumer_close(self._rd_consumer)
    if err_no ~= librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
        return ffi.string(librdkafka.rd_kafka_err2str(err_no))
    end

--    -- FIXME: sometimes rd_kafka_destroy hangs forever
--    librdkafka.rd_kafka_destroy(self._rd_consumer)
--    librdkafka.rd_kafka_wait_destroyed(timeout_ms)

    self._rd_consumer = nil

    return nil
end

function Consumer:subscribe(topics)
    if self._rd_consumer == nil then
        return "'add_topic' method must be called only after consumer was started "
    end

    local list = librdkafka.rd_kafka_topic_partition_list_new(#topics)
    for _, topic in ipairs(topics) do
        librdkafka.rd_kafka_topic_partition_list_add(list, topic, librdkafka.RD_KAFKA_PARTITION_UA)
    end

    local err = nil
    local err_no = librdkafka.rd_kafka_subscribe(self._rd_consumer, list)
    if err_no ~= librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
        err = ffi.string(librdkafka.rd_kafka_err2str(err_no))
    end


    librdkafka.rd_kafka_topic_partition_list_destroy(list)

    return err
end

function Consumer:output()
    if self._output_ch == nil then
        return nil, "'output' method must be called only after consumer was started "
    end

    return self._output_ch, nil
end

function Consumer:store_offset(message)
    if self._rd_consumer == nil then
        return "'store_offset' method must be called only after consumer was started "
    end

    if self.config:get_auto_offset_store() then
        return "auto offset store was enabled by configuration"
    end

    local err_no = librdkafka.rd_kafka_offset_store(message._rd_message.rkt, message._rd_message.partition, message._rd_message.offset)
    if err_no ~= librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
        return ffi.string(librdkafka.rd_kafka_err2str(err_no))
    end

    return nil
end

return {
    ConsumerConfig = ConsumerConfig,
    Consumer = Consumer,
}
