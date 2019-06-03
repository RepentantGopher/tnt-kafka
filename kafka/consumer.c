#include <stdlib.h>
#include <errno.h>

#include <librdkafka/rdkafka.h>
#include <tarantool/module.h>

#include <common.h>
#include <callbacks.h>
#include <queue.h>
#include <consumer_msg.h>

#include "consumer.h"

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Consumer
 */

static inline consumer_t *
lua_check_consumer(struct lua_State *L, int index) {
    consumer_t **consumer_p = (consumer_t **)luaL_checkudata(L, index, consumer_label);
    if (consumer_p == NULL || *consumer_p == NULL)
        luaL_error(L, "Kafka consumer fatal error: failed to retrieve consumer from lua stack!");
    return *consumer_p;
}

int
lua_consumer_subscribe(struct lua_State *L) {
    if (lua_gettop(L) != 2 || !lua_istable(L, 2))
        luaL_error(L, "Usage: err = consumer:subscribe({'topic'})");

    consumer_t *consumer = lua_check_consumer(L, 1);

    if (consumer->topics == NULL) {
        consumer->topics = rd_kafka_topic_partition_list_new(lua_objlen(L, 1));
    }

    lua_pushnil(L);
    // stack now contains: -1 => nil; -2 => table; -3 => consumer
    while (lua_next(L, -2)) {
        // stack now contains: -1 => value; -2 => key; -3 => table; -4 => consumer
        const char *value = lua_tostring(L, -1);

        rd_kafka_topic_partition_list_add(consumer->topics, value, -1);

        // pop value, leaving original key
        lua_pop(L, 1);
        // stack now contains: -1 => key; -2 => table; -3 => consumer
    }
    // stack now contains: -1 => table; -2 => consumer

    rd_kafka_resp_err_t err = rd_kafka_subscribe(consumer->rd_consumer, consumer->topics);
    if (err) {
        const char *const_err_str = rd_kafka_err2str(err);
        char err_str[512];
        strcpy(err_str, const_err_str);
        int fail = safe_pushstring(L, err_str);
        return fail ? lua_push_error(L): 1;
    }

    return 0;
}

int
lua_consumer_unsubscribe(struct lua_State *L) {
    if (lua_gettop(L) != 2 || !lua_istable(L, 2))
        luaL_error(L, "Usage: err = consumer:unsubscribe({'topic'})");

    consumer_t *consumer = lua_check_consumer(L, 1);

    if (consumer->topics == NULL) {
        return 0;
    }

    lua_pushnil(L);
    // stack now contains: -1 => nil; -2 => table; -3 => consumer
    while (lua_next(L, -2)) {
        // stack now contains: -1 => value; -2 => key; -3 => table; -4 => consumer
        const char *value = lua_tostring(L, -1);

        rd_kafka_topic_partition_list_del(consumer->topics, value, -1);

        // pop value, leaving original key
        lua_pop(L, 1);
        // stack now contains: -1 => key; -2 => table; -3 => consumer
    }
    // stack now contains: -1 => table; -2 => consumer

    if (consumer->topics->cnt > 0) {
        rd_kafka_resp_err_t err = rd_kafka_subscribe(consumer->rd_consumer, consumer->topics);
        if (err) {
            const char *const_err_str = rd_kafka_err2str(err);
            char err_str[512];
            strcpy(err_str, const_err_str);
            int fail = safe_pushstring(L, err_str);
            return fail ? lua_push_error(L): 1;
        }
    } else {
        rd_kafka_resp_err_t err = rd_kafka_unsubscribe(consumer->rd_consumer);
        if (err) {
            const char *const_err_str = rd_kafka_err2str(err);
            char err_str[512];
            strcpy(err_str, const_err_str);
            int fail = safe_pushstring(L, err_str);
            return fail ? lua_push_error(L): 1;
        }
    }

    return 0;
}

int
lua_consumer_tostring(struct lua_State *L) {
    consumer_t *consumer = lua_check_consumer(L, 1);
    lua_pushfstring(L, "Kafka Consumer: %p", consumer);
    return 1;
}

static ssize_t
consumer_poll(va_list args) {
    rd_kafka_t *rd_consumer = va_arg(args, rd_kafka_t *);
    rd_kafka_poll(rd_consumer, 1000);
    return 0;
}

int
lua_consumer_poll(struct lua_State *L) {
    if (lua_gettop(L) != 1)
        luaL_error(L, "Usage: err = consumer:poll()");

    consumer_t *consumer = lua_check_consumer(L, 1);
    if (coio_call(consumer_poll, consumer->rd_consumer) == -1) {
        lua_pushstring(L, "unexpected error on consumer poll");
        return 1;
    }
    return 0;
}

int
lua_consumer_poll_msg(struct lua_State *L) {
    if (lua_gettop(L) != 2)
        luaL_error(L, "Usage: msgs = consumer:poll_msg(msgs_limit)");

    consumer_t *consumer = lua_check_consumer(L, 1);
    int counter = 0;
    int msgs_limit = lua_tonumber(L, 2);
    rd_kafka_event_t *event = NULL;
    lua_createtable(L, msgs_limit, 0);

    while (msgs_limit > counter) {
        event = rd_kafka_queue_poll(consumer->rd_msg_queue, 0);
        if (event == NULL) {
            break;
        }
        if (rd_kafka_event_type(event) == RD_KAFKA_EVENT_FETCH) {
            counter += 1;

            msg_t *msg;
            msg = malloc(sizeof(msg_t));
            msg->rd_message = rd_kafka_event_message_next(event);
            msg->rd_event = event;

            msg_t **msg_p = (msg_t **)lua_newuserdata(L, sizeof(msg));
            *msg_p = msg;

            luaL_getmetatable(L, consumer_msg_label);
            lua_setmetatable(L, -2);

            lua_rawseti(L, -2, counter);
        } else {
            rd_kafka_event_destroy(event);
        }
    }
    return 1;
}

int
lua_consumer_poll_logs(struct lua_State *L) {
    if (lua_gettop(L) != 2)
        luaL_error(L, "Usage: count, err = consumer:poll_logs(limit)");

    consumer_t *consumer = lua_check_consumer(L, 1);
    if (consumer->event_queues == NULL || consumer->event_queues->log_queue == NULL || consumer->event_queues->log_cb_ref == LUA_REFNIL) {
        lua_pushnumber(L, 0);
        int fail = safe_pushstring(L, "Consumer poll logs error: callback for logs is not set");
        if (fail) {
            return lua_push_error(L);
        }
        return 2;
    }

    int limit = lua_tonumber(L, 2);
    log_msg_t *msg = NULL;
    int count = 0;
    char *err_str = NULL;
    while (count < limit) {
        msg = queue_pop(consumer->event_queues->log_queue) ;
        if (msg == NULL) {
            break;
        }
        count++;
        lua_rawgeti(L, LUA_REGISTRYINDEX, consumer->event_queues->log_cb_ref);
        lua_pushstring(L, msg->fac);
        lua_pushstring(L, msg->buf);
        lua_pushnumber(L, (double)msg->level);
        /* do the call (3 arguments, 0 result) */
        if (lua_pcall(L, 3, 0, 0) != 0) {
            err_str = (char *)lua_tostring(L, -1);
        }

        destroy_log_msg(msg);

        if (err_str != NULL) {
            break;
        }
    }
    lua_pushnumber(L, (double)count);
    if (err_str != NULL) {
        int fail = safe_pushstring(L, err_str);
        if (fail) {
            return lua_push_error(L);
        }
    } else {
        lua_pushnil(L);
    }
    return 2;
}

int
lua_consumer_poll_errors(struct lua_State *L) {
    if (lua_gettop(L) != 2)
        luaL_error(L, "Usage: count, err = consumer:poll_errors(limit)");

    consumer_t *consumer = lua_check_consumer(L, 1);
    if (consumer->event_queues == NULL || consumer->event_queues->error_queue == NULL || consumer->event_queues->error_cb_ref == LUA_REFNIL) {
        lua_pushnumber(L, 0);
        int fail = safe_pushstring(L, "Consumer poll errors error: callback for logs is not set");
        if (fail) {
            return lua_push_error(L);
        }
        return 2;
    }

    int limit = lua_tonumber(L, 2);
    error_msg_t *msg = NULL;
    int count = 0;
    char *err_str = NULL;
    while (count < limit) {
        msg = queue_pop(consumer->event_queues->error_queue) ;
        if (msg == NULL) {
            break;
        }
        count++;
        lua_rawgeti(L, LUA_REGISTRYINDEX, consumer->event_queues->error_cb_ref);
        lua_pushstring(L, msg->reason);
        /* do the call (1 arguments, 0 result) */
        if (lua_pcall(L, 1, 0, 0) != 0) {
            err_str = (char *)lua_tostring(L, -1);
        }

        destroy_error_msg(msg);

        if (err_str != NULL) {
            break;
        }
    }
    lua_pushnumber(L, (double)count);
    if (err_str != NULL) {
        int fail = safe_pushstring(L, err_str);
        if (fail) {
            return lua_push_error(L);
        }
    } else {
        lua_pushnil(L);
    }
    return 2;
}

int
lua_prepare_rebalance_callback_args_on_stack(struct lua_State *L, rebalance_msg_t *msg) {
    rd_kafka_topic_partition_t *tp = NULL;
    // creating main table
    lua_createtable(L, 0, 3); // main table
    if (msg->assigned != NULL) {
        lua_pushstring(L, "assigned"); //  "assigned" > main table
        // creating table for assigned topics
        lua_createtable(L, 0, 10);  //  table with topics > "assigned" > main table

        for (int i = 0; i < msg->assigned->cnt; i++) {
            tp = &msg->assigned->elems[i];

            lua_pushstring(L, tp->topic); //  topic name > table with topics > "assigned" > main table

            // get value from table
            lua_pushstring(L, tp->topic); //  topic name > topic name > table with topics > "assigned" > main table
            lua_gettable(L, -3); // table with partitions or nil > topic name > table with topics > "assigned" > main table

            if (lua_isnil(L, -1) == 1) {
                // removing nil from top of stack
                lua_pop(L, 1); // topic name > table with topics > "assigned" > main table
                // creating table for assigned partitions on topic
                lua_createtable(L, 0, 10); // table with partitions > topic name > table with topics > "assigned" > main table
            }

            // add partition to table
            lua_pushinteger(L, tp->partition);  // key > table with partitions > topic name > table with topics > "assigned" > main table
            luaL_pushint64(L, tp->offset);  // value > key > table with partitions > topic name > table with topics > "assigned" > main table
            lua_settable(L, -3);  // table with partitions > topic name > table with topics > "assigned" > main table

            // add table with partitions to table with topics
            lua_settable(L, -3);  // table with topics > "assigned" > main table
        }

        lua_settable(L, -3);  // main table
    }
    else if (msg->revoked != NULL) {
        lua_pushstring(L, "revoked"); //  "revoked" > main table
        // creating table for revoked topics
        lua_createtable(L, 0, 10);  //  table with topics > "revoked" > main table

        for (int i = 0; i < msg->revoked->cnt; i++) {
            tp = &msg->revoked->elems[i];

            lua_pushstring(L, tp->topic); //  topic name > table with topics > "revoked" > main table

            // get value from table
            lua_pushstring(L, tp->topic); //  topic name > topic name > table with topics > "revoked" > main table
            lua_gettable(L, -3); // table with partitions or nil > topic name > table with topics > "revoked" > main table

            if (lua_isnil(L, -1) == 1) {
                // removing nil from top of stack
                lua_pop(L, 1); // topic name > table with topics > "revoked" > main table
                // creating table for revoked partitions on topic
                lua_createtable(L, 0, 10); // table with partitions > topic name > table with topics > "revoked" > main table
            }

            // add partition to table
            lua_pushinteger(L, tp->partition);  // key > table with partitions > topic name > table with topics > "revoked" > main table
            luaL_pushint64(L, tp->offset);  // value > key > table with partitions > topic name > table with topics > "revoked" > main table
            lua_settable(L, -3);  // table with partitions > topic name > table with topics > "revoked" > main table

            // add table with partitions to table with topics
            lua_settable(L, -3);  // table with topics > "revoked" > main table
        }

        lua_settable(L, -3);  // main table
    }
    else if (msg->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        lua_pushstring(L, "error");  //  "error" > main table
        lua_pushstring(L, rd_kafka_err2str(msg->err));  // msg > "error" > main table
        lua_settable(L, -3);  // main table
    }
    else {
        return -1;
    }
    return 0;
}

int
lua_consumer_poll_rebalances(struct lua_State *L) {
    if (lua_gettop(L) != 2)
        luaL_error(L, "Usage: count, err = consumer:poll_rebalances(limit)");

    consumer_t *consumer = lua_check_consumer(L, 1);
    if (consumer->event_queues == NULL || consumer->event_queues->rebalance_queue == NULL || consumer->event_queues->rebalance_cb_ref == LUA_REFNIL) {
        lua_pushnumber(L, 0);
        int fail = safe_pushstring(L, "Consumer poll rebalances error: callback for rebalance is not set");
        if (fail) {
            return lua_push_error(L);
        }
        return 2;
    }

    // FIXME:
    // rd_kafka_consumer_poll(consumer, 0);

    int limit = lua_tonumber(L, 2);
    rebalance_msg_t *msg = NULL;
    int count = 0;
    char *err_str = NULL;

    while (count < limit) {
        msg = queue_pop(consumer->event_queues->rebalance_queue);
        if (msg == NULL) {
            break;
        }
        count++;

        pthread_mutex_lock(&msg->lock);

        // push callback on stack
        lua_rawgeti(L, LUA_REGISTRYINDEX, consumer->event_queues->rebalance_cb_ref);

        // push rebalance args on stack
        if (lua_prepare_rebalance_callback_args_on_stack(L, msg) == 0) {
            /* do the call (1 arguments, 0 result) */
            if (lua_pcall(L, 1, 0, 0) != 0) {
                err_str = (char *)lua_tostring(L, -1);
            }
        } else {
            err_str = "unknown error on rebalance callback args processing";
        }

        // allowing background thread proceed rebalancing
        pthread_cond_signal(&msg->sync);

        pthread_mutex_unlock(&msg->lock);

        if (err_str != NULL) {
            break;
        }
    }

    lua_pushnumber(L, (double)count);
    if (err_str != NULL) {
        int fail = safe_pushstring(L, err_str);
        if (fail) {
            return lua_push_error(L);
        }
    } else {
        lua_pushnil(L);
    }
    return 2;
}

int
lua_consumer_store_offset(struct lua_State *L) {
    if (lua_gettop(L) != 2)
        luaL_error(L, "Usage: err = consumer:store_offset(msg)");

    msg_t *msg = lua_check_consumer_msg(L, 2);
    rd_kafka_resp_err_t err = rd_kafka_offset_store(msg->rd_message->rkt, msg->rd_message->partition, msg->rd_message->offset);
    if (err) {
        const char *const_err_str = rd_kafka_err2str(err);
        char err_str[512];
        strcpy(err_str, const_err_str);
        int fail = safe_pushstring(L, err_str);
        return fail ? lua_push_error(L): 1;
    }
    return 0;
}

static rd_kafka_resp_err_t
consumer_close(struct lua_State *L, consumer_t *consumer) {
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

    if (consumer->topics != NULL) {
        rd_kafka_topic_partition_list_destroy(consumer->topics);
    }

    if (consumer->rd_msg_queue != NULL) {
        rd_kafka_queue_destroy(consumer->rd_msg_queue);
    }

    if (consumer->rd_consumer != NULL) {
        err = rd_kafka_consumer_close(consumer->rd_consumer);
    }

    if (consumer->event_queues != NULL) {
        destroy_event_queues(L, consumer->event_queues);
    }

    if (consumer->rd_consumer != NULL) {
        /* Destroy handle */
        // FIXME: kafka_destroy hangs forever
//        coio_call(kafka_destroy, consumer->rd_consumer);
    }

    free(consumer);

    return err;
}

int
lua_consumer_close(struct lua_State *L) {
    consumer_t **consumer_p = (consumer_t **)luaL_checkudata(L, 1, consumer_label);
    if (consumer_p == NULL || *consumer_p == NULL) {
        lua_pushboolean(L, 0);
        return 1;
    }

    rd_kafka_resp_err_t err = consumer_close(L, *consumer_p);
    if (err) {
        lua_pushboolean(L, 1);

        const char *const_err_str = rd_kafka_err2str(err);
        char err_str[512];
        strcpy(err_str, const_err_str);
        int fail = safe_pushstring(L, err_str);
        return fail ? lua_push_error(L): 2;
    }

    *consumer_p = NULL;
    lua_pushboolean(L, 1);
    return 1;
}

int
lua_consumer_gc(struct lua_State *L) {
    consumer_t **consumer_p = (consumer_t **)luaL_checkudata(L, 1, consumer_label);
    if (consumer_p && *consumer_p) {
        consumer_close(L, *consumer_p);
    }
    if (consumer_p)
        *consumer_p = NULL;
    return 0;
}

int
lua_create_consumer(struct lua_State *L) {
    if (lua_gettop(L) != 1 || !lua_istable(L, 1))
        luaL_error(L, "Usage: consumer, err = create_consumer(conf)");

    lua_pushstring(L, "brokers");
    lua_gettable(L, -2 );
    const char *brokers = lua_tostring(L, -1);
    lua_pop(L, 1);
    if (brokers == NULL) {
        lua_pushnil(L);
        int fail = safe_pushstring(L, "consumer config table must have non nil key 'brokers' which contains string");
        return fail ? lua_push_error(L): 2;
    }

    char errstr[512];
    rd_kafka_conf_t *rd_config = rd_kafka_conf_new();

    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    lua_pushstring(L, "default_topic_options");
    lua_gettable(L, -2);
    if (lua_istable(L, -1)) {
        lua_pushnil(L);
        // stack now contains: -1 => nil; -2 => table
        while (lua_next(L, -2)) {
            // stack now contains: -1 => value; -2 => key; -3 => table
            if (!(lua_isstring(L, -1)) || !(lua_isstring(L, -2))) {
                lua_pushnil(L);
                int fail = safe_pushstring(L, "consumer config default topic options must contains only string keys and string values");
                return fail ? lua_push_error(L): 2;
            }

            const char *value = lua_tostring(L, -1);
            const char *key = lua_tostring(L, -2);
            if (rd_kafka_topic_conf_set(topic_conf, key, value, errstr, sizeof(errstr))) {
                lua_pushnil(L);
                int fail = safe_pushstring(L, errstr);
                return fail ? lua_push_error(L): 2;
            }

            // pop value, leaving original key
            lua_pop(L, 1);
            // stack now contains: -1 => key; -2 => table
        }
        // stack now contains: -1 => table
    }
    lua_pop(L, 1);
    rd_kafka_conf_set_default_topic_conf(rd_config, topic_conf);

    event_queues_t *event_queues = new_event_queues();

    lua_pushstring(L, "error_callback");
    lua_gettable(L, -2 );
    if (lua_isfunction(L, -1)) {
        event_queues->error_cb_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        event_queues->error_queue = new_queue();
        rd_kafka_conf_set_error_cb(rd_config, error_callback);
    } else {
        lua_pop(L, 1);
    }

    lua_pushstring(L, "log_callback");
    lua_gettable(L, -2 );
    if (lua_isfunction(L, -1)) {
        event_queues->log_cb_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        event_queues->log_queue = new_queue();
        rd_kafka_conf_set_log_cb(rd_config, log_callback);
    } else {
        lua_pop(L, 1);
    }

    lua_pushstring(L, "rebalance_callback");
    lua_gettable(L, -2 );
    if (lua_isfunction(L, -1)) {
        event_queues->rebalance_cb_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        event_queues->rebalance_queue = new_queue();
        rd_kafka_conf_set_rebalance_cb(rd_config, rebalance_callback);
    } else {
        lua_pop(L, 1);
    }

    rd_kafka_conf_set_opaque(rd_config, event_queues);

    lua_pushstring(L, "options");
    lua_gettable(L, -2);
    if (lua_istable(L, -1)) {
        lua_pushnil(L);
        // stack now contains: -1 => nil; -2 => table
        while (lua_next(L, -2)) {
            // stack now contains: -1 => value; -2 => key; -3 => table
            if (!(lua_isstring(L, -1)) || !(lua_isstring(L, -2))) {
                lua_pushnil(L);
                int fail = safe_pushstring(L, "consumer config options must contains only string keys and string values");
                return fail ? lua_push_error(L): 2;
            }

            const char *value = lua_tostring(L, -1);
            const char *key = lua_tostring(L, -2);
            if (rd_kafka_conf_set(rd_config, key, value, errstr, sizeof(errstr))) {
                lua_pushnil(L);
                int fail = safe_pushstring(L, errstr);
                return fail ? lua_push_error(L): 2;
            }

            // pop value, leaving original key
            lua_pop(L, 1);
            // stack now contains: -1 => key; -2 => table
        }
        // stack now contains: -1 => table
    }
    lua_pop(L, 1);

    rd_kafka_t *rd_consumer;
    if (!(rd_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, rd_config, errstr, sizeof(errstr)))) {
        lua_pushnil(L);
        int fail = safe_pushstring(L, errstr);
        return fail ? lua_push_error(L): 2;
    }

    if (rd_kafka_brokers_add(rd_consumer, brokers) == 0) {
        lua_pushnil(L);
        int fail = safe_pushstring(L, "No valid brokers specified");
        return fail ? lua_push_error(L): 2;
    }

    // FIXME:
    // rd_kafka_poll_set_consumer(rk);

    rd_kafka_queue_t *rd_msg_queue = rd_kafka_queue_get_consumer(rd_consumer);

    consumer_t *consumer;
    consumer = malloc(sizeof(consumer_t));
    consumer->rd_consumer = rd_consumer;
    consumer->topics = NULL;
    consumer->event_queues = event_queues;
    consumer->rd_msg_queue = rd_msg_queue;

    consumer_t **consumer_p = (consumer_t **)lua_newuserdata(L, sizeof(consumer));
    *consumer_p = consumer;

    luaL_getmetatable(L, consumer_label);
    lua_setmetatable(L, -2);
    return 1;
}
