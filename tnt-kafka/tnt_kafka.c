#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdarg.h>
#include <pthread.h>
#include <unistd.h>

#include <errno.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <tarantool/module.h>

#include <librdkafka/rdkafka.h>

#ifdef UNUSED
#elif defined(__GNUC__)
# define UNUSED(x) UNUSED_ ## x __attribute__((unused))
#elif defined(__LCLINT__)
# define UNUSED(x) /*@unused@*/ x
#else
# define UNUSED(x) x
#endif

static const char consumer_label[] = "__tnt_kafka_consumer";
static const char consumer_msg_label[] = "__tnt_kafka_consumer_msg";
static const char producer_label[] = "__tnt_kafka_producer";

static int
save_pushstring_wrapped(struct lua_State *L) {
    char *str = (char *)lua_topointer(L, 1);
    lua_pushstring(L, str);
    return 1;
}

static int
safe_pushstring(struct lua_State *L, char *str) {
    lua_pushcfunction(L, save_pushstring_wrapped);
    lua_pushlightuserdata(L, str);
    return lua_pcall(L, 1, 1, 0);
}

/**
 * Push native lua error with code -3
 */
static int
lua_push_error(struct lua_State *L)
{
    lua_pushnumber(L, -3);
    lua_insert(L, -2);
    return 2;
}

// FIXME: suppress warning
//static ssize_t
//kafka_destroy(va_list args) {
//    rd_kafka_t *kafka = va_arg(args, rd_kafka_t *);
//
//    // waiting in background while garbage collector collects all refs
//    sleep(5);
//
//    rd_kafka_destroy(kafka);
//    while (rd_kafka_wait_destroyed(1000) == -1) {}
//    return 0;
//}

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * General thread safe queue based on licked list
 */

typedef struct queue_node_t {
    void *value;
    struct queue_node_t *next;
} queue_node_t;

typedef struct {
    pthread_mutex_t lock;
    queue_node_t *head;
    queue_node_t *tail;
} queue_t;

/**
 * Pop without locking mutex.
 * Caller must lock and unlock queue mutex by itself.
 * Use with caution!
 * @param queue
 * @return
 */
static void *
queue_lockfree_pop(queue_t *queue) {
    void *output = NULL;

    if (queue->head != NULL) {
        output = queue->head->value;
        queue_node_t *tmp = queue->head;
        queue->head = queue->head->next;
        free(tmp);
        if (queue->head == NULL) {
            queue->tail = NULL;
        }
    }

    return output;
}

static void *
queue_pop(queue_t *queue) {
    pthread_mutex_lock(&queue->lock);

    void *output = queue_lockfree_pop(queue);

    pthread_mutex_unlock(&queue->lock);

    return output;
}

static int
queue_push(queue_t *queue, void *value) {
    if (value == NULL || queue == NULL) {
        return -1;
    }

    pthread_mutex_lock(&queue->lock);

    queue_node_t *new_node;
    new_node = malloc(sizeof(queue_node_t));
    if (new_node == NULL) {
        return -1;
    }

    new_node->value = value;
    new_node->next = NULL;

    if (queue->tail != NULL) {
        queue->tail->next = new_node;
    }

    queue->tail = new_node;
    if (queue->head == NULL) {
        queue->head = new_node;
    }

    pthread_mutex_unlock(&queue->lock);

    return 0;
}

static queue_t *
new_queue() {
    queue_t *queue = malloc(sizeof(queue_t));
    if (queue == NULL) {
        return NULL;
    }

    pthread_mutex_t lock;
    if (pthread_mutex_init(&lock, NULL) != 0) {
        return NULL;
    }

    queue->lock = lock;
    queue->head = NULL;
    queue->tail = NULL;

    return queue;
}

void
destroy_queue(queue_t *queue) {
    pthread_mutex_destroy(&queue->lock);
    free(queue);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Common callbacks handling
 */

typedef struct {
    int level;
    char *fac;
    char *buf;
} log_msg_t;

static log_msg_t *
new_log_msg(int level, const char *fac, const char *buf) {
    log_msg_t *msg = malloc(sizeof(log_msg_t));
    if (msg == NULL) {
        return NULL;
    }
    msg->level = level;
    msg->fac = malloc(sizeof(char) * strlen(fac) + 1);
    strcpy(msg->fac, fac);
    msg->buf = malloc(sizeof(char) * strlen(buf) + 1);
    strcpy(msg->buf, buf);
    return msg;
}

void
destroy_log_msg(log_msg_t *msg) {
    if (msg->fac != NULL) {
        free(msg->fac);
    }
    if (msg->buf != NULL) {
        free(msg->buf);
    }
    free(msg);
}

typedef struct {
    int err;
    char *reason;
} error_msg_t;

static error_msg_t *
new_error_msg(int err, const char *reason) {
    error_msg_t *msg = malloc(sizeof(error_msg_t));
    if (msg == NULL) {
        return NULL;
    }
    msg->err = err;
    msg->reason = malloc(sizeof(char) * strlen(reason) + 1);
    strcpy(msg->reason, reason);
    return msg;
}

void
destroy_error_msg(error_msg_t *msg) {
    if (msg->reason != NULL) {
        free(msg->reason);
    }
    free(msg);
}

typedef struct {
    queue_t *log_queue;
    int log_cb_ref;
    queue_t *error_queue;
    int error_cb_ref;
} event_queues_t;

void
log_callback(const rd_kafka_t *rd_kafka, int level, const char *fac, const char *buf) {
    event_queues_t *event_queues = rd_kafka_opaque(rd_kafka);
    if (event_queues != NULL && event_queues->log_queue != NULL) {
        log_msg_t *msg = new_log_msg(level, fac, buf);
        if (msg != NULL) {
            if (queue_push(event_queues->log_queue, msg) != 0) {
                destroy_log_msg(msg);
            }
        }
    }
}

void
error_callback(rd_kafka_t *UNUSED(rd_kafka), int err, const char *reason, void *opaque) {
    event_queues_t *event_queues = opaque;
    if (event_queues != NULL && event_queues->error_queue != NULL) {
        error_msg_t *msg = new_error_msg(err, reason);
        if (msg != NULL) {
            if (queue_push(event_queues->error_queue, msg) != 0) {
                destroy_error_msg(msg);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Consumer Message
 */
typedef struct {
    const rd_kafka_message_t *rd_message;
    rd_kafka_event_t *rd_event;
} msg_t;


static inline msg_t *
lua_check_consumer_msg(struct lua_State *L, int index) {
    msg_t **msg_p = (msg_t **)luaL_checkudata(L, index, consumer_msg_label);
    if (msg_p == NULL || *msg_p == NULL)
        luaL_error(L, "Kafka consumer message fatal error: failed to retrieve message from lua stack!");
    return *msg_p;
}

static int
lua_consumer_msg_topic(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    const char *const_topic = rd_kafka_topic_name(msg->rd_message->rkt);
    char topic[sizeof(const_topic)];
    strcpy(topic, const_topic);
    int fail = safe_pushstring(L, topic);
    return fail ? lua_push_error(L): 1;
}

static int
lua_consumer_msg_partition(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    lua_pushnumber(L, (double)msg->rd_message->partition);
    return 1;
}

static int
lua_consumer_msg_offset(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    luaL_pushint64(L, msg->rd_message->offset);
    return 1;
}

static int
lua_consumer_msg_key(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    if (msg->rd_message->key_len <= 0 || msg->rd_message->key == NULL || ((char*)msg->rd_message->key) == NULL) {
        return 0;
    }

    lua_pushlstring(L, (char*)msg->rd_message->key, msg->rd_message->key_len);
    return 1;
}

static int
lua_consumer_msg_value(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    if (msg->rd_message->len <= 0 || msg->rd_message->payload == NULL || ((char*)msg->rd_message->payload) == NULL) {
        return 0;
    }

    lua_pushlstring(L, (char*)msg->rd_message->payload, msg->rd_message->len);
    return 1;
}

static int
lua_consumer_msg_tostring(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    size_t key_len = msg->rd_message->key_len <= 0 ? 5: msg->rd_message->key_len + 1;
    char key[key_len];

    if (msg->rd_message->key_len <= 0 || msg->rd_message->key == NULL || ((char*)msg->rd_message->key) == NULL) {
        strncpy(key, "NULL", 5);
    } else {
        strncpy(key, msg->rd_message->key, msg->rd_message->key_len + 1);
        if (key[msg->rd_message->key_len] != '\0') {
            key[msg->rd_message->key_len] = '\0';
        }
    }

    size_t value_len = msg->rd_message->len <= 0 ? 5: msg->rd_message->len + 1;
    char value[value_len];

    if (msg->rd_message->len <= 0 || msg->rd_message->payload == NULL || ((char*)msg->rd_message->payload) == NULL) {
        strncpy(value, "NULL", 5);
    } else {
        strncpy(value, msg->rd_message->payload, msg->rd_message->len + 1);
        if (value[msg->rd_message->len] != '\0') {
            value[msg->rd_message->len] = '\0';
        }
    }

    lua_pushfstring(L,
                    "Kafka Consumer Message: topic=%s partition=%d offset=%d key=%s value=%s",
                    rd_kafka_topic_name(msg->rd_message->rkt),
                    msg->rd_message->partition,
                    msg->rd_message->offset,
                    key,
                    value);
    return 1;
}

static int
lua_consumer_msg_gc(struct lua_State *L) {
    msg_t **msg_p = (msg_t **)luaL_checkudata(L, 1, consumer_msg_label);
    if (msg_p && *msg_p) {
        rd_kafka_event_destroy((*msg_p)->rd_event);
        free(*msg_p);
    }
    if (msg_p)
        *msg_p = NULL;

    return 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Consumer
 */

typedef struct {
    rd_kafka_t                      *rd_consumer;
    rd_kafka_topic_partition_list_t *topics;
    rd_kafka_queue_t                *rd_msg_queue;
    event_queues_t                  *event_queues;
} consumer_t;

static inline consumer_t *
lua_check_consumer(struct lua_State *L, int index) {
    consumer_t **consumer_p = (consumer_t **)luaL_checkudata(L, index, consumer_label);
    if (consumer_p == NULL || *consumer_p == NULL)
        luaL_error(L, "Kafka consumer fatal error: failed to retrieve consumer from lua stack!");
    return *consumer_p;
}

static int
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

static int
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

static int
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

static int
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

static int
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

static int
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

static int
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

static int
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
        if (consumer->event_queues->log_queue != NULL) {
            log_msg_t *msg = NULL;
            while (true) {
                msg = queue_pop(consumer->event_queues->log_queue);
                if (msg == NULL) {
                    break;
                }
                destroy_log_msg(msg);
            }
            destroy_queue(consumer->event_queues->log_queue);
        }
        if (consumer->event_queues->error_queue != NULL) {
            error_msg_t *msg = NULL;
            while (true) {
                msg = queue_pop(consumer->event_queues->error_queue);
                if (msg == NULL) {
                    break;
                }
                destroy_error_msg(msg);
            }
            destroy_queue(consumer->event_queues->error_queue);
        }

        if (consumer->event_queues->error_cb_ref != LUA_REFNIL) {
            luaL_unref(L, LUA_REGISTRYINDEX, consumer->event_queues->error_cb_ref);
        }

        if (consumer->event_queues->log_cb_ref != LUA_REFNIL) {
            luaL_unref(L, LUA_REGISTRYINDEX, consumer->event_queues->log_cb_ref);
        }

        free(consumer->event_queues);
    }

    if (consumer->rd_consumer != NULL) {
        /* Destroy handle */
        // FIXME: kafka_destroy hangs forever
//        coio_call(kafka_destroy, consumer->rd_consumer);
    }

    free(consumer);

    return err;
}

static int
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

static int
lua_consumer_gc(struct lua_State *L) {
    consumer_t **consumer_p = (consumer_t **)luaL_checkudata(L, 1, consumer_label);
    if (consumer_p && *consumer_p) {
        consumer_close(L, *consumer_p);
    }
    if (consumer_p)
        *consumer_p = NULL;
    return 0;
}

static int
lua_create_consumer(struct lua_State *L) {
    if (lua_gettop(L) != 1 || !lua_istable(L, 1))
        luaL_error(L, "Usage: consumer, err = create_consumer(conf)");

    char errstr[512];
    rd_kafka_conf_t *rd_config = rd_kafka_conf_new();
    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    rd_kafka_conf_set_default_topic_conf(rd_config, topic_conf);

    lua_pushstring(L, "brokers");
    lua_gettable(L, -2 );
    const char *brokers = lua_tostring(L, -1);
    lua_pop(L, 1);
    if (brokers == NULL) {
        lua_pushnil(L);
        int fail = safe_pushstring(L, "consumer config table must have non nil key 'brokers' which contains string");
        return fail ? lua_push_error(L): 2;
    }

    int error_cb_ref = LUA_REFNIL;
    queue_t *error_queue = NULL;
    lua_pushstring(L, "error_callback");
    lua_gettable(L, -2 );
    if (lua_isfunction(L, -1)) {
        error_cb_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        error_queue = new_queue();
        rd_kafka_conf_set_error_cb(rd_config, error_callback);
    } else {
        lua_pop(L, 1);
    }

    int log_cb_ref = LUA_REFNIL;
    queue_t *log_queue = NULL;
    lua_pushstring(L, "log_callback");
    lua_gettable(L, -2 );
    if (lua_isfunction(L, -1)) {
        log_cb_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        log_queue = new_queue();
        rd_kafka_conf_set_log_cb(rd_config, log_callback);
    } else {
        lua_pop(L, 1);
    }

    event_queues_t *event_queues = malloc(sizeof(event_queues_t));
    event_queues->error_queue = error_queue;
    event_queues->error_cb_ref = error_cb_ref;
    event_queues->log_queue = log_queue;
    event_queues->log_cb_ref = log_cb_ref;

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

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Producer
 */

typedef struct {
    rd_kafka_topic_t **elements;
    int32_t count;
    int32_t capacity;
} producer_topics_t;

static producer_topics_t *
new_producer_topics(int32_t capacity) {
    rd_kafka_topic_t **elements;
    elements = malloc(sizeof(rd_kafka_topic_t *) * capacity);

    producer_topics_t *topics;
    topics = malloc(sizeof(producer_topics_t));
    topics->capacity = capacity;
    topics->count = 0;
    topics->elements = elements;

    return topics;
}

static int
add_producer_topics(producer_topics_t *topics, rd_kafka_topic_t *element) {
    if (topics->count >= topics->capacity) {
        rd_kafka_topic_t **new_elements = realloc(topics->elements, sizeof(rd_kafka_topic_t *) * topics->capacity * 2);
        if (new_elements == NULL) {
            printf("realloc failed to relloc rd_kafka_topic_t array.");
            return 1;
        }
        topics->elements = new_elements;
        topics->capacity *= 2;
    }
    topics->elements[topics->count++] = element;
    return 0;
}

static rd_kafka_topic_t *
find_producer_topic_by_name(producer_topics_t *topics, const char *name) {
    rd_kafka_topic_t *topic;
    for (int i = 0; i < topics->count; i++) {
        topic = topics->elements[i];
        if (strcmp(rd_kafka_topic_name(topic), name) == 0) {
            return topic;
        }
    }
    return NULL;
}

static void
destroy_producer_topics(producer_topics_t *topics) {
    rd_kafka_topic_t **topic_p;
    rd_kafka_topic_t **end = topics->elements + topics->count;
    for (topic_p = topics->elements; topic_p < end; topic_p++) {
        rd_kafka_topic_destroy(*topic_p);
    }

    free(topics->elements);
    free(topics);
}

// Cause `rd_kafka_conf_set_events(rd_config, RD_KAFKA_EVENT_DR)` produces segfault with queue api, we are forced to
// implement our own thread safe queue to push incoming events from callback thread to lua thread.
typedef struct {
    int dr_callback;
    int err;
} queue_element_t;

queue_element_t *
new_queue_element(int dr_callback, int err) {
    queue_element_t *element;
    element = malloc(sizeof(queue_element_t));
    element->dr_callback = dr_callback;
    element->err = err;
    return element;
}

void
destroy_queue_element(queue_element_t *element) {
    free(element);
}

typedef struct {
    rd_kafka_conf_t *rd_config;
    rd_kafka_t *rd_producer;
    producer_topics_t *topics;
    queue_t *delivery_queue;
} producer_t;

static inline producer_t *
lua_check_producer(struct lua_State *L, int index) {
    producer_t **producer_p = (producer_t **)luaL_checkudata(L, index, producer_label);
    if (producer_p == NULL || *producer_p == NULL)
        luaL_error(L, "Kafka consumer fatal error: failed to retrieve producer from lua stack!");
    return *producer_p;
}

static int
lua_producer_tostring(struct lua_State *L) {
    producer_t *producer = lua_check_producer(L, 1);
    lua_pushfstring(L, "Kafka Producer: %p", producer);
    return 1;
}

static ssize_t
producer_poll(va_list args) {
    rd_kafka_t *rd_producer = va_arg(args, rd_kafka_t *);
    rd_kafka_poll(rd_producer, 1000);
    return 0;
}

static int
lua_producer_poll(struct lua_State *L) {
    if (lua_gettop(L) != 1)
        luaL_error(L, "Usage: err = producer:poll()");

    producer_t *producer = lua_check_producer(L, 1);
    if (coio_call(producer_poll, producer->rd_producer) == -1) {
        lua_pushstring(L, "unexpected error on producer poll");
        return 1;
    }
    return 0;
}

static int
lua_producer_msg_delivery_poll(struct lua_State *L) {
    if (lua_gettop(L) != 2)
        luaL_error(L, "Usage: count, err = producer:msg_delivery_poll(events_limit)");

    producer_t *producer = lua_check_producer(L, 1);

    int events_limit = lua_tonumber(L, 2);
    int callbacks_count = 0;
    char *err_str = NULL;
    queue_element_t *element = NULL;

    pthread_mutex_lock(&producer->delivery_queue->lock);

    while (events_limit > callbacks_count) {
        element = queue_lockfree_pop(producer->delivery_queue);
        if (element == NULL) {
            break;
        }
        callbacks_count += 1;
        lua_rawgeti(L, LUA_REGISTRYINDEX, element->dr_callback);
        if (element->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            lua_pushstring(L, (char *)rd_kafka_err2str(element->err));
        } else {
            lua_pushnil(L);
        }
        /* do the call (1 arguments, 0 result) */
        if (lua_pcall(L, 1, 0, 0) != 0) {
            err_str = (char *)lua_tostring(L, -1);
        }
        luaL_unref(L, LUA_REGISTRYINDEX, element->dr_callback);
        destroy_queue_element(element);
        if (err_str != NULL) {
            break;
        }
    }

    pthread_mutex_unlock(&producer->delivery_queue->lock);

    lua_pushnumber(L, (double)callbacks_count);
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

static int
lua_producer_produce(struct lua_State *L) {
    if (lua_gettop(L) != 2 || !lua_istable(L, 2))
        luaL_error(L, "Usage: err = producer:produce(msg)");

    lua_pushstring(L, "topic");
    lua_gettable(L, -2 );
    const char *topic = lua_tostring(L, -1);
    lua_pop(L, 1);
    if (topic == NULL) {
        int fail = safe_pushstring(L, "producer message must contains non nil 'topic' key");
        return fail ? lua_push_error(L): 1;
    }

    lua_pushstring(L, "key");
    lua_gettable(L, -2 );
    // rd_kafka will copy key so no need to worry about this cast
    char *key = (char *)lua_tostring(L, -1);
    lua_pop(L, 1);

    size_t key_len = key != NULL ? strlen(key) : 0;

    lua_pushstring(L, "value");
    lua_gettable(L, -2 );
    // rd_kafka will copy value so no need to worry about this cast
    char *value = (char *)lua_tostring(L, -1);
    lua_pop(L, 1);

    size_t value_len = value != NULL ? strlen(value) : 0;

    if (key == NULL && value == NULL) {
        int fail = safe_pushstring(L, "producer message must contains non nil key or value");
        return fail ? lua_push_error(L): 1;
    }

    // create delivery callback queue if got msg id
    queue_element_t *element = NULL;
    lua_pushstring(L, "dr_callback");
    lua_gettable(L, -2 );
    if (lua_isfunction(L, -1)) {
        element = new_queue_element(luaL_ref(L, LUA_REGISTRYINDEX), RD_KAFKA_RESP_ERR_NO_ERROR);
        if (element == NULL) {
            int fail = safe_pushstring(L, "failed to create callback message");
            return fail ? lua_push_error(L): 1;
        }
    } else {
        lua_pop(L, 1);
    }

    // pop msg
    lua_pop(L, 1);

    producer_t *producer = lua_check_producer(L, 1);
    rd_kafka_topic_t *rd_topic = find_producer_topic_by_name(producer->topics, topic);
    if (rd_topic == NULL) {
        rd_topic = rd_kafka_topic_new(producer->rd_producer, topic, NULL);
        if (rd_topic == NULL) {
            const char *const_err_str = rd_kafka_err2str(rd_kafka_errno2err(errno));
            char err_str[512];
            strcpy(err_str, const_err_str);
            int fail = safe_pushstring(L, err_str);
            return fail ? lua_push_error(L): 1;
        }
        if (add_producer_topics(producer->topics, rd_topic) != 0) {
            int fail = safe_pushstring(L, "Unexpected error: failed to add new topic to topic list!");
            return fail ? lua_push_error(L): 1;
        }
    }

    if (rd_kafka_produce(rd_topic, -1, RD_KAFKA_MSG_F_COPY, value, value_len, key, key_len, element) == -1) {
        const char *const_err_str = rd_kafka_err2str(rd_kafka_errno2err(errno));
        char err_str[512];
        strcpy(err_str, const_err_str);
        int fail = safe_pushstring(L, err_str);
        return fail ? lua_push_error(L): 1;
    }
    return 0;
}

static ssize_t
producer_flush(va_list args) {
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
    rd_kafka_t *rd_producer = va_arg(args, rd_kafka_t *);
    while (true) {
        err = rd_kafka_flush(rd_producer, 1000);
        if (err != RD_KAFKA_RESP_ERR__TIMED_OUT) {
            break;
        }
    }
    return 0;
}

static rd_kafka_resp_err_t
producer_close(producer_t *producer) {
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

    if (producer->rd_producer != NULL) {
        coio_call(producer_flush, producer->rd_producer);
    }

    if (producer->topics != NULL) {
        destroy_producer_topics(producer->topics);
    }

    if (producer->delivery_queue != NULL) {
        destroy_queue(producer->delivery_queue);
    }

    if (producer->rd_producer != NULL) {
        // FIXME: if instance of consumer exists then kafka_destroy always hangs forever
        /* Destroy handle */
//        coio_call(kafka_destroy, producer->rd_producer);
    }

    free(producer);
    return err;
}

static int
lua_producer_close(struct lua_State *L) {
    producer_t **producer_p = (producer_t **)luaL_checkudata(L, 1, producer_label);
    if (producer_p == NULL || *producer_p == NULL) {
        lua_pushboolean(L, 0);
        return 1;
    }

    rd_kafka_resp_err_t err = producer_close(*producer_p);
    if (err) {
        lua_pushboolean(L, 1);

        const char *const_err_str = rd_kafka_err2str(err);
        char err_str[512];
        strcpy(err_str, const_err_str);
        int fail = safe_pushstring(L, err_str);
        return fail ? lua_push_error(L): 2;
    }

    *producer_p = NULL;
    lua_pushboolean(L, 1);
    return 1;
}

static int
lua_producer_gc(struct lua_State *L) {
    producer_t **producer_p = (producer_t **)luaL_checkudata(L, 1, producer_label);
    if (producer_p && *producer_p) {
        producer_close(*producer_p);
    }
    if (producer_p)
        *producer_p = NULL;
    return 0;
}

void
msg_delivery_callback(rd_kafka_t *UNUSED(producer), const rd_kafka_message_t *msg, void *opaque) {
    if (msg->_private != NULL) {
        queue_element_t *element = msg->_private;
        queue_t *queue = opaque;
        if (element != NULL) {
            if (msg->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                element->err = msg->err;
            }
            queue_push(queue, element);
        }
    }
}

static int
lua_create_producer(struct lua_State *L) {
    if (lua_gettop(L) != 1 || !lua_istable(L, 1))
        luaL_error(L, "Usage: producer, err = create_producer(conf)");

    lua_pushstring(L, "brokers");
    lua_gettable(L, -2 );
    const char *brokers = lua_tostring(L, -1);
    lua_pop(L, 1);
    if (brokers == NULL) {
        lua_pushnil(L);
        int fail = safe_pushstring(L, "producer config table must have non nil key 'brokers' which contains string");
        return fail ? lua_push_error(L): 2;
    }

    char errstr[512];

    rd_kafka_conf_t *rd_config = rd_kafka_conf_new();

    queue_t *delivery_queue = new_queue();
    // queue now accessible from callback
    rd_kafka_conf_set_opaque(rd_config, delivery_queue);

    rd_kafka_conf_set_dr_msg_cb(rd_config, msg_delivery_callback);

    // enabling delivering events
//    rd_kafka_conf_set_events(rd_config, RD_KAFKA_EVENT_STATS | RD_KAFKA_EVENT_DR);

    lua_pushstring(L, "options");
    lua_gettable(L, -2 );
    if (lua_istable(L, -1)) {
        lua_pushnil(L);
        // stack now contains: -1 => nil; -2 => table
        while (lua_next(L, -2)) {
            // stack now contains: -1 => value; -2 => key; -3 => table
            if (!(lua_isstring(L, -1)) || !(lua_isstring(L, -2))) {
                lua_pushnil(L);
                int fail = safe_pushstring(L, "producer config options must contains only string keys and string values");
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

    rd_kafka_t *rd_producer;
    if (!(rd_producer = rd_kafka_new(RD_KAFKA_PRODUCER, rd_config, errstr, sizeof(errstr)))) {
        lua_pushnil(L);
        int fail = safe_pushstring(L, errstr);
        return fail ? lua_push_error(L): 2;
    }

    if (rd_kafka_brokers_add(rd_producer, brokers) == 0) {
        lua_pushnil(L);
        int fail = safe_pushstring(L, "No valid brokers specified");
        return fail ? lua_push_error(L): 2;
    }

    producer_t *producer;
    producer = malloc(sizeof(producer_t));
    producer->rd_config = rd_config;
    producer->rd_producer = rd_producer;
    producer->topics = new_producer_topics(256);
    producer->delivery_queue = delivery_queue;

    producer_t **producer_p = (producer_t **)lua_newuserdata(L, sizeof(producer));
    *producer_p = producer;

    luaL_getmetatable(L, producer_label);
    lua_setmetatable(L, -2);
    return 1;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Entry point
 */

LUA_API int
luaopen_kafka_tntkafka(lua_State *L) {
    static const struct luaL_Reg consumer_methods [] = {
            {"subscribe", lua_consumer_subscribe},
            {"unsubscribe", lua_consumer_unsubscribe},
            {"poll", lua_consumer_poll},
            {"poll_msg", lua_consumer_poll_msg},
            {"poll_logs", lua_consumer_poll_logs},
            {"poll_errors", lua_consumer_poll_errors},
            {"store_offset", lua_consumer_store_offset},
            {"close", lua_consumer_close},
            {"__tostring", lua_consumer_tostring},
            {"__gc", lua_consumer_gc},
            {NULL, NULL}
    };

    luaL_newmetatable(L, consumer_label);
    lua_pushvalue(L, -1);
    luaL_register(L, NULL, consumer_methods);
    lua_setfield(L, -2, "__index");
    lua_pushstring(L, consumer_label);
    lua_setfield(L, -2, "__metatable");
    lua_pop(L, 1);

    static const struct luaL_Reg consumer_msg_methods [] = {
            {"topic", lua_consumer_msg_topic},
            {"partition", lua_consumer_msg_partition},
            {"offset", lua_consumer_msg_offset},
            {"key", lua_consumer_msg_key},
            {"value", lua_consumer_msg_value},
            {"__tostring", lua_consumer_msg_tostring},
            {"__gc", lua_consumer_msg_gc},
            {NULL, NULL}
    };

    luaL_newmetatable(L, consumer_msg_label);
    lua_pushvalue(L, -1);
    luaL_register(L, NULL, consumer_msg_methods);
    lua_setfield(L, -2, "__index");
    lua_pushstring(L, consumer_msg_label);
    lua_setfield(L, -2, "__metatable");
    lua_pop(L, 1);

    static const struct luaL_Reg producer_methods [] = {
            {"poll", lua_producer_poll},
            {"produce", lua_producer_produce},
            {"msg_delivery_poll", lua_producer_msg_delivery_poll},
            {"close", lua_producer_close},
            {"__tostring", lua_producer_tostring},
            {"__gc", lua_producer_gc},
            {NULL, NULL}
    };

    luaL_newmetatable(L, producer_label);
    lua_pushvalue(L, -1);
    luaL_register(L, NULL, producer_methods);
    lua_setfield(L, -2, "__index");
    lua_pushstring(L, producer_label);
    lua_setfield(L, -2, "__metatable");
    lua_pop(L, 1);

	lua_newtable(L);
	static const struct luaL_Reg meta [] = {
        {"create_consumer", lua_create_consumer},
        {"create_producer", lua_create_producer},
        {NULL, NULL}
	};
	luaL_register(L, NULL, meta);
	return 1;
}
