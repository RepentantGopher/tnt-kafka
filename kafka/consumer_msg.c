#include <stdlib.h>

#include <tarantool/module.h>

#include <common.h>

#include <consumer_msg.h>

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Consumer Message
 */

msg_t *
lua_check_consumer_msg(struct lua_State *L, int index) {
    msg_t **msg_p = (msg_t **)luaL_checkudata(L, index, consumer_msg_label);
    if (msg_p == NULL || *msg_p == NULL)
        luaL_error(L, "Kafka consumer message fatal error: failed to retrieve message from lua stack!");
    return *msg_p;
}

int
lua_consumer_msg_topic(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);
    lua_pushstring(L, rd_kafka_topic_name(msg->rd_message->rkt));
    return 1;
}

int
lua_consumer_msg_partition(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    lua_pushnumber(L, (double)msg->rd_message->partition);
    return 1;
}

int
lua_consumer_msg_offset(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    luaL_pushint64(L, msg->rd_message->offset);
    return 1;
}

int
lua_consumer_msg_key(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    if (msg->rd_message->key_len <= 0 || msg->rd_message->key == NULL || ((char*)msg->rd_message->key) == NULL) {
        return 0;
    }

    lua_pushlstring(L, (char*)msg->rd_message->key, msg->rd_message->key_len);
    return 1;
}

int
lua_consumer_msg_value(struct lua_State *L) {
    msg_t *msg = lua_check_consumer_msg(L, 1);

    if (msg->rd_message->len <= 0 || msg->rd_message->payload == NULL || ((char*)msg->rd_message->payload) == NULL) {
        return 0;
    }

    lua_pushlstring(L, (char*)msg->rd_message->payload, msg->rd_message->len);
    return 1;
}

int
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

int
lua_consumer_msg_gc(struct lua_State *L) {
    msg_t **msg_p = (msg_t **)luaL_checkudata(L, 1, consumer_msg_label);
    if (msg_p && *msg_p) {
        if ((*msg_p)->rd_message != NULL) {
            rd_kafka_message_destroy((*msg_p)->rd_message);
        }
        free(*msg_p);
    }
    if (msg_p)
        *msg_p = NULL;

    return 0;
}