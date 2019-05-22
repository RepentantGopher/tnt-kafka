#ifndef TNT_KAFKA_CONSUMER_MSG_H
#define TNT_KAFKA_CONSUMER_MSG_H

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <librdkafka/rdkafka.h>

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Consumer Message
 */
typedef struct {
    const rd_kafka_message_t *rd_message;
    rd_kafka_event_t *rd_event;
} msg_t;

msg_t *lua_check_consumer_msg(struct lua_State *L, int index);

int lua_consumer_msg_topic(struct lua_State *L);

int lua_consumer_msg_partition(struct lua_State *L);

int lua_consumer_msg_offset(struct lua_State *L);

int lua_consumer_msg_key(struct lua_State *L);

int lua_consumer_msg_value(struct lua_State *L);

int lua_consumer_msg_tostring(struct lua_State *L);

int lua_consumer_msg_gc(struct lua_State *L);

#endif //TNT_KAFKA_CONSUMER_MSG_H
