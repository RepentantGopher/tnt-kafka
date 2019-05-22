#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <common.h>

const char* const consumer_label = "__tnt_kafka_consumer";
const char* const consumer_msg_label = "__tnt_kafka_consumer_msg";
const char* const producer_label = "__tnt_kafka_producer";

int
save_pushstring_wrapped(struct lua_State *L) {
    char *str = (char *)lua_topointer(L, 1);
    lua_pushstring(L, str);
    return 1;
}

int
safe_pushstring(struct lua_State *L, char *str) {
    lua_pushcfunction(L, save_pushstring_wrapped);
    lua_pushlightuserdata(L, str);
    return lua_pcall(L, 1, 1, 0);
}

/**
 * Push native lua error with code -3
 */
int
lua_push_error(struct lua_State *L) {
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
