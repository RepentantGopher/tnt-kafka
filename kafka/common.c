#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <tarantool/module.h>

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
