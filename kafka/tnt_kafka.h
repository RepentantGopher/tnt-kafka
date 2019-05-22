#ifndef TNT_KAFKA_TNT_KAFKA_H
#define TNT_KAFKA_TNT_KAFKA_H

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Entry point
 */

LUA_API int luaopen_kafka_tntkafka(lua_State *L);

#endif //TNT_KAFKA_TNT_KAFKA_H
