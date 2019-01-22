#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include <errno.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <tarantool/module.h>

#include <librdkafka/rdkafka.h>

static const char consumer_label[] = "__tnt_kafka_consumer";
static const char producer_label[] = "__tnt_kafka_producer";

static int
lua_consumer_subscribe(struct lua_State *L) {
    luaL_checktype(L, 1, LUA_TTABLE);

    lua_pushnil(L);
    // stack now contains: -1 => nil; -2 => table
    while (lua_next(L, -2))
    {
        // stack now contains: -1 => value; -2 => key; -3 => table
        const char *value = lua_tostring(L, -1);
        printf("%s => %s\n", key, value);
        // pop value, leaving original key
        lua_pop(L, 1);
        // stack now contains: -1 => key; -2 => table
    }

    if ((err = rd_kafka_subscribe(rd_consumer, topics))) {
        box_error_raise(500, rd_kafka_err2str(err));
    }

    return 0;
}

static int
lua_consumer_poll(struct lua_State *L) {
    return 0;
}

static int
lua_consumer_store_offsets(struct lua_State *L) {
    return 0;
}

static int
lua_consumer_close(struct lua_State *L) {
    return 0;
}

static int
lua_consumer_gc(struct lua_State *L) {
    return 0;
}

static int
lua_consumer_tostring(struct lua_State *L) {
    return 0;
}

static int
lua_create_consumer(struct lua_State *L) {
    luaL_checktype(L, 1, LUA_TTABLE);

    lua_pushstring(L, "brokers");
    lua_gettable(L, -2 );
    const char *brokers = lua_tostring(L, -1);
    lua_pop(L, 1);

    rd_kafka_resp_err_t err;
    char errstr[512];

    rd_kafka_conf_t *rd_config = rd_kafka_conf_new();

    rd_kafka_t *rd_consumer;
    if (!(rd_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, rd_config, errstr, sizeof(errstr)))) {
        box_error_raise(500, errstr);
    }

    if (rd_kafka_brokers_add(rd_consumer, brokers) == 0) {
        box_error_raise(500, "No valid brokers specified");
    }

    /* Redirect rd_kafka_poll() to consumer_poll() */
    rd_kafka_poll_set_consumer(rd_consumer);

    return 0;
}

LUA_API int
luaopen_tnt_kafka(lua_State *L) {
    static const struct luaL_Reg consumer_methods [] = {
            {"subscribe", lua_consumer_subscribe},
            {"poll", lua_consumer_poll},
            {"store_offsets", lua_consumer_store_offsets},
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

	lua_newtable(L);
	static const struct luaL_Reg meta [] = {
        {"create_consumer", lua_create_consumer},
//        {"create_producer", lua_create_producer},
        {NULL, NULL}
	};
	luaL_register(L, NULL, meta);
	return 1;
}
