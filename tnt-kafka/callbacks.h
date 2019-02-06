#ifndef TNT_KAFKA_CALLBACKS_H
#define TNT_KAFKA_CALLBACKS_H

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <queue.h>

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Common callbacks handling
 */

typedef struct {
    int level;
    char *fac;
    char *buf;
} log_msg_t;

log_msg_t *new_log_msg(int level, const char *fac, const char *buf);

void destroy_log_msg(log_msg_t *msg);

typedef struct {
    int err;
    char *reason;
} error_msg_t;

error_msg_t *new_error_msg(int err, const char *reason);

void destroy_error_msg(error_msg_t *msg);

typedef struct {
    queue_t *log_queue;
    int log_cb_ref;
    queue_t *error_queue;
    int error_cb_ref;
} event_queues_t;

void log_callback(const rd_kafka_t *rd_kafka, int level, const char *fac, const char *buf);

void error_callback(rd_kafka_t *UNUSED(rd_kafka), int err, const char *reason, void *opaque);

#endif //TNT_KAFKA_CALLBACKS_H
