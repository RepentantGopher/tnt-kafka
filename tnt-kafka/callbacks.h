#ifndef TNT_KAFKA_CALLBACKS_H
#define TNT_KAFKA_CALLBACKS_H

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <librdkafka/rdkafka.h>

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

void log_callback(const rd_kafka_t *rd_kafka, int level, const char *fac, const char *buf);

typedef struct {
    int err;
    char *reason;
} error_msg_t;

error_msg_t *new_error_msg(int err, const char *reason);

void destroy_error_msg(error_msg_t *msg);

void error_callback(rd_kafka_t *UNUSED(rd_kafka), int err, const char *reason, void *opaque);

typedef struct {
    int dr_callback;
    int err;
} dr_msg_t;

dr_msg_t *new_dr_msg(int dr_callback, int err);

void destroy_dr_msg(dr_msg_t *dr_msg);

void msg_delivery_callback(rd_kafka_t *UNUSED(producer), const rd_kafka_message_t *msg, void *opaque);

typedef struct {
    queue_t *log_queue;
    int log_cb_ref;
    queue_t *error_queue;
    int error_cb_ref;
    queue_t *delivery_queue;
} event_queues_t;

event_queues_t *new_event_queues();

void destroy_event_queues(event_queues_t *event_queues);

#endif //TNT_KAFKA_CALLBACKS_H
