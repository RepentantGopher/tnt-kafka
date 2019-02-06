#include <stdlib.h>
#include <string.h>

#include <librdkafka/rdkafka.h>

#include <common.h>
#include <queue.h>
#include <callbacks.h>

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Common callbacks handling
 */

log_msg_t *
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

error_msg_t *
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
