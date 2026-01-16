/*
 * kafka_shim.c - C shim for interfacing Lean with librdkafka
 *
 * This file provides Lean-compatible FFI wrappers around librdkafka functions.
 * librdkafka must be installed on the system.
 */

#include <lean/lean.h>
#include <librdkafka/rdkafka.h>
#include <string.h>
#include <stdlib.h>

/* ============================================================================
 * Helper functions for Option types
 * ============================================================================ */

static inline lean_obj_res mk_option_none(void) {
    return lean_box(0);
}

static inline lean_obj_res mk_option_some(lean_obj_arg a) {
    lean_object *obj = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(obj, 0, a);
    return obj;
}

/* ============================================================================
 * Helper functions for Result types (Except)
 * In Lean 4, Except is:
 *   - error e: constructor 0, one field
 *   - ok a: constructor 1, one field
 * ============================================================================ */

static inline lean_obj_res mk_except_error(lean_obj_arg e) {
    lean_object *obj = lean_alloc_ctor(0, 1, 0);
    lean_ctor_set(obj, 0, e);
    return obj;
}

static inline lean_obj_res mk_except_ok(lean_obj_arg a) {
    lean_object *obj = lean_alloc_ctor(1, 1, 0);
    lean_ctor_set(obj, 0, a);
    return obj;
}

/* ============================================================================
 * External classes for librdkafka pointers
 * ============================================================================ */

/* Configuration */
static void kafka_conf_finalizer(void *ptr) {
    if (ptr) {
        rd_kafka_conf_destroy((rd_kafka_conf_t *)ptr);
    }
}

static lean_external_class *g_kafka_conf_class = NULL;

static lean_external_class *get_kafka_conf_class(void) {
    if (g_kafka_conf_class == NULL) {
        g_kafka_conf_class = lean_register_external_class(kafka_conf_finalizer, NULL);
    }
    return g_kafka_conf_class;
}

/* Topic configuration */
static void kafka_topic_conf_finalizer(void *ptr) {
    if (ptr) {
        rd_kafka_topic_conf_destroy((rd_kafka_topic_conf_t *)ptr);
    }
}

static lean_external_class *g_kafka_topic_conf_class = NULL;

static lean_external_class *get_kafka_topic_conf_class(void) {
    if (g_kafka_topic_conf_class == NULL) {
        g_kafka_topic_conf_class = lean_register_external_class(kafka_topic_conf_finalizer, NULL);
    }
    return g_kafka_topic_conf_class;
}

/* Kafka handle (producer/consumer) */
static void kafka_handle_finalizer(void *ptr) {
    if (ptr) {
        rd_kafka_destroy((rd_kafka_t *)ptr);
    }
}

static lean_external_class *g_kafka_handle_class = NULL;

static lean_external_class *get_kafka_handle_class(void) {
    if (g_kafka_handle_class == NULL) {
        g_kafka_handle_class = lean_register_external_class(kafka_handle_finalizer, NULL);
    }
    return g_kafka_handle_class;
}

/* Topic handle */
static void kafka_topic_finalizer(void *ptr) {
    if (ptr) {
        rd_kafka_topic_destroy((rd_kafka_topic_t *)ptr);
    }
}

static lean_external_class *g_kafka_topic_class = NULL;

static lean_external_class *get_kafka_topic_class(void) {
    if (g_kafka_topic_class == NULL) {
        g_kafka_topic_class = lean_register_external_class(kafka_topic_finalizer, NULL);
    }
    return g_kafka_topic_class;
}

/* Message - not owned, just borrowed during callback */
static lean_external_class *g_kafka_message_class = NULL;

static lean_external_class *get_kafka_message_class(void) {
    if (g_kafka_message_class == NULL) {
        g_kafka_message_class = lean_register_external_class(NULL, NULL);
    }
    return g_kafka_message_class;
}

/* Headers */
static void kafka_headers_finalizer(void *ptr) {
    if (ptr) {
        rd_kafka_headers_destroy((rd_kafka_headers_t *)ptr);
    }
}

static lean_external_class *g_kafka_headers_class = NULL;

static lean_external_class *get_kafka_headers_class(void) {
    if (g_kafka_headers_class == NULL) {
        g_kafka_headers_class = lean_register_external_class(kafka_headers_finalizer, NULL);
    }
    return g_kafka_headers_class;
}

/* Topic partition list */
static void kafka_topic_partition_list_finalizer(void *ptr) {
    if (ptr) {
        rd_kafka_topic_partition_list_destroy((rd_kafka_topic_partition_list_t *)ptr);
    }
}

static lean_external_class *g_kafka_topic_partition_list_class = NULL;

static lean_external_class *get_kafka_topic_partition_list_class(void) {
    if (g_kafka_topic_partition_list_class == NULL) {
        g_kafka_topic_partition_list_class = lean_register_external_class(kafka_topic_partition_list_finalizer, NULL);
    }
    return g_kafka_topic_partition_list_class;
}

/* ============================================================================
 * Version and Error Functions
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_version(lean_obj_arg world) {
    const char *version = rd_kafka_version_str();
    return lean_io_result_mk_ok(lean_mk_string(version));
}

LEAN_EXPORT lean_obj_res lean_kafka_err2str(int32_t err, lean_obj_arg world) {
    const char *str = rd_kafka_err2str((rd_kafka_resp_err_t)err);
    return lean_io_result_mk_ok(lean_mk_string(str));
}

LEAN_EXPORT lean_obj_res lean_kafka_last_error(lean_obj_arg world) {
    rd_kafka_resp_err_t err = rd_kafka_last_error();
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* ============================================================================
 * Configuration
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_conf_new(lean_obj_arg world) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (conf == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    lean_object *obj = lean_alloc_external(get_kafka_conf_class(), (void *)conf);
    return lean_io_result_mk_ok(mk_option_some(obj));
}

LEAN_EXPORT lean_obj_res lean_kafka_conf_dup(b_lean_obj_arg conf_obj, lean_obj_arg world) {
    rd_kafka_conf_t *conf = (rd_kafka_conf_t *)lean_get_external_data(conf_obj);
    rd_kafka_conf_t *dup = rd_kafka_conf_dup(conf);
    if (dup == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    lean_object *obj = lean_alloc_external(get_kafka_conf_class(), (void *)dup);
    return lean_io_result_mk_ok(mk_option_some(obj));
}

LEAN_EXPORT lean_obj_res lean_kafka_conf_set(b_lean_obj_arg conf_obj,
                                              b_lean_obj_arg name,
                                              b_lean_obj_arg value,
                                              lean_obj_arg world) {
    rd_kafka_conf_t *conf = (rd_kafka_conf_t *)lean_get_external_data(conf_obj);
    const char *name_str = lean_string_cstr(name);
    const char *value_str = lean_string_cstr(value);
    char errstr[512];

    rd_kafka_conf_res_t res = rd_kafka_conf_set(conf, name_str, value_str, errstr, sizeof(errstr));

    if (res != RD_KAFKA_CONF_OK) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(errstr)));
    }
    return lean_io_result_mk_ok(mk_except_ok(lean_box(0)));
}

LEAN_EXPORT lean_obj_res lean_kafka_conf_get(b_lean_obj_arg conf_obj,
                                              b_lean_obj_arg name,
                                              lean_obj_arg world) {
    rd_kafka_conf_t *conf = (rd_kafka_conf_t *)lean_get_external_data(conf_obj);
    const char *name_str = lean_string_cstr(name);
    char value[512];
    size_t value_size = sizeof(value);

    rd_kafka_conf_res_t res = rd_kafka_conf_get(conf, name_str, value, &value_size);

    if (res != RD_KAFKA_CONF_OK) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    return lean_io_result_mk_ok(mk_option_some(lean_mk_string(value)));
}

/* ============================================================================
 * Topic Configuration
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_topic_conf_new(lean_obj_arg world) {
    rd_kafka_topic_conf_t *conf = rd_kafka_topic_conf_new();
    if (conf == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    lean_object *obj = lean_alloc_external(get_kafka_topic_conf_class(), (void *)conf);
    return lean_io_result_mk_ok(mk_option_some(obj));
}

LEAN_EXPORT lean_obj_res lean_kafka_topic_conf_set(b_lean_obj_arg conf_obj,
                                                    b_lean_obj_arg name,
                                                    b_lean_obj_arg value,
                                                    lean_obj_arg world) {
    rd_kafka_topic_conf_t *conf = (rd_kafka_topic_conf_t *)lean_get_external_data(conf_obj);
    const char *name_str = lean_string_cstr(name);
    const char *value_str = lean_string_cstr(value);
    char errstr[512];

    rd_kafka_conf_res_t res = rd_kafka_topic_conf_set(conf, name_str, value_str, errstr, sizeof(errstr));

    if (res != RD_KAFKA_CONF_OK) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(errstr)));
    }
    return lean_io_result_mk_ok(mk_except_ok(lean_box(0)));
}

/* ============================================================================
 * Producer
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_new_producer(lean_obj_arg conf_obj, lean_obj_arg world) {
    /* Note: rd_kafka_new takes ownership of conf on success */
    rd_kafka_conf_t *conf = (rd_kafka_conf_t *)lean_get_external_data(conf_obj);
    rd_kafka_conf_t *conf_dup = rd_kafka_conf_dup(conf);

    char errstr[512];
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf_dup, errstr, sizeof(errstr));

    if (rk == NULL) {
        rd_kafka_conf_destroy(conf_dup);
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(errstr)));
    }

    lean_object *obj = lean_alloc_external(get_kafka_handle_class(), (void *)rk);
    return lean_io_result_mk_ok(mk_except_ok(obj));
}

LEAN_EXPORT lean_obj_res lean_kafka_produce(b_lean_obj_arg rk_obj,
                                             b_lean_obj_arg topic,
                                             int32_t partition,
                                             b_lean_obj_arg payload,
                                             b_lean_obj_arg key,
                                             lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const char *topic_str = lean_string_cstr(topic);

    /* Get payload data */
    size_t payload_len = lean_sarray_size(payload);
    const uint8_t *payload_data = lean_sarray_cptr(payload);

    /* Get key data (may be empty) */
    size_t key_len = lean_sarray_size(key);
    const uint8_t *key_data = key_len > 0 ? lean_sarray_cptr(key) : NULL;

    rd_kafka_resp_err_t err = rd_kafka_producev(
        rk,
        RD_KAFKA_V_TOPIC(topic_str),
        RD_KAFKA_V_PARTITION(partition),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void *)payload_data, payload_len),
        RD_KAFKA_V_KEY((void *)key_data, key_len),
        RD_KAFKA_V_END
    );

    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

LEAN_EXPORT lean_obj_res lean_kafka_flush(b_lean_obj_arg rk_obj, int32_t timeout_ms, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_resp_err_t err = rd_kafka_flush(rk, timeout_ms);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

LEAN_EXPORT lean_obj_res lean_kafka_poll(b_lean_obj_arg rk_obj, int32_t timeout_ms, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    int events = rd_kafka_poll(rk, timeout_ms);
    return lean_io_result_mk_ok(lean_box((uint32_t)events));
}

LEAN_EXPORT lean_obj_res lean_kafka_outq_len(b_lean_obj_arg rk_obj, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    int len = rd_kafka_outq_len(rk);
    return lean_io_result_mk_ok(lean_box((uint32_t)len));
}

/* ============================================================================
 * Consumer
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_new_consumer(lean_obj_arg conf_obj, lean_obj_arg world) {
    rd_kafka_conf_t *conf = (rd_kafka_conf_t *)lean_get_external_data(conf_obj);
    rd_kafka_conf_t *conf_dup = rd_kafka_conf_dup(conf);

    char errstr[512];
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf_dup, errstr, sizeof(errstr));

    if (rk == NULL) {
        rd_kafka_conf_destroy(conf_dup);
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(errstr)));
    }

    /* Redirect rd_kafka_poll() to consumer_poll() */
    rd_kafka_poll_set_consumer(rk);

    lean_object *obj = lean_alloc_external(get_kafka_handle_class(), (void *)rk);
    return lean_io_result_mk_ok(mk_except_ok(obj));
}

LEAN_EXPORT lean_obj_res lean_kafka_subscribe(b_lean_obj_arg rk_obj,
                                               b_lean_obj_arg topics,
                                               lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);

    /* topics is an Array String */
    size_t num_topics = lean_array_size(topics);
    rd_kafka_topic_partition_list_t *subscription = rd_kafka_topic_partition_list_new((int)num_topics);

    for (size_t i = 0; i < num_topics; i++) {
        lean_object *topic_obj = lean_array_get_core(topics, i);
        const char *topic_str = lean_string_cstr(topic_obj);
        rd_kafka_topic_partition_list_add(subscription, topic_str, RD_KAFKA_PARTITION_UA);
    }

    rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, subscription);
    rd_kafka_topic_partition_list_destroy(subscription);

    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

LEAN_EXPORT lean_obj_res lean_kafka_unsubscribe(b_lean_obj_arg rk_obj, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_resp_err_t err = rd_kafka_unsubscribe(rk);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* Consumer poll - returns message data as a Lean structure */
LEAN_EXPORT lean_obj_res lean_kafka_consumer_poll(b_lean_obj_arg rk_obj,
                                                   int32_t timeout_ms,
                                                   lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_message_t *msg = rd_kafka_consumer_poll(rk, timeout_ms);

    if (msg == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }

    /* Build a Lean structure with message data:
     * Structure KafkaMessage where
     *   topic : String
     *   partition : Int32
     *   offset : Int64
     *   key : ByteArray
     *   payload : ByteArray
     *   error : UInt32
     */

    lean_object *result = lean_alloc_ctor(0, 6, 0);

    /* Topic */
    const char *topic_name = rd_kafka_topic_name(msg->rkt);
    lean_ctor_set(result, 0, lean_mk_string(topic_name ? topic_name : ""));

    /* Partition */
    lean_ctor_set(result, 1, lean_box((uint32_t)msg->partition));

    /* Offset */
    lean_ctor_set(result, 2, lean_box_uint64((uint64_t)msg->offset));

    /* Key */
    if (msg->key && msg->key_len > 0) {
        lean_object *key_arr = lean_alloc_sarray(1, msg->key_len, msg->key_len);
        memcpy(lean_sarray_cptr(key_arr), msg->key, msg->key_len);
        lean_ctor_set(result, 3, key_arr);
    } else {
        lean_ctor_set(result, 3, lean_alloc_sarray(1, 0, 0));
    }

    /* Payload */
    if (msg->payload && msg->len > 0) {
        lean_object *payload_arr = lean_alloc_sarray(1, msg->len, msg->len);
        memcpy(lean_sarray_cptr(payload_arr), msg->payload, msg->len);
        lean_ctor_set(result, 4, payload_arr);
    } else {
        lean_ctor_set(result, 4, lean_alloc_sarray(1, 0, 0));
    }

    /* Error code */
    lean_ctor_set(result, 5, lean_box((uint32_t)msg->err));

    rd_kafka_message_destroy(msg);

    return lean_io_result_mk_ok(mk_option_some(result));
}

LEAN_EXPORT lean_obj_res lean_kafka_consumer_close(b_lean_obj_arg rk_obj, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_resp_err_t err = rd_kafka_consumer_close(rk);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* ============================================================================
 * Commit
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_commit(b_lean_obj_arg rk_obj,
                                            uint8_t async,
                                            lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_resp_err_t err = rd_kafka_commit(rk, NULL, async ? 1 : 0);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* ============================================================================
 * Metadata
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_memberid(b_lean_obj_arg rk_obj, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    char *memberid = rd_kafka_memberid(rk);
    if (memberid == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    lean_object *result = mk_option_some(lean_mk_string(memberid));
    rd_kafka_mem_free(rk, memberid);
    return lean_io_result_mk_ok(result);
}

LEAN_EXPORT lean_obj_res lean_kafka_name(b_lean_obj_arg rk_obj, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const char *name = rd_kafka_name(rk);
    return lean_io_result_mk_ok(lean_mk_string(name ? name : ""));
}

/* ============================================================================
 * Resource cleanup
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_destroy(lean_obj_arg rk_obj, lean_obj_arg world) {
    /* The external object finalizer will handle cleanup */
    /* Just return success */
    return lean_io_result_mk_ok(lean_box(0));
}

/* ============================================================================
 * Headers
 * ============================================================================ */

LEAN_EXPORT lean_obj_res lean_kafka_headers_new(lean_obj_arg world) {
    rd_kafka_headers_t *hdrs = rd_kafka_headers_new(8);
    if (hdrs == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    lean_object *obj = lean_alloc_external(get_kafka_headers_class(), (void *)hdrs);
    return lean_io_result_mk_ok(mk_option_some(obj));
}

LEAN_EXPORT lean_obj_res lean_kafka_header_add(b_lean_obj_arg hdrs_obj,
                                                b_lean_obj_arg name,
                                                b_lean_obj_arg value,
                                                lean_obj_arg world) {
    rd_kafka_headers_t *hdrs = (rd_kafka_headers_t *)lean_get_external_data(hdrs_obj);
    const char *name_str = lean_string_cstr(name);
    size_t value_len = lean_sarray_size(value);
    const uint8_t *value_data = lean_sarray_cptr(value);

    rd_kafka_resp_err_t err = rd_kafka_header_add(hdrs, name_str, -1, value_data, value_len);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

LEAN_EXPORT lean_obj_res lean_kafka_headers_count(b_lean_obj_arg hdrs_obj, lean_obj_arg world) {
    rd_kafka_headers_t *hdrs = (rd_kafka_headers_t *)lean_get_external_data(hdrs_obj);
    size_t count = rd_kafka_header_cnt(hdrs);
    return lean_io_result_mk_ok(lean_box_uint64((uint64_t)count));
}

/* Convert headers to Lean Array of (String, ByteArray) pairs */
LEAN_EXPORT lean_obj_res lean_kafka_headers_to_array(b_lean_obj_arg hdrs_obj, lean_obj_arg world) {
    rd_kafka_headers_t *hdrs = (rd_kafka_headers_t *)lean_get_external_data(hdrs_obj);
    size_t count = rd_kafka_header_cnt(hdrs);

    lean_object *arr = lean_alloc_array(count, count);

    for (size_t i = 0; i < count; i++) {
        const char *name;
        const void *value;
        size_t value_size;

        rd_kafka_resp_err_t err = rd_kafka_header_get_all(hdrs, i, &name, &value, &value_size);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            continue;
        }

        /* Create tuple (String, ByteArray) */
        lean_object *tuple = lean_alloc_ctor(0, 2, 0);
        lean_ctor_set(tuple, 0, lean_mk_string(name ? name : ""));

        if (value && value_size > 0) {
            lean_object *val_arr = lean_alloc_sarray(1, value_size, value_size);
            memcpy(lean_sarray_cptr(val_arr), value, value_size);
            lean_ctor_set(tuple, 1, val_arr);
        } else {
            lean_ctor_set(tuple, 1, lean_alloc_sarray(1, 0, 0));
        }

        lean_array_set_core(arr, i, tuple);
    }

    return lean_io_result_mk_ok(arr);
}

/* Produce with headers */
LEAN_EXPORT lean_obj_res lean_kafka_produce_with_headers(b_lean_obj_arg rk_obj,
                                                          b_lean_obj_arg topic,
                                                          int32_t partition,
                                                          b_lean_obj_arg payload,
                                                          b_lean_obj_arg key,
                                                          b_lean_obj_arg headers_arr,
                                                          lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const char *topic_str = lean_string_cstr(topic);

    /* Get payload data */
    size_t payload_len = lean_sarray_size(payload);
    const uint8_t *payload_data = lean_sarray_cptr(payload);

    /* Get key data (may be empty) */
    size_t key_len = lean_sarray_size(key);
    const uint8_t *key_data = key_len > 0 ? lean_sarray_cptr(key) : NULL;

    /* Build headers from Lean Array of (String, ByteArray) */
    size_t hdrs_count = lean_array_size(headers_arr);
    rd_kafka_headers_t *hdrs = NULL;

    if (hdrs_count > 0) {
        hdrs = rd_kafka_headers_new(hdrs_count);
        for (size_t i = 0; i < hdrs_count; i++) {
            lean_object *tuple = lean_array_get_core(headers_arr, i);
            lean_object *hdr_name = lean_ctor_get(tuple, 0);
            lean_object *hdr_value = lean_ctor_get(tuple, 1);

            const char *name_str = lean_string_cstr(hdr_name);
            size_t value_len = lean_sarray_size(hdr_value);
            const uint8_t *value_data = lean_sarray_cptr(hdr_value);

            rd_kafka_header_add(hdrs, name_str, -1, value_data, value_len);
        }
    }

    rd_kafka_resp_err_t err;
    if (hdrs) {
        err = rd_kafka_producev(
            rk,
            RD_KAFKA_V_TOPIC(topic_str),
            RD_KAFKA_V_PARTITION(partition),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE((void *)payload_data, payload_len),
            RD_KAFKA_V_KEY((void *)key_data, key_len),
            RD_KAFKA_V_HEADERS(hdrs),
            RD_KAFKA_V_END
        );
        /* Note: hdrs is freed by librdkafka on success, or we need to free on error */
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            rd_kafka_headers_destroy(hdrs);
        }
    } else {
        err = rd_kafka_producev(
            rk,
            RD_KAFKA_V_TOPIC(topic_str),
            RD_KAFKA_V_PARTITION(partition),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE((void *)payload_data, payload_len),
            RD_KAFKA_V_KEY((void *)key_data, key_len),
            RD_KAFKA_V_END
        );
    }

    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* ============================================================================
 * Metadata API
 * ============================================================================ */

/* Helper to create broker info structure */
static lean_object *mk_broker_info(int32_t id, const char *host, int port) {
    lean_object *result = lean_alloc_ctor(0, 3, 0);
    lean_ctor_set(result, 0, lean_box((uint32_t)id));
    lean_ctor_set(result, 1, lean_mk_string(host ? host : ""));
    lean_ctor_set(result, 2, lean_box((uint32_t)port));
    return result;
}

/* Helper to create partition info structure */
static lean_object *mk_partition_info(int32_t id, rd_kafka_resp_err_t err, int32_t leader,
                                       int32_t *replicas, int replica_cnt,
                                       int32_t *isrs, int isr_cnt) {
    lean_object *result = lean_alloc_ctor(0, 5, 0);
    lean_ctor_set(result, 0, lean_box((uint32_t)id));
    lean_ctor_set(result, 1, lean_box((uint32_t)err));
    lean_ctor_set(result, 2, lean_box((uint32_t)leader));

    /* Replicas array */
    lean_object *replicas_arr = lean_alloc_array(replica_cnt, replica_cnt);
    for (int i = 0; i < replica_cnt; i++) {
        lean_array_set_core(replicas_arr, i, lean_box((uint32_t)replicas[i]));
    }
    lean_ctor_set(result, 3, replicas_arr);

    /* ISRs array */
    lean_object *isrs_arr = lean_alloc_array(isr_cnt, isr_cnt);
    for (int i = 0; i < isr_cnt; i++) {
        lean_array_set_core(isrs_arr, i, lean_box((uint32_t)isrs[i]));
    }
    lean_ctor_set(result, 4, isrs_arr);

    return result;
}

/* Helper to create topic info structure */
static lean_object *mk_topic_info(const struct rd_kafka_metadata_topic *topic) {
    lean_object *result = lean_alloc_ctor(0, 3, 0);
    lean_ctor_set(result, 0, lean_mk_string(topic->topic ? topic->topic : ""));
    lean_ctor_set(result, 1, lean_box((uint32_t)topic->err));

    /* Partitions array */
    lean_object *partitions_arr = lean_alloc_array(topic->partition_cnt, topic->partition_cnt);
    for (int i = 0; i < topic->partition_cnt; i++) {
        const struct rd_kafka_metadata_partition *p = &topic->partitions[i];
        lean_object *pinfo = mk_partition_info(p->id, p->err, p->leader,
                                                p->replicas, p->replica_cnt,
                                                p->isrs, p->isr_cnt);
        lean_array_set_core(partitions_arr, i, pinfo);
    }
    lean_ctor_set(result, 2, partitions_arr);

    return result;
}

/* Get cluster metadata */
LEAN_EXPORT lean_obj_res lean_kafka_metadata(b_lean_obj_arg rk_obj,
                                              uint8_t all_topics,
                                              int32_t timeout_ms,
                                              lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const struct rd_kafka_metadata *metadata = NULL;

    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, all_topics ? 1 : 0, NULL, &metadata, timeout_ms);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR || metadata == NULL) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(rd_kafka_err2str(err))));
    }

    /* Build metadata structure:
     * Structure ClusterMetadata where
     *   brokers : Array BrokerInfo
     *   topics : Array TopicInfo
     *   origBrokerId : Int32
     *   origBrokerName : String
     */
    lean_object *result = lean_alloc_ctor(0, 4, 0);

    /* Brokers array */
    lean_object *brokers_arr = lean_alloc_array(metadata->broker_cnt, metadata->broker_cnt);
    for (int i = 0; i < metadata->broker_cnt; i++) {
        const struct rd_kafka_metadata_broker *b = &metadata->brokers[i];
        lean_array_set_core(brokers_arr, i, mk_broker_info(b->id, b->host, b->port));
    }
    lean_ctor_set(result, 0, brokers_arr);

    /* Topics array */
    lean_object *topics_arr = lean_alloc_array(metadata->topic_cnt, metadata->topic_cnt);
    for (int i = 0; i < metadata->topic_cnt; i++) {
        lean_array_set_core(topics_arr, i, mk_topic_info(&metadata->topics[i]));
    }
    lean_ctor_set(result, 1, topics_arr);

    /* Origin broker */
    lean_ctor_set(result, 2, lean_box((uint32_t)metadata->orig_broker_id));
    lean_ctor_set(result, 3, lean_mk_string(metadata->orig_broker_name ? metadata->orig_broker_name : ""));

    rd_kafka_metadata_destroy(metadata);

    return lean_io_result_mk_ok(mk_except_ok(result));
}

/* Get metadata for a specific topic */
LEAN_EXPORT lean_obj_res lean_kafka_metadata_for_topic(b_lean_obj_arg rk_obj,
                                                        b_lean_obj_arg topic_name,
                                                        int32_t timeout_ms,
                                                        lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const char *topic_str = lean_string_cstr(topic_name);

    /* Create a temporary topic handle for the query */
    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, topic_str, NULL);
    if (rkt == NULL) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string("Failed to create topic handle")));
    }

    const struct rd_kafka_metadata *metadata = NULL;
    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 0, rkt, &metadata, timeout_ms);

    rd_kafka_topic_destroy(rkt);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR || metadata == NULL) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(rd_kafka_err2str(err))));
    }

    /* Should have exactly one topic */
    if (metadata->topic_cnt != 1) {
        rd_kafka_metadata_destroy(metadata);
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string("Topic not found")));
    }

    lean_object *result = mk_topic_info(&metadata->topics[0]);
    rd_kafka_metadata_destroy(metadata);

    return lean_io_result_mk_ok(mk_except_ok(result));
}

/* Query watermark offsets (makes broker request) */
LEAN_EXPORT lean_obj_res lean_kafka_query_watermark_offsets(b_lean_obj_arg rk_obj,
                                                             b_lean_obj_arg topic,
                                                             int32_t partition,
                                                             int32_t timeout_ms,
                                                             lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const char *topic_str = lean_string_cstr(topic);
    int64_t lo, hi;

    rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(rk, topic_str, partition, &lo, &hi, timeout_ms);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(rd_kafka_err2str(err))));
    }

    /* Return tuple (Int64, Int64) */
    lean_object *result = lean_alloc_ctor(0, 2, 0);
    lean_ctor_set(result, 0, lean_box_uint64((uint64_t)lo));
    lean_ctor_set(result, 1, lean_box_uint64((uint64_t)hi));

    return lean_io_result_mk_ok(mk_except_ok(result));
}

/* Get cached watermark offsets (from local cache) */
LEAN_EXPORT lean_obj_res lean_kafka_get_watermark_offsets(b_lean_obj_arg rk_obj,
                                                           b_lean_obj_arg topic,
                                                           int32_t partition,
                                                           lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const char *topic_str = lean_string_cstr(topic);
    int64_t lo, hi;

    rd_kafka_resp_err_t err = rd_kafka_get_watermark_offsets(rk, topic_str, partition, &lo, &hi);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(rd_kafka_err2str(err))));
    }

    /* Return tuple (Int64, Int64) */
    lean_object *result = lean_alloc_ctor(0, 2, 0);
    lean_ctor_set(result, 0, lean_box_uint64((uint64_t)lo));
    lean_ctor_set(result, 1, lean_box_uint64((uint64_t)hi));

    return lean_io_result_mk_ok(mk_except_ok(result));
}

/* Offsets for times - lookup offset by timestamp */
LEAN_EXPORT lean_obj_res lean_kafka_offsets_for_times(b_lean_obj_arg rk_obj,
                                                       b_lean_obj_arg topic,
                                                       int32_t partition,
                                                       int64_t timestamp_ms,
                                                       int32_t timeout_ms,
                                                       lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const char *topic_str = lean_string_cstr(topic);

    /* Create a topic partition list with the timestamp */
    rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_t *tp = rd_kafka_topic_partition_list_add(offsets, topic_str, partition);
    tp->offset = timestamp_ms;

    rd_kafka_resp_err_t err = rd_kafka_offsets_for_times(rk, offsets, timeout_ms);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_topic_partition_list_destroy(offsets);
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(rd_kafka_err2str(err))));
    }

    /* Get the result offset */
    int64_t result_offset = offsets->elems[0].offset;
    rd_kafka_resp_err_t tp_err = offsets->elems[0].err;

    rd_kafka_topic_partition_list_destroy(offsets);

    if (tp_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(rd_kafka_err2str(tp_err))));
    }

    return lean_io_result_mk_ok(mk_except_ok(lean_box_uint64((uint64_t)result_offset)));
}

/* ============================================================================
 * Partition Assignment Control
 * ============================================================================ */

/* Create a new topic partition list */
LEAN_EXPORT lean_obj_res lean_kafka_topic_partition_list_new(uint32_t size, lean_obj_arg world) {
    rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new((int)size);
    if (tpl == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }
    lean_object *obj = lean_alloc_external(get_kafka_topic_partition_list_class(), (void *)tpl);
    return lean_io_result_mk_ok(mk_option_some(obj));
}

/* Add a topic partition to the list */
LEAN_EXPORT lean_obj_res lean_kafka_topic_partition_list_add(b_lean_obj_arg tpl_obj,
                                                              b_lean_obj_arg topic,
                                                              int32_t partition,
                                                              lean_obj_arg world) {
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    const char *topic_str = lean_string_cstr(topic);
    rd_kafka_topic_partition_list_add(tpl, topic_str, partition);
    return lean_io_result_mk_ok(lean_box(0));
}

/* Add a topic partition with offset */
LEAN_EXPORT lean_obj_res lean_kafka_topic_partition_list_add_range(b_lean_obj_arg tpl_obj,
                                                                    b_lean_obj_arg topic,
                                                                    int32_t start,
                                                                    int32_t stop,
                                                                    lean_obj_arg world) {
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    const char *topic_str = lean_string_cstr(topic);
    rd_kafka_topic_partition_list_add_range(tpl, topic_str, start, stop);
    return lean_io_result_mk_ok(lean_box(0));
}

/* Set offset for a topic partition */
LEAN_EXPORT lean_obj_res lean_kafka_topic_partition_list_set_offset(b_lean_obj_arg tpl_obj,
                                                                     b_lean_obj_arg topic,
                                                                     int32_t partition,
                                                                     int64_t offset,
                                                                     lean_obj_arg world) {
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    const char *topic_str = lean_string_cstr(topic);
    rd_kafka_resp_err_t err = rd_kafka_topic_partition_list_set_offset(tpl, topic_str, partition, offset);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* Convert topic partition list to Lean array */
LEAN_EXPORT lean_obj_res lean_kafka_topic_partition_list_to_array(b_lean_obj_arg tpl_obj, lean_obj_arg world) {
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);

    lean_object *arr = lean_alloc_array(tpl->cnt, tpl->cnt);
    for (int i = 0; i < tpl->cnt; i++) {
        rd_kafka_topic_partition_t *tp = &tpl->elems[i];
        /* Create tuple (String, Int32, Int64, UInt32) - topic, partition, offset, error */
        lean_object *tuple = lean_alloc_ctor(0, 4, 0);
        lean_ctor_set(tuple, 0, lean_mk_string(tp->topic ? tp->topic : ""));
        lean_ctor_set(tuple, 1, lean_box((uint32_t)tp->partition));
        lean_ctor_set(tuple, 2, lean_box_uint64((uint64_t)tp->offset));
        lean_ctor_set(tuple, 3, lean_box((uint32_t)tp->err));
        lean_array_set_core(arr, i, tuple);
    }
    return lean_io_result_mk_ok(arr);
}

/* Assign partitions to consumer */
LEAN_EXPORT lean_obj_res lean_kafka_assign(b_lean_obj_arg rk_obj,
                                            b_lean_obj_arg tpl_obj,
                                            lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    rd_kafka_resp_err_t err = rd_kafka_assign(rk, tpl);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* Unassign all partitions */
LEAN_EXPORT lean_obj_res lean_kafka_unassign(b_lean_obj_arg rk_obj, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_resp_err_t err = rd_kafka_assign(rk, NULL);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* Get current assignment */
LEAN_EXPORT lean_obj_res lean_kafka_assignment(b_lean_obj_arg rk_obj, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *tpl = NULL;

    rd_kafka_resp_err_t err = rd_kafka_assignment(rk, &tpl);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR || tpl == NULL) {
        return lean_io_result_mk_ok(mk_except_error(lean_mk_string(rd_kafka_err2str(err))));
    }

    lean_object *obj = lean_alloc_external(get_kafka_topic_partition_list_class(), (void *)tpl);
    return lean_io_result_mk_ok(mk_except_ok(obj));
}

/* Seek to offset */
LEAN_EXPORT lean_obj_res lean_kafka_seek(b_lean_obj_arg rk_obj,
                                          b_lean_obj_arg topic,
                                          int32_t partition,
                                          int64_t offset,
                                          int32_t timeout_ms,
                                          lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    const char *topic_str = lean_string_cstr(topic);

    /* Create a topic partition list for seek_partitions */
    rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_t *tp = rd_kafka_topic_partition_list_add(tpl, topic_str, partition);
    tp->offset = offset;

    rd_kafka_error_t *error = rd_kafka_seek_partitions(rk, tpl, timeout_ms);

    rd_kafka_topic_partition_list_destroy(tpl);

    if (error != NULL) {
        const char *err_str = rd_kafka_error_string(error);
        lean_object *result = mk_except_error(lean_mk_string(err_str));
        rd_kafka_error_destroy(error);
        return lean_io_result_mk_ok(result);
    }

    return lean_io_result_mk_ok(mk_except_ok(lean_box(0)));
}

/* Pause partitions */
LEAN_EXPORT lean_obj_res lean_kafka_pause_partitions(b_lean_obj_arg rk_obj,
                                                      b_lean_obj_arg tpl_obj,
                                                      lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    rd_kafka_resp_err_t err = rd_kafka_pause_partitions(rk, tpl);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* Resume partitions */
LEAN_EXPORT lean_obj_res lean_kafka_resume_partitions(b_lean_obj_arg rk_obj,
                                                       b_lean_obj_arg tpl_obj,
                                                       lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    rd_kafka_resp_err_t err = rd_kafka_resume_partitions(rk, tpl);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* Store offset */
LEAN_EXPORT lean_obj_res lean_kafka_offsets_store(b_lean_obj_arg rk_obj,
                                                   b_lean_obj_arg tpl_obj,
                                                   lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    rd_kafka_resp_err_t err = rd_kafka_offsets_store(rk, tpl);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* Commit specific offsets */
LEAN_EXPORT lean_obj_res lean_kafka_commit_offsets(b_lean_obj_arg rk_obj,
                                                    b_lean_obj_arg tpl_obj,
                                                    uint8_t async,
                                                    lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    rd_kafka_resp_err_t err = rd_kafka_commit(rk, tpl, async ? 1 : 0);
    return lean_io_result_mk_ok(lean_box((uint32_t)err));
}

/* Get committed offsets */
LEAN_EXPORT lean_obj_res lean_kafka_committed(b_lean_obj_arg rk_obj,
                                               b_lean_obj_arg tpl_obj,
                                               int32_t timeout_ms,
                                               lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);

    rd_kafka_resp_err_t err = rd_kafka_committed(rk, tpl, timeout_ms);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return lean_io_result_mk_ok(lean_box((uint32_t)err));
    }

    return lean_io_result_mk_ok(lean_box(0));
}

/* Get position (next offset to be read) */
LEAN_EXPORT lean_obj_res lean_kafka_position(b_lean_obj_arg rk_obj,
                                              b_lean_obj_arg tpl_obj,
                                              lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *tpl = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);

    rd_kafka_resp_err_t err = rd_kafka_position(rk, tpl);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return lean_io_result_mk_ok(lean_box((uint32_t)err));
    }

    return lean_io_result_mk_ok(lean_box(0));
}

/* ============================================================================
 * Transactions
 * ============================================================================ */

/* Helper to convert rd_kafka_error_t to Lean Except */
static lean_obj_res rd_kafka_error_to_except(rd_kafka_error_t *error) {
    if (error == NULL) {
        return mk_except_ok(lean_box(0));
    }

    const char *err_str = rd_kafka_error_string(error);
    int is_fatal = rd_kafka_error_is_fatal(error);
    int is_retriable = rd_kafka_error_is_retriable(error);
    int txn_requires_abort = rd_kafka_error_txn_requires_abort(error);
    rd_kafka_resp_err_t code = rd_kafka_error_code(error);

    /* Build error structure: (code, message, isFatal, isRetriable, txnRequiresAbort) */
    lean_object *err_obj = lean_alloc_ctor(0, 5, 0);
    lean_ctor_set(err_obj, 0, lean_box((uint32_t)code));
    lean_ctor_set(err_obj, 1, lean_mk_string(err_str ? err_str : "Unknown error"));
    lean_ctor_set(err_obj, 2, lean_box(is_fatal ? 1 : 0));
    lean_ctor_set(err_obj, 3, lean_box(is_retriable ? 1 : 0));
    lean_ctor_set(err_obj, 4, lean_box(txn_requires_abort ? 1 : 0));

    rd_kafka_error_destroy(error);

    return mk_except_error(err_obj);
}

/* Initialize transactions (must be called once before any transactional operation) */
LEAN_EXPORT lean_obj_res lean_kafka_init_transactions(b_lean_obj_arg rk_obj,
                                                       int32_t timeout_ms,
                                                       lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_error_t *error = rd_kafka_init_transactions(rk, timeout_ms);
    return lean_io_result_mk_ok(rd_kafka_error_to_except(error));
}

/* Begin a new transaction */
LEAN_EXPORT lean_obj_res lean_kafka_begin_transaction(b_lean_obj_arg rk_obj,
                                                       lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_error_t *error = rd_kafka_begin_transaction(rk);
    return lean_io_result_mk_ok(rd_kafka_error_to_except(error));
}

/* Commit current transaction */
LEAN_EXPORT lean_obj_res lean_kafka_commit_transaction(b_lean_obj_arg rk_obj,
                                                        int32_t timeout_ms,
                                                        lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_error_t *error = rd_kafka_commit_transaction(rk, timeout_ms);
    return lean_io_result_mk_ok(rd_kafka_error_to_except(error));
}

/* Abort current transaction */
LEAN_EXPORT lean_obj_res lean_kafka_abort_transaction(b_lean_obj_arg rk_obj,
                                                       int32_t timeout_ms,
                                                       lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_error_t *error = rd_kafka_abort_transaction(rk, timeout_ms);
    return lean_io_result_mk_ok(rd_kafka_error_to_except(error));
}

/* Consumer group metadata - needed for send_offsets_to_transaction */
static void kafka_consumer_group_metadata_finalizer(void *ptr) {
    if (ptr) {
        rd_kafka_consumer_group_metadata_destroy((rd_kafka_consumer_group_metadata_t *)ptr);
    }
}

static lean_external_class *g_kafka_consumer_group_metadata_class = NULL;

static lean_external_class *get_kafka_consumer_group_metadata_class(void) {
    if (g_kafka_consumer_group_metadata_class == NULL) {
        g_kafka_consumer_group_metadata_class = lean_register_external_class(kafka_consumer_group_metadata_finalizer, NULL);
    }
    return g_kafka_consumer_group_metadata_class;
}

/* Get consumer group metadata from consumer */
LEAN_EXPORT lean_obj_res lean_kafka_consumer_group_metadata(b_lean_obj_arg rk_obj, lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_consumer_group_metadata_t *cgmd = rd_kafka_consumer_group_metadata(rk);

    if (cgmd == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }

    lean_object *obj = lean_alloc_external(get_kafka_consumer_group_metadata_class(), (void *)cgmd);
    return lean_io_result_mk_ok(mk_option_some(obj));
}

/* Create consumer group metadata from group ID string */
LEAN_EXPORT lean_obj_res lean_kafka_consumer_group_metadata_new(b_lean_obj_arg group_id, lean_obj_arg world) {
    const char *group_id_str = lean_string_cstr(group_id);
    rd_kafka_consumer_group_metadata_t *cgmd = rd_kafka_consumer_group_metadata_new(group_id_str);

    if (cgmd == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }

    lean_object *obj = lean_alloc_external(get_kafka_consumer_group_metadata_class(), (void *)cgmd);
    return lean_io_result_mk_ok(mk_option_some(obj));
}

/* Send offsets to transaction (for exactly-once consume-transform-produce) */
LEAN_EXPORT lean_obj_res lean_kafka_send_offsets_to_transaction(b_lean_obj_arg rk_obj,
                                                                 b_lean_obj_arg tpl_obj,
                                                                 b_lean_obj_arg cgmd_obj,
                                                                 int32_t timeout_ms,
                                                                 lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_topic_partition_list_t *offsets = (rd_kafka_topic_partition_list_t *)lean_get_external_data(tpl_obj);
    rd_kafka_consumer_group_metadata_t *cgmd = (rd_kafka_consumer_group_metadata_t *)lean_get_external_data(cgmd_obj);

    rd_kafka_error_t *error = rd_kafka_send_offsets_to_transaction(rk, offsets, cgmd, timeout_ms);
    return lean_io_result_mk_ok(rd_kafka_error_to_except(error));
}

/* Consumer poll with headers - returns message data including headers */
LEAN_EXPORT lean_obj_res lean_kafka_consumer_poll_with_headers(b_lean_obj_arg rk_obj,
                                                                int32_t timeout_ms,
                                                                lean_obj_arg world) {
    rd_kafka_t *rk = (rd_kafka_t *)lean_get_external_data(rk_obj);
    rd_kafka_message_t *msg = rd_kafka_consumer_poll(rk, timeout_ms);

    if (msg == NULL) {
        return lean_io_result_mk_ok(mk_option_none());
    }

    /* Build a Lean structure with message data including headers:
     * Structure RawMessageWithHeaders where
     *   topic : String
     *   partition : UInt32
     *   offset : UInt64
     *   key : ByteArray
     *   payload : ByteArray
     *   error : UInt32
     *   headers : Array (String × ByteArray)
     */

    lean_object *result = lean_alloc_ctor(0, 7, 0);

    /* Topic */
    const char *topic_name = rd_kafka_topic_name(msg->rkt);
    lean_ctor_set(result, 0, lean_mk_string(topic_name ? topic_name : ""));

    /* Partition */
    lean_ctor_set(result, 1, lean_box((uint32_t)msg->partition));

    /* Offset */
    lean_ctor_set(result, 2, lean_box_uint64((uint64_t)msg->offset));

    /* Key */
    if (msg->key && msg->key_len > 0) {
        lean_object *key_arr = lean_alloc_sarray(1, msg->key_len, msg->key_len);
        memcpy(lean_sarray_cptr(key_arr), msg->key, msg->key_len);
        lean_ctor_set(result, 3, key_arr);
    } else {
        lean_ctor_set(result, 3, lean_alloc_sarray(1, 0, 0));
    }

    /* Payload */
    if (msg->payload && msg->len > 0) {
        lean_object *payload_arr = lean_alloc_sarray(1, msg->len, msg->len);
        memcpy(lean_sarray_cptr(payload_arr), msg->payload, msg->len);
        lean_ctor_set(result, 4, payload_arr);
    } else {
        lean_ctor_set(result, 4, lean_alloc_sarray(1, 0, 0));
    }

    /* Error code */
    lean_ctor_set(result, 5, lean_box((uint32_t)msg->err));

    /* Headers */
    rd_kafka_headers_t *hdrs;
    rd_kafka_resp_err_t hdrs_err = rd_kafka_message_headers(msg, &hdrs);

    if (hdrs_err == RD_KAFKA_RESP_ERR_NO_ERROR && hdrs != NULL) {
        size_t hdrs_count = rd_kafka_header_cnt(hdrs);
        lean_object *hdrs_arr = lean_alloc_array(hdrs_count, hdrs_count);

        for (size_t i = 0; i < hdrs_count; i++) {
            const char *name;
            const void *value;
            size_t value_size;

            rd_kafka_resp_err_t get_err = rd_kafka_header_get_all(hdrs, i, &name, &value, &value_size);
            if (get_err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                lean_object *tuple = lean_alloc_ctor(0, 2, 0);
                lean_ctor_set(tuple, 0, lean_mk_string(name ? name : ""));

                if (value && value_size > 0) {
                    lean_object *val_arr = lean_alloc_sarray(1, value_size, value_size);
                    memcpy(lean_sarray_cptr(val_arr), value, value_size);
                    lean_ctor_set(tuple, 1, val_arr);
                } else {
                    lean_ctor_set(tuple, 1, lean_alloc_sarray(1, 0, 0));
                }
                lean_array_set_core(hdrs_arr, i, tuple);
            } else {
                /* Create empty tuple on error */
                lean_object *tuple = lean_alloc_ctor(0, 2, 0);
                lean_ctor_set(tuple, 0, lean_mk_string(""));
                lean_ctor_set(tuple, 1, lean_alloc_sarray(1, 0, 0));
                lean_array_set_core(hdrs_arr, i, tuple);
            }
        }
        lean_ctor_set(result, 6, hdrs_arr);
    } else {
        /* No headers - empty array */
        lean_ctor_set(result, 6, lean_alloc_array(0, 0));
    }

    rd_kafka_message_destroy(msg);

    return lean_io_result_mk_ok(mk_option_some(result));
}
