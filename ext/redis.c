#include <ruby.h>
#include <string.h>
#include <stdarg.h>
#include "redis.h"

VALUE cRedis, cRedisError;


/* Redis struct functions */

void Redis_free(Redis * redis) {
    Connection_free(redis->connection);
    Module_free(redis->module);
}

static VALUE Redis_alloc(VALUE klass) {
    VALUE obj;

    Redis * redis = (Redis *) malloc(sizeof(Redis));
    redis->module = Module_new();
    Module_init(redis->module);

    obj = Data_Wrap_Struct(klass, 0, Redis_free, redis);
    return obj;
}


/* Reply struct functions */

static Reply * Reply_new() {
    return (Reply *) malloc(sizeof(Reply));
}

static void Reply_free(Reply * reply) {
    Executor_free(reply->executor);
    free(reply);
}


/* Utility functions */

static Batch * create_batch(int n, ...) {
    va_list ap;
    va_start(ap, n);

    Batch * batch = Batch_new();

    int i;
    for(i = 0; i < n; i++) {
        char * partial = va_arg(ap, char *);
        int size = va_arg(ap, int);
        Batch_write(batch, partial, size, 0);
    }
    va_end(ap);

    return batch;
}

static Batch * finish_batch(Batch * batch) {
    Batch_write(batch, CRLF, (sizeof(CRLF) - 1), 1);
    return batch;
}

static Reply * execute_batch(Redis * redis, Batch * batch) {
    Executor * executor = Executor_new();
    Executor_add(executor, redis->connection, batch);
    
    if(Executor_execute(executor, 500) <= 0) {
        char * error = Module_last_error(redis->module);
        rb_raise(cRedisError, error);
    }
    
    Reply * reply = Reply_new();

    reply->executor = executor;

    Batch_next_reply(batch, &(reply->reply_type), &(reply->data), &(reply->length));

    return reply;
}

static VALUE return_value(Reply * reply) {
    switch(reply->reply_type) {
    case RT_INTEGER:
        return INT2FIX(atoi(reply->data));
    case RT_OK:
        return Qtrue;
    case RT_NONE:
        return Qnil;
    case RT_BULK_NIL:
        return Qnil;
    case RT_BULK:
        return rb_str_new(reply->data, reply->length);
    }
}

static void add_blob(Batch * batch, VALUE value) {
    Batch_write(batch, " ", sizeof(" ") - 1, 0);
    Batch_write_decimal(batch, RSTRING_LEN(value));
    Batch_write(batch, CRLF, sizeof(CRLF) - 1, 0);
    Batch_write(batch, RSTRING_PTR(value), RSTRING_LEN(value), 0);
}

/* API functions */

static VALUE Redis_initialize(VALUE self, VALUE conn_string) {
    Redis * redis;
    Data_Get_Struct(self, Redis, redis);
    redis->connection_string = conn_string;
    redis->connection = Connection_new(RSTRING_PTR(conn_string));
    return self;
}

static VALUE Redis_connections(VALUE self) {
    Redis * redis;
    Data_Get_Struct(self, Redis, redis);
    VALUE connections = rb_ary_new();
    rb_ary_push(connections, redis->connection_string);
    return connections;
}

static VALUE Redis_incr(VALUE self, VALUE key) {
    EXECUTE(redis, 2, INCR, (int) (sizeof(INCR) - 1), RSTRING_PTR(key), (int) RSTRING_LEN(key));
    VALUE ret = return_value(reply);
    CLEANUP();
    return ret;
}

static VALUE Redis_decr(VALUE self, VALUE key) {
    EXECUTE(redis, 2, DECR, (int) (sizeof(DECR) - 1), RSTRING_PTR(key), (int) RSTRING_LEN(key));
    VALUE ret = return_value(reply);
    CLEANUP();
    return ret;
}

static VALUE Redis_keys(VALUE self, VALUE pattern) {
    EXECUTE(redis, 2, KEYS, (int) (sizeof(KEYS) - 1), RSTRING_PTR(pattern), (int) RSTRING_LEN(pattern));
    VALUE ret = return_value(reply);
    CLEANUP();
    return rb_str_split(ret, " ");
}

static VALUE Redis_set(VALUE self, VALUE key, VALUE value) {
    GET_REDIS(redis);
    Batch * batch = create_batch(2, SET, (int) (sizeof(SET) - 1), RSTRING_PTR(key), (int) RSTRING_LEN(key));
    add_blob(batch, value);
    finish_batch(batch);

    Reply * reply = execute_batch(redis, batch);
    
    VALUE ret = return_value(reply);
    CLEANUP();
    return ret;
}

static VALUE Redis_get(VALUE self, VALUE key) {
    EXECUTE(redis, 2, GET, (int) (sizeof(GET) - 1), RSTRING_PTR(key), (int) RSTRING_LEN(key));
    VALUE ret = return_value(reply);
    CLEANUP();
    return ret;
}


void Init_redis() {
    cRedis = rb_define_class("Redis", rb_cObject);
    rb_define_alloc_func(cRedis, Redis_alloc);
    rb_define_method(cRedis, "initialize", Redis_initialize, 1);
    rb_define_method(cRedis, "connections", Redis_connections, 0);
    rb_define_method(cRedis, "set", Redis_set, 2);
    rb_define_method(cRedis, "get", Redis_get, 1);
    rb_define_method(cRedis, "incr", Redis_incr, 1);
    rb_define_method(cRedis, "decr", Redis_decr, 1);
    rb_define_method(cRedis, "keys", Redis_keys, 1);

    cRedisError = rb_define_class("RedisError", rb_eStandardError);
}
