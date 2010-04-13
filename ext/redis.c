#include <ruby.h>
#include <string.h>
#include <stdarg.h>
#include "include/redis.h"

typedef struct {
    Module * module;
    Connection * connection;
    VALUE connection_string;
} Redis;

#define SET_CMD "SET %s %ld\r\n%s\r\n"
#define GET_CMD "GET %s\r\n"

#define OK "OK\r\n"

VALUE cRedis, cRedisError;

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

#define GET_REDIS() \
    Redis * redis; \
    Data_Get_Struct(self, Redis, redis)

#define CRLF "\r\n"

#define INCR "INCR "

typedef struct {
    char * data;
    ReplyType reply_type;
    size_t length;
    Executor * executor;
} Reply;

static Reply * Reply_new() {
    return (Reply *) malloc(sizeof(Reply));
}

static void Reply_free(Reply * reply) {
    Executor_free(reply->executor);
    free(reply);
}

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

#define REPLY_TO_BOOLEAN(reply, reply_ret) \
    VALUE reply_ret; \
    do { \
        if(strcmp((reply)->data, OK)) { \
            reply_ret = Qfalse; \
        } else { \
            reply_ret = Qtrue; \
        } \
    } while(0)

#define REPLY_TO_INT(reply, reply_ret) \
    VALUE reply_ret; \
    do { \
        if((reply)->reply_type == RT_INTEGER) { \
            reply_ret = INT2FIX(atoi(reply->data)); \
        } else { \
            rb_raise(cRedisError, "Expected Integer return type.  Got something else."); \
        } \
    } while(0)
    


#define EXECUTE(args, ...) \
    GET_REDIS(); \
    Batch * batch = create_batch(args, ##__VA_ARGS__); \
    Reply * reply = execute_batch(redis, batch)

#define CLEANUP() \
    Reply_free(reply); \
    Batch_free(batch)
    

static VALUE Redis_incr(VALUE self, VALUE key) {
    EXECUTE(2, INCR, (int) (sizeof(INCR) - 1), RSTRING_PTR(key), (int) RSTRING_LEN(key));
    REPLY_TO_INT(reply, ret);
    CLEANUP();
    return ret;
}


static VALUE Redis_set(VALUE self, VALUE key, VALUE value) {
    int cmd_len = RSTRING_LEN(key) + RSTRING_LEN(value) + sizeof(SET_CMD) + 16;
    char cmd[cmd_len];
    sprintf(cmd, SET_CMD, RSTRING_PTR(key), RSTRING_LEN(value), RSTRING_PTR(value));

    GET_REDIS();

    Batch * batch = Batch_new();
    Batch_write(batch, cmd, strlen(cmd), 1);
    
    Executor * executor = Executor_new();
    Executor_add(executor, redis->connection, batch);
    
    if(Executor_execute(executor, 500) <= 0) {
        char * error = Module_last_error(redis->module);
        rb_raise(cRedisError, error);
    }
    
    ReplyType reply_type;
    char * reply_data;
    size_t reply_len;

    Batch_next_reply(batch, &reply_type, &reply_data, &reply_len);

    VALUE ret;

    if(strcmp(reply_data, OK)) {
        ret = Qfalse;
    } else {
        ret = Qtrue;
    }

    Executor_free(executor);
    Batch_free(batch);

    return ret;
}

static VALUE Redis_get(VALUE self, VALUE key) {
    int cmd_len = RSTRING_LEN(key) + sizeof(GET_CMD) + 16;
    char cmd[cmd_len];
    sprintf(cmd, GET_CMD, RSTRING_PTR(key));

    Redis * redis;
    Data_Get_Struct(self, Redis, redis);

    Batch * batch = Batch_new();
    Batch_write(batch, cmd, strlen(cmd), 1);
    
    Executor * executor = Executor_new();
    Executor_add(executor, redis->connection, batch);
    
    if(Executor_execute(executor, 500) <= 0) {
        char * error = Module_last_error(redis->module);
        rb_raise(cRedisError, error);
    }
    
    ReplyType reply_type;
    char * reply_data;
    size_t reply_len;

    Batch_next_reply(batch, &reply_type, &reply_data, &reply_len);

    VALUE ret = rb_str_new(reply_data, reply_len);

    Executor_free(executor);
    Batch_free(batch);

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

    cRedisError = rb_define_class("RedisError", rb_eStandardError);
}
