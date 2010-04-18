#include "include/redis.h"

#define CRLF "\r\n"

typedef struct {
    Module * module;
    Connection * connection;
    VALUE connection_string;
} Redis;

typedef struct {
    char * data;
    ReplyType reply_type;
    size_t length;
    Executor * executor;
} Reply;

#define FUNCTION_LINE_NOARGS(method)            \
    static VALUE Redis_##method(VALUE self)

#define FUNCTION_LINE_1ARG(method, arg_name)                \
    static VALUE Redis_##method(VALUE self, VALUE arg_name)

#define FUNCTION_LINE_2ARGS(method, arg1_name, arg2_name)               \
    static VALUE Redis_##method(VALUE self, VALUE arg1_name, VALUE arg2_name)

#define FUNCTION_LINE_3ARGS(method, arg1_name, arg2_name, arg3_name)    \
    static VALUE Redis_##method(VALUE self, VALUE arg1_name, VALUE arg2_name, VALUE arg3_name)


#define SETUP(command)                                                  \
    Redis * redis;                                                      \
    Data_Get_Struct(self, Redis, redis);                                \
    Batch * batch = Batch_new();                                        \
    Batch_write(batch, #command " ", (int) (sizeof(#command " ") - 1), 0)


#define WRITE_CRLF()                                \
    Batch_write(batch, CRLF, sizeof(CRLF) - 1, 0)

#define WRITE_SPACE()                           \
    Batch_write(batch, " ", 1, 0)

#define WRITE_INT(a)                        \
    Batch_write_decimal(batch, FIX2LONG(a))

#define WRITE_STRING(a)                                 \
    Batch_write(batch, RSTRING_PTR(a), RSTRING_LEN(a), 0)

#define WRITE_BLOB(a)                                       \
    Batch_write_decimal(batch, RSTRING_LEN(a));             \
    WRITE_CRLF();                                           \
    Batch_write(batch, RSTRING_PTR(a), RSTRING_LEN(a), 0)

#define FINISH_BATCH() \
    Batch_write(batch, CRLF, (sizeof(CRLF) - 1), 1)


#define RUN_EXECUTE()                                                   \
    Executor * executor = Executor_new();                               \
    Executor_add(executor, redis->connection, batch);                   \
    if(Executor_execute(executor, 500) <= 0) {                          \
        char * error = Module_last_error(redis->module);                \
        rb_raise(cRedisError, error);                                   \
    }

#define GET_REPLY()                                                     \
    Reply * reply = Reply_new();                                        \
    reply->executor = executor;                                         \
    Batch_next_reply(batch, &(reply->reply_type), &(reply->data), &(reply->length))

#define CLEANUP() \
    Reply_free(reply); \
    Batch_free(batch)


#define ANY                                     \
    VALUE ret = return_value(reply)

#define BOOLEAN                                                    \
    VALUE ret;                                                      \
    switch(reply->reply_type) {                                     \
    case RT_INTEGER:                                                \
        ret = *(reply->data) == '0' ? Qfalse : Qtrue;               \
        break;                                                      \
    case RT_ERROR:                                                  \
        rb_raise(cRedisError, reply->data);                         \
        break;                                                      \
    default:                                                        \
        rb_raise(cRedisError, "Unexpected return type from Redis"); \
    }

#define STATUS                                                      \
    VALUE ret;                                                      \
    switch(reply->reply_type) {                                     \
    case RT_OK:                                                     \
        ret = Qtrue;                                                \
        break;                                                      \
    case RT_INTEGER:                                                \
        /* This is retarded */                                      \
        ret = *(reply->data) == '0' ? Qfalse : Qtrue;               \
        break;                                                      \
    case RT_ERROR:                                                  \
        rb_raise(cRedisError, reply->data);                         \
        break;                                                      \
    default:                                                        \
        rb_raise(cRedisError, "Unexpected return type from Redis"); \
    }

#define EXECUTE(return_body)                    \
    FINISH_BATCH();                             \
    RUN_EXECUTE();                              \
    GET_REPLY();                                \
    return_body;                                \
    CLEANUP()

/* Function prototypes */

#define REDIS_CMD_INT(command, method, return_body)     \
    FUNCTION_LINE_1ARG(method, a) {         \
        SETUP(command);                     \
        WRITE_INT(a);                       \
        EXECUTE(return_body);                          \
        return ret;                         \
    }

#define REDIS_CMD_STR(command, method, return_body)        \
    FUNCTION_LINE_1ARG(method, a) {            \
        SETUP(command);                        \
        WRITE_STRING(a);                       \
        EXECUTE(return_body);                             \
        return ret;                            \
    }

#define REDIS_CMD_STR_INT(command, method, return_body)        \
    FUNCTION_LINE_2ARGS(method, a, b) {             \
        SETUP(command);                             \
        WRITE_STRING(a);                            \
        WRITE_SPACE();                              \
        WRITE_INT(b);                               \
        EXECUTE(return_body);                                  \
        return ret;                                 \
    }

#define REDIS_CMD_STR_INT_INT(command, method, return_body)        \
    FUNCTION_LINE_3ARGS(method, a, b, c) {                         \
        SETUP(command);                             \
        WRITE_STRING(a);                            \
        WRITE_SPACE();                              \
        WRITE_INT(b);                               \
        WRITE_SPACE();                              \
        WRITE_INT(c);                               \
        EXECUTE(return_body);                                  \
        return ret;                                 \
    }

#define REDIS_CMD_STR_INT_BLOB(command, method, return_body)        \
    FUNCTION_LINE_3ARGS(method, a, b, c) {                         \
        SETUP(command);                             \
        WRITE_STRING(a);                            \
        WRITE_SPACE();                              \
        WRITE_INT(b);                               \
        WRITE_SPACE();                              \
        WRITE_BLOB(c);                               \
        EXECUTE(return_body);                                  \
        return ret;                                 \
    }

#define REDIS_CMD_STR_STR(command, method, return_body)        \
    FUNCTION_LINE_2ARGS(method, a, b) {             \
        SETUP(command);                             \
        WRITE_STRING(a);                            \
        WRITE_SPACE();                              \
        WRITE_STRING(b);                            \
        EXECUTE(return_body);                                  \
        return ret;                                 \
    }

#define REDIS_CMD_STR_BLOB(command, method, return_body)       \
    FUNCTION_LINE_2ARGS(method, a, b) {             \
        SETUP(command);                             \
        WRITE_STRING(a);                            \
        WRITE_SPACE();                              \
        WRITE_BLOB(b);                              \
        EXECUTE(return_body);                                  \
        return ret;                                 \
    }

#define REDIS_CMD_NOARGS(command, method, return_body)        \
    FUNCTION_LINE_NOARGS(method) {            \
        SETUP(command);                          \
        EXECUTE(return_body);                               \
        return ret;                              \
    }
