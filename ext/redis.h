#include "include/redis.h"

#define SET "SET "
#define GET "GET "
#define INCR "INCR "
#define DECR "DECR "
#define KEYS "KEYS "
#define QUIT "QUIT"
#define AUTH "AUTH "
#define EXISTS "EXISTS "
#define DEL "DEL "
#define CMD_TYPE "TYPE "
#define RANDOMKEY "RANDOMKEY "
#define RENAME "RENAME "
#define RENAMENX "RENAMENX "
#define DBSIZE "DBSIZE "
#define FLUSHALL "FLUSHALL "

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

#define INT2BOOL(x) (FIX2LONG(x) == 0 ? Qfalse : Qtrue)

#define GET_REDIS(__redis) \
    Redis * __redis; \
    Data_Get_Struct(self, Redis, __redis)

#define CLEANUP() \
    Reply_free(reply); \
    Batch_free(batch)

#define EXECUTE(__redis, __arg_count, ...)       \
    GET_REDIS(__redis); \
    Batch * batch = finish_batch(create_batch(__arg_count, ##__VA_ARGS__));    \
    Reply * reply = execute_batch(__redis, batch)

#define REDIS_COMMAND_NOARGS(command, method)                       \
    static VALUE Redis_##method(VALUE self) {                       \
        EXECUTE(redis, 1, command, (int) (sizeof(command) - 1));    \
        VALUE ret = return_value(reply);                            \
        CLEANUP();                                                  \
        return ret;                                                 \
    }

#define REDIS_COMMAND_1ARG_BODY(command, method, ret_body)      \
    static VALUE Redis_##method(VALUE self, VALUE arg) {        \
        EXECUTE(redis, 2, command, (int) (sizeof(command) - 1), \
                RSTRING_PTR(arg), (int) RSTRING_LEN(arg));      \
        VALUE ret = return_value(reply);                        \
        CLEANUP();                                              \
        return ret_body;                                        \
    }

#define REDIS_COMMAND_1ARG(command, method) \
    REDIS_COMMAND_1ARG_BODY(command, method, ret);

#define REDIS_COMMAND_2ARGS(command, method)             \
    REDIS_COMMAND_2ARGS_BODY(command, method, ret)

#define REDIS_COMMAND_2ARGS_BODY(command, method, ret_body)             \
    static VALUE Redis_##method(VALUE self, VALUE arg1, VALUE arg2) {   \
        EXECUTE(redis, 4, command, (int) (sizeof(command) - 1),         \
                RSTRING_PTR(arg1), (int) RSTRING_LEN(arg1),             \
                " ", 1,                                                 \
                RSTRING_PTR(arg2), (int) RSTRING_LEN(arg2));            \
        VALUE ret = return_value(reply);                                \
        CLEANUP();                                                      \
        return ret_body;                                                \
    }
