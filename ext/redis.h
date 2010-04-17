#include "include/redis.h"

#define SET "SET "
#define GET "GET "
#define INCR "INCR "
#define DECR "DECR "
#define KEYS "KEYS "

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

