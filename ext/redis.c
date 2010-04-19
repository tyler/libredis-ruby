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

static VALUE return_value(Reply * reply) {
    switch(reply->reply_type) {
    case RT_INTEGER:
        return INT2FIX(atoi(reply->data));
    case RT_OK:
        return rb_str_new(reply->data, reply->length);
    case RT_NONE:
        return Qnil;
    case RT_BULK_NIL:
        return Qnil;
    case RT_BULK:
        return rb_str_new(reply->data, reply->length);
    case RT_ERROR:
        rb_raise(cRedisError, reply->data);
    }
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


REDIS_CMD_0(QUIT, quit, ANY)
REDIS_CMD_1(AUTH, auth, STR, ANY)

REDIS_CMD_1(EXISTS, exists, STR, BOOLEAN)
REDIS_CMD_1(DEL, del, STR, ANY)
REDIS_CMD_1(TYPE, type, STR, ANY)
REDIS_CMD_0(RANDOMKEY, random_key, ANY)
REDIS_CMD_2(RENAME, rename, STR, STR, STATUS)
REDIS_CMD_2(RENAMENX, renamenx, STR, STR, STATUS)
REDIS_CMD_0(DBSIZE, dbsize, ANY)
REDIS_CMD_2(EXPIRE, expire, STR, INT, ANY)
REDIS_CMD_2(EXPIREAT, expire_at, STR, INT, ANY)
REDIS_CMD_1(TTL, ttl, STR, ANY)
REDIS_CMD_1(SELECT, select, INT, ANY)
REDIS_CMD_2(MOVE, move, STR, INT, STATUS)
REDIS_CMD_0(FLUSHDB, flush_db, STATUS)
REDIS_CMD_0(FLUSHALL, flush_all, STATUS)

#define KEYS_RETURN \
    VALUE ret = rb_str_split(rb_str_new(reply->data, reply->length), " ")

REDIS_CMD_1(KEYS, keys, STR, KEYS_RETURN)

REDIS_CMD_2(SET, set, STR, BLOB, ANY)
REDIS_CMD_1(GET, get, STR, ANY)
REDIS_CMD_2(GETSET, get_set, STR, BLOB, ANY)
REDIS_CMD_2(SETNX, setnx, STR, BLOB, ANY)
REDIS_CMD_1(INCR, incr, STR, ANY)
REDIS_CMD_2(INCRBY, incrby, STR, INT, ANY)
REDIS_CMD_1(DECR, decr, STR, ANY)
REDIS_CMD_2(DECRBY, decrby, STR, INT, ANY)

REDIS_CMD_2(RPUSH, rpush, STR, BLOB, ANY)
REDIS_CMD_2(LPUSH, lpush, STR, BLOB, ANY)
REDIS_CMD_1(LLEN, llen, STR, ANY)
REDIS_CMD_3(LRANGE, lrange, STR, INT, INT, ANY)
REDIS_CMD_3(LTRIM, ltrim, STR, INT, INT, ANY)
REDIS_CMD_2(LINDEX, lindex, STR, INT, ANY)
REDIS_CMD_3(LSET, lset, STR, INT, BLOB, ANY)
REDIS_CMD_3(LREM, lrem, STR, INT, BLOB, ANY)
REDIS_CMD_1(LPOP, lpop, STR, ANY)
REDIS_CMD_1(RPOP, rpop, STR, ANY)
REDIS_CMD_2(RPOPLPUSH, rpoplpush, STR, STR, ANY)

REDIS_CMD_2(SADD, sadd, STR, BLOB, ANY)
REDIS_CMD_2(SREM, srem, STR, BLOB, ANY)
REDIS_CMD_1(SPOP, spop, STR, ANY)
REDIS_CMD_3(SMOVE, smove, STR, STR, BLOB, ANY)
REDIS_CMD_1(SCARD, scard, STR, ANY)
REDIS_CMD_2(SISMEMBER, sismember, STR, BLOB, BOOLEAN)
REDIS_CMD_1(SRANDMEMBER, srandmember, STR, ANY)

REDIS_CMD_3(ZADD, zadd, STR, INT, BLOB, ANY)
REDIS_CMD_2(ZREM, zrem, STR, BLOB, ANY)
REDIS_CMD_3(ZINCRBY, zincrby, STR, INT, BLOB, ANY)
REDIS_CMD_2(ZRANK, zrank, STR, BLOB, ANY)
REDIS_CMD_2(ZREVRANK, zrevrank, STR, BLOB, ANY)
REDIS_CMD_1(ZCARD, zcard, STR, ANY)
REDIS_CMD_2(ZSCORE, zscore, STR, BLOB, INTEGER)
REDIS_CMD_3(ZREMRANGEBYRANK, zremrangebyrank, STR, INT, INT, ANY)
REDIS_CMD_3(ZREMRANGEBYSCORE, zremrangebyscore, STR, INT, INT, ANY)


void Init_redis() {
    cRedis = rb_define_class("Redis", rb_cObject);
    rb_define_alloc_func(cRedis, Redis_alloc);
    rb_define_method(cRedis, "initialize", Redis_initialize, 1);
    rb_define_method(cRedis, "connections", Redis_connections, 0);

    rb_define_method(cRedis, "quit", Redis_quit, 0);
    rb_define_method(cRedis, "auth", Redis_auth, 1);

    rb_define_method(cRedis, "exists?", Redis_exists, 1);
    rb_define_method(cRedis, "del", Redis_del, 1);
    rb_define_method(cRedis, "type", Redis_type, 1);
    rb_define_method(cRedis, "keys", Redis_keys, 1);
    rb_define_method(cRedis, "random_key", Redis_random_key, 0);
    rb_define_method(cRedis, "rename", Redis_rename, 2);
    rb_define_method(cRedis, "renamenx", Redis_renamenx, 2);
    rb_define_method(cRedis, "dbsize", Redis_dbsize, 0);
    rb_define_method(cRedis, "expire", Redis_expire, 2);
    rb_define_method(cRedis, "expire_at", Redis_expire_at, 2);
    rb_define_method(cRedis, "ttl", Redis_ttl, 1);
    rb_define_method(cRedis, "select", Redis_select, 1);
    rb_define_method(cRedis, "move", Redis_move, 2);
    rb_define_method(cRedis, "flush_db", Redis_flush_db, 0);
    rb_define_method(cRedis, "flush_all", Redis_flush_all, 0);

    rb_define_method(cRedis, "set", Redis_set, 2);
    rb_define_method(cRedis, "get", Redis_get, 1);
    rb_define_method(cRedis, "getset", Redis_get_set, 2);
    rb_define_method(cRedis, "setnx", Redis_setnx, 2);
    rb_define_method(cRedis, "incr", Redis_incr, 1);
    rb_define_method(cRedis, "incrby", Redis_incrby, 2);
    rb_define_method(cRedis, "decr", Redis_decr, 1);
    rb_define_method(cRedis, "decrby", Redis_decrby, 2);

    rb_define_method(cRedis, "rpush", Redis_rpush, 2);
    rb_define_method(cRedis, "lpush", Redis_lpush, 2);
    rb_define_method(cRedis, "llen", Redis_llen, 1);
    rb_define_method(cRedis, "lrange", Redis_lrange, 3);
    rb_define_method(cRedis, "ltrim", Redis_ltrim, 3);
    rb_define_method(cRedis, "lindex", Redis_lindex, 2);
    rb_define_method(cRedis, "lset", Redis_lset, 3);
    rb_define_method(cRedis, "lrem", Redis_lrem, 3);
    rb_define_method(cRedis, "lpop", Redis_lpop, 1);
    rb_define_method(cRedis, "rpop", Redis_rpop, 1);
    rb_define_method(cRedis, "rpoplpush", Redis_rpoplpush, 2);

    rb_define_method(cRedis, "sadd", Redis_sadd, 2);
    rb_define_method(cRedis, "srem", Redis_srem, 2);
    rb_define_method(cRedis, "spop", Redis_spop, 1);
    rb_define_method(cRedis, "smove", Redis_smove, 3);
    rb_define_method(cRedis, "scard", Redis_scard, 1);
    rb_define_method(cRedis, "sismember", Redis_sismember, 2);
    rb_define_method(cRedis, "srandmember", Redis_srandmember, 1);

    rb_define_method(cRedis, "zadd", Redis_zadd, 3);
    rb_define_method(cRedis, "zrem", Redis_zrem, 2);
    rb_define_method(cRedis, "zincrby", Redis_zincrby, 3);
    rb_define_method(cRedis, "zrank", Redis_zrank, 2);
    rb_define_method(cRedis, "zrevrank", Redis_zrevrank, 2);
    rb_define_method(cRedis, "zcard", Redis_zcard, 1);
    rb_define_method(cRedis, "zscore", Redis_zscore, 2);
    rb_define_method(cRedis, "zremrangebyrank", Redis_zremrangebyrank, 3);
    rb_define_method(cRedis, "zremrangebyscore", Redis_zremrangebyscore, 3);


    cRedisError = rb_define_class("RedisError", rb_eStandardError);
}
