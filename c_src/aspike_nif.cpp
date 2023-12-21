#include <erl_nif.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_node.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_lookup.h>

// ----------------------------------------------------------------------------

#define MAX_HOST_SIZE 1024
#define MAX_KEY_STR_SIZE 1024
#define MAX_NAMESPACE_SIZE 32	// based on current server limit
#define MAX_SET_SIZE 64			// based on current server limit
#define AS_BIN_NAME_MAX_SIZE 16


// ----------------------------------------------------------------------------

static aerospike as;
static bool is_aerospike_initialised = false;
static bool is_connected = false;

// ----------------------------------------------------------------------------


static ERL_NIF_TERM as_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (!is_aerospike_initialised) {
        as_config config;
        as_config_init(&config);
        aerospike_init(&as, &config);
        is_aerospike_initialised = true;
    }
    ERL_NIF_TERM rc = enif_make_atom(env, "ok");
    ERL_NIF_TERM msg = enif_make_string(env, "initialised", ERL_NIF_UTF8);
    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM add_hosts(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char host[MAX_HOST_SIZE];
    int port;
    if (!enif_get_string(env, argv[0], host, MAX_HOST_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_int(env, argv[1], &port)) {
	    return enif_make_badarg(env);
    }

    ERL_NIF_TERM rc, msg;

    if (! as_config_add_hosts(&as.config, host, port)) {
        rc = enif_make_atom(env, "error");
        msg = enif_make_string(env, "failed to add host and port", ERL_NIF_UTF8);
    } else {
        rc = enif_make_atom(env, "ok");
        msg = enif_make_string(env, "host and port added", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM connect(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char user[AS_USER_SIZE];
    char password[AS_PASSWORD_SIZE];

    if (!enif_get_string(env, argv[0], user, AS_USER_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], password, AS_PASSWORD_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }

    ERL_NIF_TERM rc, msg;
    as_config_set_user(&as.config, user, password);
	as_error err;

    if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
        rc = enif_make_atom(env, "error");
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        is_connected = false;
    } else {
        rc = enif_make_atom(env, "ok");
        msg = enif_make_string(env, "connected", ERL_NIF_UTF8);
        is_connected = true;
    }

    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM key_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char bin[AS_BIN_NAME_MAX_SIZE];
    long val;
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], bin, AS_USER_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_long(env, argv[1], &val)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[3], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[4], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }

    ERL_NIF_TERM rc, msg;
	as_error err;
    as_key key;
	as_record rec;

	as_key_init_str(&key, name_space, set, key_str);
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, bin, val);


    if (aerospike_key_put(&as, &err, NULL, &key, &rec)  != AEROSPIKE_OK) {
        rc = enif_make_atom(env, "error");
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        // return enif_make_tuple2(env, rc, msg);
    } else {
        rc = enif_make_atom(env, "ok");
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
        // return enif_make_uint(env, 1);
    }

    return enif_make_tuple2(env, rc, msg);
}

// ------------------------------------------------------------------------------------------------

extern int foo(int x);
extern int bar(int y);

static ERL_NIF_TERM foo_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int x, ret;
    if (!enif_get_int(env, argv[0], &x)) {
	    return enif_make_badarg(env);
    }
    ret = foo(x);
    return enif_make_int(env, ret);
}

static ERL_NIF_TERM bar_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int y, ret;
    if (!enif_get_int(env, argv[0], &y)) {
	    return enif_make_badarg(env);
    }
    ret = bar(y);
    return enif_make_int(env, ret);
}

static ErlNifFunc nif_funcs[] = {
    {"as_init", 0, as_init},
    {"add_hosts", 2, add_hosts},
    {"connect", 2, connect},
    {"key_put", 5, key_put},
    // ----------------------------------------------------
    {"foo", 1, foo_nif},
    {"bar", 1, bar_nif}
};

ERL_NIF_INIT(aspike_nif, nif_funcs, NULL, NULL, NULL, NULL)
