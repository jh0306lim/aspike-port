#include <erl_nif.h>

#include <time.h>
#include <string>
#include <utility>
#include <iostream>

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
#include <aerospike/as_bin.h>
#include <aerospike/as_val.h>
#include <aerospike/as_arraylist.h>


// ----------------------------------------------------------------------------

#define MAX_HOST_SIZE 1024
#define MAX_KEY_STR_SIZE 1024
#define MAX_NAMESPACE_SIZE 32	// based on current server limit
#define MAX_SET_SIZE 64			// based on current server limit
#define AS_BIN_NAME_MAX_SIZE 16
#define MAX_BINS_NUMBER 1024

#define USE_DIRTY 1
#ifdef USE_DIRTY
#define NIF_FUN(A, B, C) {A, B, C, ERL_DIRTY_JOB_IO_BOUND}
#else
#define NIF_FUN(A, B, C) {A, B, C}
#endif

// ----------------------------------------------------------------------------

static aerospike as;
static bool is_aerospike_initialised = false;
static bool is_connected = false;
static ERL_NIF_TERM erl_error;
static ERL_NIF_TERM erl_ok;

// ----------------------------------------------------------------------------

#define CHECK_AEROSPIKE_INIT \
    if (!is_aerospike_initialised) {\
        return enif_make_tuple2(env,\
            enif_make_atom(env, "error"),\
            enif_make_string(env, "aerospike not initialised", ERL_NIF_UTF8));\
    }

#define CHECK_IS_CONNECTED \
    if (!is_connected) {\
        return enif_make_tuple2(env,\
            enif_make_atom(env, "error"),\
            enif_make_string(env, "not connected", ERL_NIF_UTF8));\
    }

#define CHECK_INIT CHECK_AEROSPIKE_INIT

#define CHECK_ALL\
    CHECK_INIT\
    CHECK_IS_CONNECTED

// ----------------------------------------------------------------------------

static ERL_NIF_TERM as_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (!is_aerospike_initialised) {
        as_config config;
        as_config_init(&config);
        aerospike_init(&as, &config);
        is_aerospike_initialised = true;
    }
    erl_error = enif_make_atom(env, "error");
    erl_ok = enif_make_atom(env, "ok");
    ERL_NIF_TERM msg = enif_make_string(env, "initialised", ERL_NIF_UTF8);
    return enif_make_tuple2(env, erl_ok, msg);
}

static ERL_NIF_TERM host_add(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char host[MAX_HOST_SIZE];
    int port;
    if (!enif_get_string(env, argv[0], host, MAX_HOST_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_int(env, argv[1], &port)) {
	    return enif_make_badarg(env);
    }
    CHECK_INIT

    ERL_NIF_TERM rc, msg;

    if (! as_config_add_hosts(&as.config, host, port)) {
        rc = erl_error;
        msg = enif_make_string(env, "failed to add host and port", ERL_NIF_UTF8);
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, "host and port added", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM host_clear(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    CHECK_INIT
    as_config_clear_hosts(&as.config);
    ERL_NIF_TERM rc = erl_ok;
    ERL_NIF_TERM msg = enif_make_string(env, "hosts list was cleared", ERL_NIF_UTF8);
    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM host_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    CHECK_INIT
    as_config  *config = &as.config;   
    as_vector  *hosts = config->hosts;
    uint32_t size = (hosts == NULL) ? 0 : hosts->size;

    ERL_NIF_TERM msg = enif_make_list(env, 0);
    
    for (uint32_t i = 0; i < size; i++) {
        as_host* host = (as_host*)as_vector_get(hosts, i);
		ERL_NIF_TERM cell = enif_make_tuple3(env,
            enif_make_string(env, host->name, ERL_NIF_UTF8),
            enif_make_string(env,  host->tls_name == NULL ? "" : host->tls_name, ERL_NIF_UTF8),
            enif_make_uint(env, host->port)
            );
        msg = enif_make_list_cell(env, cell, msg);
	}

    return enif_make_tuple2(env, erl_ok, msg);
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
    CHECK_AEROSPIKE_INIT

    ERL_NIF_TERM rc, msg;
    as_config_set_user(&as.config, user, password);
	as_error err;

    if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
        rc = erl_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        is_connected = false;
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, "connected", ERL_NIF_UTF8);
        is_connected = true;
    }

    return enif_make_tuple2(env, rc, msg);
}


static ERL_NIF_TERM binary_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin_ns, bin_set, bin_key;
    unsigned int length;
    std::string name_space, aspk_set, aspk_key;
    long ttl;

    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
	    return enif_make_badarg(env);
    }
    name_space.assign((const char*) bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
	    return enif_make_badarg(env);
    }
    aspk_set.assign((const char*) bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
	    return enif_make_badarg(env);
    }
    aspk_key.assign((const char*) bin_key.data, bin_key.size);

    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
	    return enif_make_badarg(env);
    }

    if (!enif_get_long(env, argv[4], &ttl)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM rc, msg;
    if (length == 0) {
        rc = erl_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    CHECK_ALL

	as_error err;
    as_key key;
	as_record rec;

	as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());
	as_record_inita(&rec, length);
    rec.ttl = ttl;
    
    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        ErlNifBinary bin_bin, bin_val;
        std::string bin_str, bin_str_val;
        int t_length;
        const ERL_NIF_TERM* tuple = NULL;
        as_bytes as_bytes_val;
        unsigned int ts_length;

        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if(!enif_get_tuple(env, head, &t_length, &tuple) || t_length != 2){
            return enif_make_badarg(env);
        }

        if (!enif_inspect_binary(env, tuple[0], &bin_bin)) {
            return enif_make_badarg(env);
        }
        bin_str.assign((const char*) bin_bin.data, bin_bin.size);

        if (!enif_inspect_binary(env, tuple[1], &bin_val)) {
            if (!enif_is_list(env, tuple[1]) || !enif_get_list_length(env, tuple[1], &ts_length)) {
                return enif_make_badarg(env);
            }
            // expecting list of integers
            auto ts_list = tuple[1];
            as_list* as_list_ofints = (as_list *)as_arraylist_new((uint32_t)ts_length, 0);
            for (uint ts_i = 0; ts_i < ts_length; ts_i++) {
                ERL_NIF_TERM ts_head;
                ERL_NIF_TERM ts_tail;
                long i64;
                if (!enif_get_list_cell(env, ts_list, &ts_head, &ts_tail)) {
                    break;
                }
                if(enif_get_int64(env, ts_head, &i64)){
                    std::cout << "insertind INT: " << i64 << "\r\n";
                    as_list_append_int64(as_list_ofints, i64);
                }else{
                    std::cout << "Not INT: " << i64 << "\r\n";
                }
                ts_list = ts_tail;
            }
            as_record_set_list(&rec, bin_str.c_str(), as_list_ofints);
            //as_list_destroy(as_list_ofints);??????????
            //
        }else{
            as_bytes_init_wrap(&as_bytes_val, bin_val.data, bin_val.size, true);
            as_bytes_set_type(&as_bytes_val, AS_BYTES_BLOB); // AS_BYTES_BLOB
            as_record_set_bytes(&rec, bin_str.c_str(), &as_bytes_val);
        }

        list = tail;
    }

    if (aerospike_key_put(&as, &err, NULL, &key, &rec)  != AEROSPIKE_OK) {
        rc = erl_error;;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM key_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    unsigned int length;

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL
                                        // enif_get_list_length(env, *val, &len);
    ERL_NIF_TERM rc, msg;
    if (length == 0) {
        rc = erl_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

	as_error err;
    as_key key;
	as_record rec;

	as_key_init_str(&key, name_space, set, key_str);
	as_record_inita(&rec, length);

    
    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        char bin[AS_BIN_NAME_MAX_SIZE] = {0};
        long val = 0;
        int t_length;
        const ERL_NIF_TERM* tuple = NULL;

        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if(!enif_get_tuple(env, head, &t_length, &tuple) || t_length != 2){
            return enif_make_badarg(env);
        }
        if (!enif_get_string(env, tuple[0], bin, MAX_SET_SIZE, ERL_NIF_UTF8)) {
    	    return enif_make_badarg(env);
        }
        if (!enif_get_long(env, tuple[1], &val)) {
            return enif_make_badarg(env);
        }
        as_record_set_int64(&rec, bin, val);
        list = tail;
    }

    if (aerospike_key_put(&as, &err, NULL, &key, &rec)  != AEROSPIKE_OK) {
        rc = erl_error;;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM key_inc(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    unsigned int length;

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL

    ERL_NIF_TERM rc, msg;
    if (length == 0) {
        rc = erl_ok;
        msg = enif_make_string(env, "key_inc", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

	as_error err;
    as_key key;
	as_key_init_str(&key, name_space, set, key_str);
	
    as_operations ops;
	as_operations_inita(&ops, length);

    for (uint i = 0; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        char bin[AS_BIN_NAME_MAX_SIZE] = {0};
        long val = 0;
        int t_length;
        const ERL_NIF_TERM* tuple = NULL;

        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if(!enif_get_tuple(env, head, &t_length, &tuple) || t_length != 2){
            return enif_make_badarg(env);
        }
        if (!enif_get_string(env, tuple[0], bin, MAX_SET_SIZE, ERL_NIF_UTF8)) {
    	    return enif_make_badarg(env);
        }
        if (!enif_get_long(env, tuple[1], &val)) {
            return enif_make_badarg(env);
        }
        as_operations_add_incr(&ops, bin, val);
        list = tail;
    }

    if (aerospike_key_operate(&as, &err, NULL, &key, &ops, NULL)  != AEROSPIKE_OK) {
        rc = erl_error;;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, "key_inc", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

// #define CLOCK_REALTIME 0 
// #define CLOCK_MONOTONIC 6 
// #define CLOCK_PROCESS_CPUTIME_ID  12 
// #define CLOCK_THREAD_CPUTIME_ID  16 

static ERL_NIF_TERM a_key_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char bin[AS_BIN_NAME_MAX_SIZE];
    long val;
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    long n;

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
    if (!enif_get_long(env, argv[5], &n)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL

    ERL_NIF_TERM rc, msg;
	as_error err;
    as_key key;
	as_record rec;

	as_key_init_str(&key, name_space, set, key_str);
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, bin, val);

    struct timespec thread_start, thread_done;
    struct timespec real_start, real_done;
    
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &thread_start);
    clock_gettime(CLOCK_REALTIME, &real_start);

    for (uint i=0; i < n; i++) {
        if (aerospike_key_put(&as, &err, NULL, &key, &rec)  != AEROSPIKE_OK) {
            rc = erl_error;
            msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
            return enif_make_tuple2(env, rc, msg);
        }
    }
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &thread_done);
    clock_gettime(CLOCK_REALTIME, &real_done);
    // Convert to microseconds
    ErlNifSInt64 thread_tspent = (thread_done.tv_sec - thread_start.tv_sec) * 1000000 + (thread_done.tv_nsec - thread_start.tv_nsec) / 1000;
    ErlNifSInt64 real_tspent = (real_done.tv_sec - real_start.tv_sec) * 1000000 + (real_done.tv_nsec - real_start.tv_nsec) / 1000;

    rc = erl_ok;

    ERL_NIF_TERM keys[3];
    ERL_NIF_TERM vals[3];
    keys[0] = enif_make_string(env, "repetitions", ERL_NIF_UTF8);
    vals[0] = enif_make_uint64(env, n);
    keys[1] = enif_make_string(env, "CLOCK_THREAD_CPUTIME_ID", ERL_NIF_UTF8);
    vals[1] = enif_make_double(env, thread_tspent/n);    // from micro to mille seconds
    keys[2] = enif_make_string(env, "CLOCK_REALTIME", ERL_NIF_UTF8);
    vals[2] = enif_make_double(env, real_tspent/n);       // from micro to mille seconds
    enif_make_map_from_arrays(env, keys, vals, 3, &msg);
    return enif_make_tuple2(env, rc, msg);

}


static ERL_NIF_TERM key_remove(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL

    ERL_NIF_TERM rc, msg;
	as_error err;
    as_key key;

	as_key_init_str(&key, name_space, set, key_str);

    if (aerospike_key_remove(&as, &err, NULL, &key)  != AEROSPIKE_OK) {
        rc = erl_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc =erl_ok;
        msg = enif_make_string(env, "key_remove", ERL_NIF_UTF8);
    }

    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM format_value_out(ErlNifEnv* env, as_val_t type, as_bin_value *val) {
    switch(type) {
        case AS_INTEGER:
            return enif_make_int64(env, val->integer.value);
        case AS_STRING:
        case AS_BYTES: {
            as_bytes asbval = val->bytes;
            uint8_t * bin_as_str = as_bytes_get(&asbval);
            auto len = asbval.size;

            unsigned char * val_data;
            ERL_NIF_TERM res;
            val_data = enif_make_new_binary(env, len, &res);
            memcpy(val_data, bin_as_str, len);
            return res;
        }break;
        default:
            char * val_as_str = as_val_tostring(val);
            ERL_NIF_TERM res =  enif_make_string(env, as_val_tostring(val), ERL_NIF_UTF8);
            free(val_as_str);
            return res;
    }
}

static ERL_NIF_TERM dump_records(ErlNifEnv* env, const as_record *p_rec) {
    ERL_NIF_TERM res;

	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);
        res = enif_make_string(env, key_val_as_str, ERL_NIF_UTF8);
		free(key_val_as_str);
        return res;
	}

	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);
    
    res = enif_make_list(env, 0);

    while (as_record_iterator_has_next(&it)) {
        const as_bin* p_bin = as_record_iterator_next(&it);
        char* name = as_bin_get_name(p_bin);
        uint type = as_bin_get_type(p_bin);
		ERL_NIF_TERM cell = enif_make_tuple2(env,
            enif_make_string(env, name, ERL_NIF_UTF8),
            format_value_out(env, type, as_bin_get_value(p_bin))
            );
        res = enif_make_list_cell(env, cell, res);
	}

	as_record_iterator_destroy(&it);

    return res;
}

static ERL_NIF_TERM key_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL

    ERL_NIF_TERM rc, msg;
	as_error err;
    as_key key;
    as_record* p_rec = NULL;    

	as_key_init_str(&key, name_space, set, key_str);

    if (aerospike_key_get(&as, &err, NULL, &key, &p_rec)  != AEROSPIKE_OK) {
        if (p_rec != NULL) {
            as_record_destroy(p_rec);
        }
        rc = erl_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    if (p_rec == NULL) {
        rc = erl_error;
        msg = enif_make_string(env, "NULL p_rec - internal error", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    msg = dump_records(env, p_rec);
    rc = erl_ok;
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, rc, msg);
}


static ERL_NIF_TERM dump_binary_records(ErlNifEnv* env, const as_record *p_rec) {
    ERL_NIF_TERM res;
	
    if (p_rec->key.valuep) {
        unsigned char* key_data;
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);
        auto len = strlen(key_val_as_str);

        key_data = enif_make_new_binary(env, len, &res);
        memcpy(key_data, key_val_as_str, len);
        //res = enif_make_string(env, key_val_as_str, ERL_NIF_UTF8);
		free(key_val_as_str);
        return res;
	}

	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);
    
    res = enif_make_list(env, 0);

    while (as_record_iterator_has_next(&it)) {
        const as_bin* p_bin = as_record_iterator_next(&it);
        char* name = as_bin_get_name(p_bin);
        auto namelen = strlen(name);
        uint type = as_bin_get_type(p_bin);

        unsigned char* name_data;
        ERL_NIF_TERM name_term;
        name_data = enif_make_new_binary(env, namelen, &name_term);
        memcpy(name_data, name, namelen);

		ERL_NIF_TERM cell = enif_make_tuple2(env,
            name_term,
            format_value_out(env, type, as_bin_get_value(p_bin))
            );
        res = enif_make_list_cell(env, cell, res);
	}

	as_record_iterator_destroy(&it);

    return res;

}

static ERL_NIF_TERM binary_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin_ns, bin_set, bin_key;
    std::string name_space, aspk_set, aspk_key;
    
    if (!enif_inspect_binary(env, argv[0], &bin_ns)) {
	    return enif_make_badarg(env);
    }
    name_space.assign((const char*) bin_ns.data, bin_ns.size);

    if (!enif_inspect_binary(env, argv[1], &bin_set)) {
	    return enif_make_badarg(env);
    }
    aspk_set.assign((const char*) bin_set.data, bin_set.size);

    if (!enif_inspect_binary(env, argv[2], &bin_key)) {
	    return enif_make_badarg(env);
    }
    aspk_key.assign((const char*) bin_key.data, bin_key.size);

    CHECK_ALL

    ERL_NIF_TERM rc, msg;
	as_error err;
    as_key key;
    as_record* p_rec = NULL;    

	as_key_init_str(&key, name_space.c_str(), aspk_set.c_str(), aspk_key.c_str());

    if (aerospike_key_get(&as, &err, NULL, &key, &p_rec)  != AEROSPIKE_OK) {
        if (p_rec != NULL) {
            as_record_destroy(p_rec);
        }
        rc = erl_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    if (p_rec == NULL) {
        rc = erl_error;
        msg = enif_make_string(env, "NULL p_rec - internal error", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    msg = dump_binary_records(env, p_rec);
    rc = erl_ok;
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, rc, msg);

//
}

static ERL_NIF_TERM key_select(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    unsigned int length = 0;
    unsigned int i = 0;
    as_record* p_rec = NULL;

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    ERL_NIF_TERM list = argv[3];
    if (!enif_is_list(env, list) || !enif_get_list_length(env, list, &length)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL

    ERL_NIF_TERM rc, msg;

    if (length == 0) {
        msg = enif_make_list(env, 0);  // empty list
        rc = erl_ok;
        return enif_make_tuple2(env, rc, msg);
    }

    const char *bins[MAX_BINS_NUMBER];
    for (; i < length; i++) {
        ERL_NIF_TERM head;
        ERL_NIF_TERM tail;
        char bin[AS_BIN_NAME_MAX_SIZE] = {0};
        if (!enif_get_list_cell(env, list, &head, &tail)) {
            break;
        }
        if(!enif_get_string(env, head, bin, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)){
            break;
        }
        bins[i] = strdup(bin);
        list = tail;
    }
    bins[i] = NULL;

	as_error err;
    as_key key;
	as_key_init_str(&key, name_space, set, key_str);

    if (aerospike_key_select(&as, &err, NULL, &key, bins, &p_rec)  != AEROSPIKE_OK) {
        rc = erl_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }

    msg = dump_records(env, p_rec);
    rc = erl_ok;
    for (uint j = 0; j < i; j++) {
        delete(bins[j]);
    }
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }

    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM key_generation(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL

    ERL_NIF_TERM rc, msg;
	as_error err;
    as_key key;
    as_record* p_rec = NULL;    

	as_key_init_str(&key, name_space, set, key_str);

    if (aerospike_key_get(&as, &err, NULL, &key, &p_rec)  != AEROSPIKE_OK) {
        rc = erl_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    if (p_rec == NULL) {
        rc = erl_error;
        msg = enif_make_string(env, "NULL p_rec - internal error", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    ERL_NIF_TERM keys[2];
    ERL_NIF_TERM vals[2];
    keys[0] = enif_make_string(env, "gen", ERL_NIF_UTF8);
    vals[0] = enif_make_uint64(env, p_rec->gen);
    keys[1] = enif_make_string(env, "ttl", ERL_NIF_UTF8);
    vals[1] = enif_make_uint64(env, p_rec->ttl);
    enif_make_map_from_arrays(env, keys, vals, 2, &msg);
    rc = erl_ok;
    as_record_destroy(p_rec);
    return enif_make_tuple2(env, rc, msg);
}

static ERL_NIF_TERM key_exists(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name_space[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (!enif_get_string(env, argv[0], name_space, MAX_NAMESPACE_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], set, MAX_SET_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], key_str, MAX_KEY_STR_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL

    ERL_NIF_TERM rc, msg;
    as_error err;
    as_key key;
    as_record* p_rec = NULL;    

	as_key_init_str(&key, name_space, set, key_str);
    int as_rc = aerospike_key_exists(&as, &err, NULL, &key, &p_rec);

    if (as_rc != AEROSPIKE_OK) {
        rc = erl_error;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);
    }
    rc = erl_ok;
    static ERL_NIF_TERM tr = enif_make_atom(env, "true");
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    return enif_make_tuple2(env, rc, tr);
}

static ERL_NIF_TERM node_random(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    CHECK_ALL
    ERL_NIF_TERM rc, msg;
    as_node* node = as_node_get_random(as.cluster);
    if (! node) {
        rc = erl_error;
        msg = enif_make_string(env, "Failed to find server node.", ERL_NIF_UTF8);
	} else {
        rc = erl_ok;
        msg = enif_make_string(env, as_node_get_address_string(node), ERL_NIF_UTF8);
        as_node_release(node);
    }

    return enif_make_tuple2(env, rc, msg);   
}

static ERL_NIF_TERM node_names(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    CHECK_ALL
    ERL_NIF_TERM rc;
	as_nodes* nodes = as_nodes_reserve(as.cluster);
    uint32_t n_nodes = (nodes == NULL) ? 0 : nodes->size;

    ERL_NIF_TERM lst = enif_make_list(env, 0);

    for(uint32_t i = 0; i < n_nodes; i++){
        as_node* node = nodes->array[i];
        ERL_NIF_TERM cell = enif_make_tuple2(
            env,
            enif_make_string(env, node->name, ERL_NIF_UTF8),
            enif_make_string(env, as_node_get_address_string(node), ERL_NIF_UTF8)
            );
        lst = enif_make_list_cell(env, cell, lst);    
    }

    rc = erl_ok;
    as_nodes_release(nodes);

    return enif_make_tuple2(env, rc, lst);   
}

static ERL_NIF_TERM node_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char node_name[AS_NODE_NAME_MAX_SIZE];
    if (!enif_get_string(env, argv[0], node_name, AS_USER_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL
    ERL_NIF_TERM rc, msg;
    
    as_node* node = as_node_get_by_name(as.cluster, node_name);
    if (! node) {
        rc = erl_error;
        msg = enif_make_string(env, "Failed to find server node.", ERL_NIF_UTF8);
	} else {
        rc = erl_ok;
        msg = enif_make_string(env, as_node_get_address_string(node), ERL_NIF_UTF8);
        as_node_release(node);
    }

    return enif_make_tuple2(env, rc, msg);   
}

static ERL_NIF_TERM nif_node_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char node_name[AS_NODE_NAME_MAX_SIZE];
    char item[1024];
    if (!enif_get_string(env, argv[0], node_name, AS_USER_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], item, AS_USER_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL
    ERL_NIF_TERM rc, msg;

	as_cluster* cluster = as.cluster;
    as_node* node = as_node_get_by_name(cluster, node_name);
    if (! node) {
        rc = erl_error;
        msg = enif_make_string(env, "Failed to find server node.", ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);   
	}

    char * info = NULL;
    as_error err;
    const as_policy_info* policy = &as.config.policies.info;
	uint64_t deadline = as_socket_deadline(policy->timeout);

    as_status status = as_info_command_node(&err, node, (char*)item, policy->send_as_is, deadline, &info);
    if (status != AEROSPIKE_OK) {
        rc = erl_error;
        msg = enif_make_string(env, err.in_doubt == true ? "unknown error" : err.message, ERL_NIF_UTF8);
    } else if (info == NULL) {
        rc = erl_error;
        msg = enif_make_string(env, "no data", ERL_NIF_UTF8);
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, &info[0], ERL_NIF_UTF8);
        free(info);
    }
    as_node_release(node);

    return enif_make_tuple2(env, rc, msg);   
}


static ERL_NIF_TERM nif_help(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char item[1024];
    if (!enif_get_string(env, argv[0], item, AS_USER_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL
    ERL_NIF_TERM rc, msg;
    char * info = NULL;
    as_error err;

    if (aerospike_info_any(&as, &err, NULL, item, &info) != AEROSPIKE_OK) {
        rc = erl_error;
        msg = enif_make_string(env, err.in_doubt == true ? "unknown error" : err.message, ERL_NIF_UTF8);
    } else if (info == NULL) {
        rc = erl_error;
        msg = enif_make_string(env, "no data", ERL_NIF_UTF8);
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, info, ERL_NIF_UTF8);
        free(info);
    }

    return enif_make_tuple2(env, rc, msg);   
}

static ERL_NIF_TERM nif_host_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char hostname[AS_NODE_NAME_MAX_SIZE];
    long port;
    char item[1024];    
    if (!enif_get_string(env, argv[0], hostname, AS_USER_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_long(env, argv[1], &port)) {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], item, AS_USER_SIZE, ERL_NIF_UTF8)) {
	    return enif_make_badarg(env);
    }
    CHECK_ALL
    ERL_NIF_TERM rc, msg;
    as_error err;
    as_address_iterator iter;

	as_status status = as_lookup_host(&iter, &err, hostname, port);	
	if (status != AEROSPIKE_OK) {
        rc = erl_error;
        msg = enif_make_string(env, err.in_doubt == true ? "unknown error" : err.message, ERL_NIF_UTF8);
        return enif_make_tuple2(env, rc, msg);   
	}
    char * info = NULL;
    as_cluster* cluster = as.cluster;
    const as_policy_info* policy = &as.config.policies.info;
	uint64_t deadline = as_socket_deadline(policy->timeout);
	struct sockaddr* addr;

	bool loop = true;
	while (loop && as_lookup_next(&iter, &addr)) {
		status = as_info_command_host(cluster, &err, addr, (char*)item, policy->send_as_is, deadline, &info, hostname);
		
		switch (status) {
			case AEROSPIKE_OK:
			case AEROSPIKE_ERR_TIMEOUT:
			case AEROSPIKE_ERR_INDEX_FOUND:
			case AEROSPIKE_ERR_INDEX_NOT_FOUND:
				loop = false;
				break;
				
			default:
				break;
		}
	}
	as_lookup_end(&iter);

    if (info == NULL) {
        rc = erl_error;
        msg = enif_make_string(env, "no data", ERL_NIF_UTF8);
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, &info[0], ERL_NIF_UTF8);
        free(info);
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
    NIF_FUN("connect", 2, connect),
    NIF_FUN("nif_host_add", 2, host_add),
    NIF_FUN("host_clear", 0, host_clear),
    NIF_FUN("nif_host_list", 0, host_list),
    NIF_FUN("key_exists", 3, key_exists),
    NIF_FUN("key_inc", 4, key_inc),
    NIF_FUN("key_get", 3, key_get),
    NIF_FUN("key_generation", 3, key_generation),
    NIF_FUN("key_put", 4, key_put),
    NIF_FUN("binary_put", 5, binary_put),
    NIF_FUN("binary_get", 3, binary_get),
    NIF_FUN("key_remove", 3, key_remove),
    NIF_FUN("key_select", 4, key_select),
    NIF_FUN("nif_node_random", 0, node_random),
    NIF_FUN("nif_node_names", 0, node_names),
    NIF_FUN("nif_node_get", 1, node_get),
    NIF_FUN("nif_node_info", 2, nif_node_info),
    NIF_FUN("nif_help", 1, nif_help),
    NIF_FUN("nif_host_info", 3, nif_host_info),
    // ----------------------------------------------------
    NIF_FUN("a_key_put", 6, a_key_put),
    {"foo", 1, foo_nif},
    {"bar", 1, bar_nif}
};

ERL_NIF_INIT(aspike_nif, nif_funcs, NULL, NULL, NULL, NULL)
