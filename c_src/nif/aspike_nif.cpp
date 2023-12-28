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
#include <aerospike/as_bin.h>
#include <aerospike/as_val.h>

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
    if (!is_aerospike_initialised) {\
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
    CHECK_ALL

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
    CHECK_ALL

    ERL_NIF_TERM rc, msg;
	as_error err;
    as_key key;
	as_record rec;

	as_key_init_str(&key, name_space, set, key_str);
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, bin, val);


    if (aerospike_key_put(&as, &err, NULL, &key, &rec)  != AEROSPIKE_OK) {
        rc = erl_error;;
        msg = enif_make_string(env, err.message, ERL_NIF_UTF8);
    } else {
        rc = erl_ok;
        msg = enif_make_string(env, "key_put", ERL_NIF_UTF8);
    }

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

static ERL_NIF_TERM format_value(ErlNifEnv* env, as_val_t type, as_bin_value *val) {
    switch(type) {
        case AS_INTEGER:
            return enif_make_int64(env, val->integer.value);
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
    
	uint16_t num_bins = as_record_numbins(p_rec);

    ERL_NIF_TERM *lst = (ERL_NIF_TERM *)enif_alloc(sizeof(ERL_NIF_TERM)* num_bins);
    int n = 0;
    while (as_record_iterator_has_next(&it)) {
        const as_bin* p_bin = as_record_iterator_next(&it);
        char* name = as_bin_get_name(p_bin);
        uint type = as_bin_get_type(p_bin);
		lst[n++] = enif_make_tuple2(env,
            enif_make_string(env, name, ERL_NIF_UTF8),
            format_value(env, type, as_bin_get_value(p_bin))
            );
	}

    res = enif_make_list_from_array(env, lst, num_bins);
    enif_free(lst);
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

    return enif_make_tuple2(env, rc, msg);
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
    ERL_NIF_TERM rc, msg;
    int n_nodes = 0;
    char* node_names = 0;
    as_cluster_get_node_names(as.cluster, &n_nodes, &node_names);

    ERL_NIF_TERM *lst = (ERL_NIF_TERM *)enif_alloc(sizeof(ERL_NIF_TERM)* n_nodes);
    
    for(int i = 0; i < n_nodes; i++){
        lst[i] = enif_make_string(env, &node_names[i], ERL_NIF_UTF8);
    }
    rc = erl_ok;
    msg = enif_make_list_from_array(env, lst, n_nodes);
    enif_free(lst);

    return enif_make_tuple2(env, rc, msg);   
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
    {"host_add", 2, host_add},
    {"connect", 2, connect},
    {"key_put", 5, key_put},
    {"key_remove", 3, key_remove},
    {"key_get", 3, key_get},
    {"key_generation", 3, key_generation},
    {"node_random", 0, node_random},
    {"node_names", 0, node_names},
    {"node_get", 1, node_get},
    {"nif_node_info", 2, nif_node_info},
    {"nif_help", 1, nif_help},
    {"nif_host_info", 3, nif_host_info},
    // ----------------------------------------------------
    {"foo", 1, foo_nif},
    {"bar", 1, bar_nif}
};

ERL_NIF_INIT(aspike_nif, nif_funcs, NULL, NULL, NULL, NULL)
