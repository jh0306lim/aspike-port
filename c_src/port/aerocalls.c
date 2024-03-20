/* aerocalls.c */

#include "ei.h"
#include <string.h>
#include <stdlib.h>
#include <cstring>
#include <string>
#include <fstream>
#include <iostream>
#include <vector>

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
#include <aerospike/as_arraylist.h>

// ----------------------------------------------------------------------------

#define MAX_HOST_SIZE 1024
#define MAX_KEY_STR_SIZE 1024
#define MAX_NAMESPACE_SIZE 32	// based on current server limit
#define MAX_SET_SIZE 64			// based on current server limit
#define AS_BIN_NAME_MAX_SIZE 16
#define MAX_BINS_NUMBER 1024

const char DEFAULT_HOST[] = "127.0.0.1";
// const int DEFAULT_PORT = 3000;
const int DEFAULT_PORT = 3010;
const char DEFAULT_NAMESPACE[] = "test";
const char DEFAULT_SET[] = "eg-set";
const char DEFAULT_KEY_STR[] = "eg-key";
const uint32_t DEFAULT_NUM_KEYS = 20;

typedef struct {
    ei_x_buff* env;
    uint32_t count;
} conversion_data;
// ----------------------------------------------------------------------------

#define OK0\
    res = 1;\
    ei_x_encode_atom(&res_buf, "ok");
#define OK(MSG)\
    OK0\
    ei_x_encode_string(&res_buf, MSG);

#define OK0P\
    res = 1;\
    ei_x_encode_atom(p_res_buf, "ok");
#define OKP(MSG)\
    OK0P\
    ei_x_encode_string(p_res_buf, MSG);


#define ERROR0\
    res = 0;\
    ei_x_encode_atom(&res_buf, "error");
#define ERROR(MSG)\
    ERROR0\
    ei_x_encode_string(&res_buf, MSG);
#define ERROR0P\
    res = 0;\
    ei_x_encode_atom(p_res_buf, "error");
#define ERRORP(MSG)\
    ERROR0P\
    ei_x_encode_string(p_res_buf, MSG);


#define PRE \
    int res = 1;\
    ei_x_buff res_buf;\
    ei_x_new_with_version(&res_buf);\
    ei_x_encode_tuple_header(&res_buf, 2);

#define POST \
    write_cmd(res_buf.buff, res_buf.index, fd_out);\
    ei_x_free(&res_buf);\
    return res;

#define STOPERROR(MSG)\
    ERROR(MSG)\
    POST

#define CHECK_AEROSPIKE_INIT \
    if (!is_aerospike_initialised) {\
        ERROR("aerospike is not initialise")\
        POST\
    }

#define CHECK_IS_CONNECTED \
    if (!is_connected) {\
        ERROR("not connected")\
        POST\
    }

#define CHECK_INIT CHECK_AEROSPIKE_INIT

#define CHECK_ALL\
    CHECK_INIT\
    CHECK_IS_CONNECTED

// ----------------------------------------------------------------------------

static aerospike as;
int is_aerospike_initialised = 0;
int is_connected = 0;

// ----------------------------------------------------------------------------

typedef char byte;

int write_cmd(byte *buf, int len, int fd);


int ifail(int ind, int fd);
int fail(const char *msg, int fd);
int note(const char *msg, int fd);
int is_function_call(const char *buf, int *index, int *arity);
int function_call(const char *buf, int *index, int arity, int fd_out);

int call_cluster_info(const char *buf, int *index, int arity, int fd_out);
int call_config_info(const char *buf, int *index, int arity, int fd_out);

int call_connect(const char *buf, int *index, int arity, int fd_out);

int call_aerospike_init(const char *buf, int *index, int arity, int fd_out);

int call_config_add_hosts(const char *buf, int *index, int arity, int fd_out);
int call_config_clear_hosts(const char *buf, int *index, int arity, int fd_out);
int call_config_list_hosts(const char *buf, int *index, int arity, int fd_out);

int call_port_status(const char *buf, int *index, int arity, int fd_out);

int call_key_exists(const char *buf, int *index, int arity, int fd_out);
int call_key_inc(const char *buf, int *index, int arity, int fd_out);
int call_key_get(const char *buf, int *index, int arity, int fd_out);
int call_binary_key_get(const char *buf, int *index, int arity, int fd_out);
int call_key_generation(const char *buf, int *index, int arity, int fd_out);
int call_key_put(const char *buf, int *index, int arity, int fd_out);
int call_binary_key_put(const char *buf, int *index, int arity, int fd_out);
int call_key_remove(const char *buf, int *index, int arity, int fd_out);
int call_key_select(const char *buf, int *index, int arity, int fd_out);

int call_node_random(const char *buf, int *index, int arity, int fd_out);
int call_node_names(const char *buf, int *index, int arity, int fd_out);
int call_node_get(const char *buf, int *index, int arity, int fd_out);
int call_node_info(const char *buf, int *index, int arity, int fd_out);

int call_host_info(const char *buf, int *index, int arity, int fd_out);

int call_help(const char *buf, int *index, int arity, int fd_out);

int call_foo(const char *buf, int *index, int arity, int fd_out);
int call_bar(const char *buf, int *index, int arity, int fd_out);
void logfile(std::string str);

char err_msg[8][80] = {
    "ei_decode_version",
    "ei_decode_tuple_header",
    "wrong arity",
    "ei_decode_atom",
    "ei_decode_long",
    "ei_x_new_with_version",
    "ei_x_encode_long",
    "ei_x_free"
};

int fail(const char *msg, int fd_out) {
    // fprintf(stderr, "Something went wrong: %s\n", msg);
    PRE
    ERROR(msg);
    POST
}

int ifail(int ind, int fd_out) {
    return fail(err_msg[ind], fd_out);
}

int note(const char *msg, int fd_out) {
    // fprintf(stdout, "note: %s\n", msg);
    PRE
    ei_x_encode_atom(&res_buf, "note");
    ei_x_encode_string(&res_buf, msg);
    POST
}

// static void res_buf_free(ei_x_buff  *res_buf, int fd_out) {
//     if (res_buf != 0 && res_buf->buff != 0 && strncmp(res_buf->buff, "", 1) != 0) { 
//         if (ei_x_free(res_buf) != 0) {
//             ifail(7, fd_out);
//             exit(1);
//         }
//     }
// }

int is_function_call(const char *buf, int *index, int *arity) {
    int res = ei_decode_tuple_header(buf, index, arity) == 0 && arity > 0;
    return res;
}

static int check_name(const char *fname, const char *pattern, int arity, int nargs) {
    return (strncmp(fname, pattern, strlen(pattern)) == 0 && arity == nargs);
}

int function_call(const char *buf, int *index, int arity, int fd_out) {
    char fname[128];
    if (ei_decode_atom(buf, index, fname) != 0) {
        ifail(3, fd_out);
        return 0;
    }

    if (check_name(fname, "aerospike_init", arity, 1)) {
        return call_aerospike_init(buf, index, arity, fd_out);
    }
    if (check_name(fname, "host_add", arity, 3)) {
        return call_config_add_hosts(buf, index, arity, fd_out);
    }
    if (check_name(fname, "host_clear", arity, 1)) {
        return call_config_clear_hosts(buf, index, arity, fd_out);
    }
    if (check_name(fname, "host_list", arity, 1)) {
        return call_config_list_hosts(buf, index, arity, fd_out);
    }
    if (check_name(fname, "connect", arity, 3)) {
        return call_connect(buf, index, arity, fd_out);
    }
    if (check_name(fname, "key_exists", arity, 4)) {
        return call_key_exists(buf, index, arity, fd_out);
    }
    if (check_name(fname, "key_inc", arity, 5)) {
        return call_key_inc(buf, index, arity, fd_out);
    }
    if (check_name(fname, "key_get", arity, 4)) {
        return call_key_get(buf, index, arity, fd_out);
    }
    if (check_name(fname, "binary_key_get", arity, 4)) {
        return call_binary_key_get(buf, index, arity, fd_out);
    }
    if (check_name(fname, "key_generation", arity, 4)) {
        return call_key_generation(buf, index, arity, fd_out);
    }
    if (check_name(fname, "key_put", arity, 5)) {
        return call_key_put(buf, index, arity, fd_out);
    }
    if (check_name(fname, "binary_key_put", arity, 6)) {
        return call_binary_key_put(buf, index, arity, fd_out);
    }
    if (check_name(fname, "key_remove", arity, 4)) {
        return call_key_remove(buf, index, arity, fd_out);
    }
    if (check_name(fname, "key_select", arity, 5)) {
        return call_key_select(buf, index, arity, fd_out);
    }
    if (check_name(fname, "node_random", arity, 1)) {
        return call_node_random(buf, index, arity, fd_out);
    }
    if (check_name(fname, "node_names", arity, 1)) {
        return call_node_names(buf, index, arity, fd_out);
    }
    if (check_name(fname, "node_get", arity, 2)) {
        return call_node_get(buf, index, arity, fd_out);
    }
    if (check_name(fname, "node_info", arity, 3)) {
        return call_node_info(buf, index, arity, fd_out);
    }
    if (check_name(fname, "host_info", arity, 4)) {
        return call_host_info(buf, index, arity, fd_out);
    }
    if (check_name(fname, "config_info", arity, 1)) {
        return call_config_info(buf, index, arity, fd_out);
    }
    if (check_name(fname, "help", arity, 2)) {
        return call_help(buf, index, arity, fd_out);
    }
    if (check_name(fname, "cluster_info", arity, 1)) {
        return call_cluster_info(buf, index, arity, fd_out);
    }
    if (check_name(fname, "port_status", arity, 1)) {
        return call_port_status(buf, index, arity, fd_out);
    }


    if (check_name(fname, "foo", arity, 2)) {
        return call_foo(buf, index, arity, fd_out);
    }
    if (check_name(fname, "bar", arity, 2)) {
        return call_bar(buf, index, arity, fd_out);
    }

    fail(fname, fd_out);
    return 0;
}

int call_config_info(const char *buf, int *index, int arity, int fd_out){
    PRE
    CHECK_AEROSPIKE_INIT     
    OK0

    as_config  *config = &as.config;   
    as_vector  *hosts = config->hosts;

    ei_x_encode_map_header(&res_buf, 29);
    ei_x_encode_string(&res_buf, "user");
    ei_x_encode_string(&res_buf, config->user);
    ei_x_encode_string(&res_buf, "password");
    ei_x_encode_string(&res_buf, config->password);
    ei_x_encode_string(&res_buf, "number_of_hosts");
    ei_x_encode_ulong(&res_buf, hosts == NULL ? 0 : hosts->size);
    ei_x_encode_string(&res_buf, "cluster_name");
    ei_x_encode_string(&res_buf, config->cluster_name == NULL ? "" : config->cluster_name);
    ei_x_encode_string(&res_buf, "ip_map_size");
    ei_x_encode_ulong(&res_buf, config->ip_map_size);
    ei_x_encode_string(&res_buf, "min_conns_per_node");
    ei_x_encode_ulong(&res_buf, config->min_conns_per_node);
    ei_x_encode_string(&res_buf, "max_conns_per_node");
    ei_x_encode_ulong(&res_buf, config->max_conns_per_node);
    ei_x_encode_string(&res_buf, "async_min_conns_per_node");
    ei_x_encode_ulong(&res_buf, config->async_min_conns_per_node);
    ei_x_encode_string(&res_buf, "async_max_conns_per_node");
    ei_x_encode_ulong(&res_buf, config->async_max_conns_per_node);
    ei_x_encode_string(&res_buf, "pipe_max_conns_per_node");
    ei_x_encode_ulong(&res_buf, config->pipe_max_conns_per_node);
    ei_x_encode_string(&res_buf, "conn_pools_per_node");
    ei_x_encode_ulong(&res_buf, config->conn_pools_per_node);
    ei_x_encode_string(&res_buf, "conn_timeout_ms");
    ei_x_encode_ulong(&res_buf, config->conn_timeout_ms);
    ei_x_encode_string(&res_buf, "login_timeout_ms");
    ei_x_encode_ulong(&res_buf, config->login_timeout_ms);
    ei_x_encode_string(&res_buf, "max_socket_idle");
    ei_x_encode_ulong(&res_buf, config->max_socket_idle);
    ei_x_encode_string(&res_buf, "max_error_rate");
    ei_x_encode_ulong(&res_buf, config->max_error_rate);
    ei_x_encode_string(&res_buf, "error_rate_window");
    ei_x_encode_ulong(&res_buf, config->error_rate_window);
    ei_x_encode_string(&res_buf, "tender_interval");
    ei_x_encode_ulong(&res_buf, config->tender_interval);
    ei_x_encode_string(&res_buf, "thread_pool_size");
    ei_x_encode_ulong(&res_buf, config->thread_pool_size);
    ei_x_encode_string(&res_buf, "tend_thread_cpu");
    ei_x_encode_long(&res_buf, config->tend_thread_cpu);
    ei_x_encode_string(&res_buf, "fail_if_not_connected");
    ei_x_encode_boolean(&res_buf, config->fail_if_not_connected);
    ei_x_encode_string(&res_buf, "use_services_alternate");
    ei_x_encode_boolean(&res_buf, config->use_services_alternate);
    ei_x_encode_string(&res_buf, "rack_aware");
    ei_x_encode_boolean(&res_buf, config->rack_aware);
    ei_x_encode_string(&res_buf, "rack_id");
    ei_x_encode_long(&res_buf, config->rack_id);
    ei_x_encode_string(&res_buf, "use_shm");
    ei_x_encode_boolean(&res_buf, config->use_shm);
    ei_x_encode_string(&res_buf, "shm_key");
    ei_x_encode_long(&res_buf, config->shm_key);
    ei_x_encode_string(&res_buf, "shm_max_nodes");
    ei_x_encode_ulong(&res_buf, config->shm_max_nodes);
    ei_x_encode_string(&res_buf, "shm_max_namespaces");
    ei_x_encode_ulong(&res_buf, config->shm_max_namespaces);
    ei_x_encode_string(&res_buf, "shm_takeover_threshold_sec");
    ei_x_encode_ulong(&res_buf, config->shm_takeover_threshold_sec);
// --------------------
    ei_x_encode_string(&res_buf, "policy_info");
    ei_x_encode_map_header(&res_buf, 3);
    ei_x_encode_string(&res_buf, "timeout");
    ei_x_encode_ulong(&res_buf, config->policies.info.timeout);
    ei_x_encode_string(&res_buf, "send_as_is");
    ei_x_encode_boolean(&res_buf, config->policies.info.send_as_is);
    ei_x_encode_string(&res_buf, "check_bounds");
    ei_x_encode_boolean(&res_buf, config->policies.info.check_bounds);


    POST
}

int call_cluster_info(const char *buf, int *index, int arity, int fd_out){
    PRE
    CHECK_ALL
    OK0

    as_cluster *cluster = as.cluster;

    ei_x_encode_map_header(&res_buf, 6);
    ei_x_encode_string(&res_buf, "cluster_name");
    ei_x_encode_string(&res_buf, cluster->cluster_name == NULL ? "" : cluster->cluster_name);
    ei_x_encode_string(&res_buf, "user");
    ei_x_encode_string(&res_buf, cluster->user == NULL ? "" : cluster->user);
    ei_x_encode_string(&res_buf, "password");
    ei_x_encode_string(&res_buf, cluster->password == NULL ? "" : cluster->password);
    ei_x_encode_string(&res_buf, "nodes_number");
    ei_x_encode_long(&res_buf, cluster->nodes == NULL ? 0 : cluster->nodes->size);
    ei_x_encode_string(&res_buf, "random_node_index");
    ei_x_encode_long(&res_buf, cluster->node_index);
    ei_x_encode_string(&res_buf, "n_partitions");
    ei_x_encode_long(&res_buf, cluster->n_partitions);

    POST
}

int call_port_status(const char *buf, int *index, int arity, int fd_out) {
    PRE
    OK0

    ei_x_encode_map_header(&res_buf, 2);
    ei_x_encode_string(&res_buf, "is_aerospike_initialised");
    ei_x_encode_boolean(&res_buf, is_aerospike_initialised);
    ei_x_encode_string(&res_buf, "is_connected");
    ei_x_encode_boolean(&res_buf, is_connected);

    POST
}

int call_aerospike_init(const char *buf, int *index, int arity, int fd_out) {
    PRE
    if (!is_aerospike_initialised) {
        as_config config;
        as_config_init(&config);
        aerospike_init(&as, &config);
        is_aerospike_initialised = 1;
    } 
    OK("initialised")
    // end:
    POST
}

int call_config_add_hosts(const char *buf, int *index, int arity, int fd_out) {
    PRE
    char host[MAX_HOST_SIZE];
    long port;

    if (ei_decode_string(buf, index, host) != 0) {
        ERROR("invalid first argument: host")
        goto end;
    } 
    if (ei_decode_long(buf, index, &port) != 0) {
        ERROR("invalid second argument: port")
        goto end;
    }
    CHECK_INIT

	if (! as_config_add_hosts(&as.config, host, port)) {
        ERROR("failed to add host or port")
		// as_event_close_loops(); ???
        goto end;
	}

    OK("host and port added")

    end:
    POST
}

int call_config_clear_hosts(const char *buf, int *index, int arity, int fd_out) {
    PRE
    CHECK_INIT

    as_config_clear_hosts(&as.config);

    OK("hosts list was cleared")

    POST
}

int call_config_list_hosts(const char *buf, int *index, int arity, int fd_out) {
    PRE
    CHECK_INIT

    as_config  *config = &as.config;   
    as_vector  *hosts = config->hosts;

    uint32_t size = (hosts == NULL) ? 0 : hosts->size;

    OK0
    ei_x_encode_list_header(&res_buf, size);
    for (uint32_t i = 0; i < size; i++) {
        as_host* host = (as_host *)as_vector_get(hosts, i);
        if (host->name) {
            ei_x_encode_tuple_header(&res_buf, 3);
            ei_x_encode_string(&res_buf, host->name);
            ei_x_encode_string(&res_buf, host->tls_name == NULL ? "" : host->tls_name);
            ei_x_encode_ulong(&res_buf, host->port);
        }
    }
    ei_x_encode_empty_list(&res_buf);

    POST
}


int call_connect(const char *buf, int *index, int arity, int fd_out) {
    PRE
    char user[AS_USER_SIZE];
    char password[AS_PASSWORD_SIZE];

    if (ei_decode_string(buf, index, user) != 0) {
        ERROR("invalid first argument: user name")
        goto end;
    } 
    if (ei_decode_string(buf, index, password) != 0) {
        ERROR("invalid seconf argument: password")
        goto end;
    } 

    CHECK_INIT

    as_config_set_user(&as.config, user, password);
	as_error err;
 
	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		// as_event_close_loops();
        ERROR(err.message)
        is_connected = 0;
        goto end;
	}

    is_connected = 1;
    OK("connected")

    end:
    POST
}

void logfile(std::string str){
    std::fstream file;
    file.open("/tmp/aspikeport", std::ios::out | std::ios::app);
    file << str << "\n";
    file.close();
}

int decode_bin_term(const char *buf, int *index, std::string& ds){
    int term_size;
    int term_type;
    long len;
     
    if (ei_get_type(buf, index, &term_type, &term_size) < 0 || term_type != ERL_BINARY_EXT){
	return -2;
    }

    char ns[term_size+1];
    if (ei_decode_binary(buf, index, ns, &len) < 0) {
	return -2;
    }
    ns[len] = '\0';
    ds = std::string(ns);
    
    return 0;
}

int call_binary_key_put(const char *buf, int *index, int arity, int fd_out) {
    PRE
    long len;
    int term_size;
    int term_type;
    int bin_list_length;


    if (ei_get_type(buf, index, &term_type, &term_size) < 0 || term_type != ERL_BINARY_EXT)
    	{STOPERROR("BKP invalid namespace bytestring size")}
    char ns[term_size + 1];
    if (ei_decode_binary(buf, index, ns, &len) < 0) 
        {STOPERROR("BKP invalid first argument: namespace")}
    ns[len] = '\0'; 
    
    if (ei_get_type(buf, index, &term_type, &term_size) < 0 || term_type != ERL_BINARY_EXT)
        {STOPERROR("BKP invalid set bytestring size")}
    char set[term_size + 1];
    if (ei_decode_binary(buf, index, set, &len) < 0)
        {STOPERROR("BKP invalid second argument: namespace")}
    set[len] = '\0'; 


    if (ei_get_type(buf, index, &term_type, &term_size) < 0 || term_type != ERL_BINARY_EXT)
        {STOPERROR("BKP invalid key bytestring size")}
    char key[term_size + 1];
    if (ei_decode_binary(buf, index, key, &len) < 0)
        {STOPERROR("BKP invalid second argument: namespace")}
    key[len] = '\0'; 
    
    if (ei_decode_list_header(buf, index, &bin_list_length) < 0)
        {STOPERROR("invalid fourth argument: list")}

    CHECK_ALL

    as_key askey;
    as_key_init_str(&askey, ns, set, key);
    as_record rec;
    as_record_inita(&rec, bin_list_length);

    int t_length;
    std::string bin_name, bin_str_value;
    long bin_int_value;
    std::vector<as_bytes*> bin_vec;
    int ret_val = 0; 
    for (int i = 0; i < bin_list_length; i++) {
        if (ei_decode_tuple_header(buf, index, &t_length) != 0 || t_length != 2)
            {STOPERROR("invalid tuple")}
        if (decode_bin_term(buf, index, bin_name) < 0 )
            {STOPERROR("invalid bin_name")}
	if (ei_get_type(buf, index, &term_type, &term_size) < 0)
	     {STOPERROR("BKP invalid bin value type (should be binary or integer)")}

        if((term_type == ERL_BINARY_EXT) && (decode_bin_term(buf, index, bin_str_value) == 0)){
	    bin_vec.push_back(as_bytes_new(bin_str_value.size()));
	    as_bytes * bytes_v = bin_vec.back();
	    as_bytes_set(bytes_v, 0, (const uint8_t *)bin_str_value.c_str(), bin_str_value.size());
	    if(!as_record_set_bytes(&rec, bin_name.c_str(), bytes_v)){
		as_bytes_destroy(bytes_v);
		ret_val = 1;
	    }
 	} else if((term_type == ERL_SMALL_INTEGER_EXT) || (term_type == ERL_INTEGER_EXT)){
           if(ei_decode_long(buf, index, &bin_int_value) == 0){
    	        as_record_set_int64(&rec, bin_name.c_str(), bin_int_value);
           }
        } else if(term_type == ERL_LIST_EXT){ // expecting list of integers
           int bin_intlist_length;
    	   if(ei_decode_list_header(buf, index, &bin_intlist_length) < 0)
        	{STOPERROR("invalid list of ints")}
           as_arraylist* as_list_ofints = as_arraylist_new((uint32_t)bin_intlist_length, 0);
           for (int j = 0; j < bin_intlist_length; j++) {
	       long i64 = 0;
               if(ei_decode_long(buf, index, &i64) == 0){
                   as_arraylist_append_int64(as_list_ofints, i64);
	       }
           }
    	   ei_decode_list_header(buf, index, &bin_intlist_length); // read end of list
	   ((as_val *)as_list_ofints)->type = AS_LIST;
           if(!as_record_set_list(&rec, bin_name.c_str(), (as_list*)as_list_ofints)){
            	as_list_destroy((as_list*)as_list_ofints);
           }
        } else if(term_type == ERL_STRING_EXT) { // expecting list of integers
    	   char listints[term_size]; 
	   if(ei_decode_string(buf, index, listints) == 0){
		   as_arraylist* as_list_ofints = as_arraylist_new((uint32_t)term_size, 0);
		   for (int j = 0; j < term_size; j++) {
		       as_arraylist_append_int64(as_list_ofints, (long)(listints[j]));
		   }
		   ((as_val *)as_list_ofints)->type = AS_LIST;
		   if(!as_record_set_list(&rec, bin_name.c_str(), (as_list*)as_list_ofints)){
			as_list_destroy((as_list*)as_list_ofints);
		   }
	   }
        } else {
	   logfile("unknown type: " + std::to_string(term_type));
	}
     
    }
    ei_decode_list_header(buf, index, &bin_list_length); // decode end of list

    long rec_ttl;
    if(ei_decode_long(buf, index, &rec_ttl) < 0)
	{STOPERROR("Invalid ttl value")}
    rec.ttl = rec_ttl;


    as_error err;
    // Write the record to the database.
    if (aerospike_key_put(&as, &err, NULL, &askey, &rec) != AEROSPIKE_OK) {
    	STOPERROR(err.message)
    }else{

    }

    for(unsigned int i = 0; i < bin_vec.size(); i++){
	as_bytes_destroy(bin_vec[i]);
    }

    OK("binary_key_put")

    POST
}

int call_key_put(const char *buf, int *index, int arity, int fd_out) {
    PRE

    char vnamesapce[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    int length;

    if (ei_decode_string(buf, index, vnamesapce) != 0) {
        ERROR("invalid first argument: namespace")
        goto end;
    } 
    if (ei_decode_string(buf, index, set) != 0) {
        ERROR("invalid second argument: set")
        
     goto end;
    } 
    if (ei_decode_string(buf, index, key_str) != 0) {
        ERROR("invalid third argument: key_str")
        goto end;
    } 
    if (ei_decode_list_header(buf, index, &length) != 0) {
        ERROR("invalid fourth argument: list")
        goto end;
    }

    CHECK_ALL

    as_key key;
	as_key_init_str(&key, vnamesapce, set, key_str);

	as_record rec;
	as_record_inita(&rec, length);

    int t_length;
    char bin[AS_BIN_NAME_MAX_SIZE];
    long val;

    for (int i = 0; i < length; i++) {
        if (ei_decode_tuple_header(buf, index, &t_length) != 0 || t_length != 2) {
            ERROR("invalid tuple")
            goto end;
        } 
        if (ei_decode_string(buf, index, bin) != 0) {
            ERROR("invalid bin")
            goto end;
        }
        if (ei_decode_long(buf, index, &val) != 0) {
            ERROR("invalid val")
            goto end;
        }
    	as_record_set_int64(&rec, bin, val);
    }
    
    if (length > 0) {
        as_error err;
        // Write the record to the database.
        if (aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
            ERROR(err.message)
            goto end;
        }
    }
    OK("key_put")

    end:
    POST
}


int call_key_inc(const char *buf, int *index, int arity, int fd_out) {
    PRE

    char vnamesapce[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    int length;

    if (ei_decode_string(buf, index, vnamesapce) != 0) {
        ERROR("invalid first argument: namespace")
        goto end;
    } 
    if (ei_decode_string(buf, index, set) != 0) {
        ERROR("invalid second argument: set")
        goto end;
    } 
    if (ei_decode_string(buf, index, key_str) != 0) {
        ERROR("invalid third argument: key_str")
        goto end;
    } 
    if (ei_decode_list_header(buf, index, &length) != 0) {
        ERROR("invalid fourth argument: list")
        goto end;
    }

    CHECK_ALL

    as_key key;
	as_key_init_str(&key, vnamesapce, set, key_str);

    as_operations ops;
	as_operations_inita(&ops, length);

    int t_length;
    char bin[AS_BIN_NAME_MAX_SIZE];
    long val;

    for (int i = 0; i < length; i++) {
        if (ei_decode_tuple_header(buf, index, &t_length) != 0 || t_length != 2) {
            ERROR("invalid tuple")
            goto end;
        } 
        if (ei_decode_string(buf, index, bin) != 0) {
            ERROR("invalid bin")
            goto end;
        }
        if (ei_decode_long(buf, index, &val) != 0) {
            ERROR("invalid val")
            goto end;
        }
        as_operations_add_incr(&ops, bin, val);
    }
    
    if (length > 0) {
        as_error err;
        // Write the record to the database.
        if (aerospike_key_operate(&as, &err, NULL, &key, &ops, NULL)  != AEROSPIKE_OK) {
            ERROR(err.message)
            goto end;
        }
    }
    OK("key_put")

    end:
    POST
}

int call_key_remove(const char *buf, int *index, int arity, int fd_out) {
    PRE

    char vnamesapce[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];

    if (ei_decode_string(buf, index, vnamesapce) != 0) {
        ERROR("invalid first argument: namespace")
        goto end;
    } 
    if (ei_decode_string(buf, index, set) != 0) {
        ERROR("invalid second argument: set")
        goto end;
    } 
    if (ei_decode_string(buf, index, key_str) != 0) {
        ERROR("invalid third argument: key_str")
        goto end;
    } 

    CHECK_ALL

    as_key key;
	as_key_init_str(&key, vnamesapce, set, key_str);

	as_error err;
    
    // Write the record to the database.
	if (aerospike_key_remove(&as, &err, NULL, &key) != AEROSPIKE_OK) {
        ERROR(err.message)
        goto end;
	}

    OK("key_remove")

    end:
    POST
}


int call_key_exists(const char *buf, int *index, int arity, int fd_out) {
    PRE

    char vnamesapce[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    as_record* p_rec = NULL;

    if (ei_decode_string(buf, index, vnamesapce) != 0) {
        ERROR("invalid first argument: namespace")
        POST
    } 
    if (ei_decode_string(buf, index, set) != 0) {
        ERROR("invalid second argument: set")
        POST
    } 
    if (ei_decode_string(buf, index, key_str) != 0) {
        ERROR("invalid third argument: key_str")
        POST
    } 

    CHECK_ALL

    as_key key;
	as_key_init_str(&key, vnamesapce, set, key_str);
	as_error err;
    
    int as_rc = aerospike_key_exists(&as, &err, NULL, &key, &p_rec);

	if (as_rc != AEROSPIKE_OK) {
        ERROR(err.message)
        POST
	}

    OK("true")

    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }

    POST
}

static void dump_bin(ei_x_buff *p_res_buf, const as_bin* p_bin) {
    char* val_as_str = NULL;
    ei_x_encode_tuple_header(p_res_buf, 2);
    ei_x_encode_string(p_res_buf, as_bin_get_name(p_bin));
    switch (as_bin_get_type(p_bin)){
        case AS_INTEGER:
            ei_x_encode_long(p_res_buf, as_bin_get_value(p_bin)->integer.value);
            break;
        default:
            val_as_str = as_val_tostring(as_bin_get_value(p_bin));
            ei_x_encode_string(p_res_buf, val_as_str);
            free(val_as_str);
    }
}

static bool list_to_intlist_each(as_val *val, void *udata){
    if (!val) {
        return false;
    }

    conversion_data *convd = (conversion_data *)udata;
    ei_x_encode_long(convd->env,  as_integer_get((as_integer*)val) );

    convd->count++;
    return true;
}

static void dump_binary_bin(ei_x_buff *p_res_buf, const as_bin* p_bin) {
    char* val_as_str = NULL;
    ei_x_encode_tuple_header(p_res_buf, 2);
    char* name = as_bin_get_name(p_bin);
    auto namelen = strlen(name);
    ei_x_encode_binary(p_res_buf, name, namelen);
    switch (as_bin_get_type(p_bin)){
        case AS_INTEGER:
            ei_x_encode_long(p_res_buf, as_bin_get_value(p_bin)->integer.value);
            break;
        case AS_STRING:
        case AS_BYTES: {
            as_bytes asbval = as_bin_get_value(p_bin)->bytes;
            uint8_t * bin_as_str = as_bytes_get(&asbval);
            auto len = asbval.size;
    	    ei_x_encode_binary(p_res_buf, bin_as_str, len);
        }break;
        case AS_LIST: {
            as_bin_value *val = as_bin_get_value(p_bin);
	    as_list *int_list = &val->list;
            auto len = as_list_size(int_list);
    	    ei_x_encode_list_header(p_res_buf, len);
	    conversion_data convd = {
            	.env = p_res_buf, .count = 0};
            as_list_foreach((as_list *)(&val->list), list_to_intlist_each, &convd);
    	    ei_x_encode_empty_list(p_res_buf);
        }break;
        default:
            val_as_str = as_val_tostring(as_bin_get_value(p_bin));
            ei_x_encode_string(p_res_buf, val_as_str);
            free(val_as_str);
    }
}

static int binary_dump_records(ei_x_buff *p_res_buf, const as_record *p_rec) {
    int res = 1;
    if (p_rec == NULL) {
        ERRORP("NULL p_rec - internal error")
        return res;
    }
    if (p_rec->key.valuep) {
	char* key_val_as_str = as_val_tostring(p_rec->key.valuep);
        OKP(key_val_as_str)
	free(key_val_as_str);
        return res;
    }

    as_record_iterator it;
    as_record_iterator_init(&it, p_rec);
    OK0P
    
    uint16_t num_bins = as_record_numbins(p_rec);
    ei_x_encode_list_header(p_res_buf, num_bins);
    while (as_record_iterator_has_next(&it)) {
    	dump_binary_bin(p_res_buf, as_record_iterator_next(&it));
    }
    ei_x_encode_empty_list(p_res_buf);
    as_record_iterator_destroy(&it);

    return res;
}

static int dump_records(ei_x_buff *p_res_buf, const as_record *p_rec) {
    int res = 1;
    if (p_rec == NULL) {
        ERRORP("NULL p_rec - internal error")
        return res;
    }
	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);
        OKP(key_val_as_str)
		free(key_val_as_str);
        return res;
	}

	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);

    OK0P
    
	uint16_t num_bins = as_record_numbins(p_rec);
    ei_x_encode_list_header(p_res_buf, num_bins);

	while (as_record_iterator_has_next(&it)) {
		dump_bin(p_res_buf, as_record_iterator_next(&it));
	}

    ei_x_encode_empty_list(p_res_buf);
	as_record_iterator_destroy(&it);

    return res;
}

int call_binary_key_get(const char *buf, int *index, int arity, int fd_out) {
    PRE

    long len;
    int term_size;
    int term_type;
    as_record* p_rec = NULL;    
    logfile("CBKG1");


    if (ei_get_type(buf, index, &term_type, &term_size) < 0 || term_type != ERL_BINARY_EXT)
    	{STOPERROR("BKG invalid namespace bytestring size")}
    char ns[term_size + 1];
    if (ei_decode_binary(buf, index, ns, &len) < 0) 
        {STOPERROR("BKG invalid first argument: namespace")}
    ns[len] = '\0'; 
    logfile("CBKG2");
    
    if (ei_get_type(buf, index, &term_type, &term_size) < 0 || term_type != ERL_BINARY_EXT)
        {STOPERROR("BKG invalid set bytestring size")}
    char set[term_size + 1];
    if (ei_decode_binary(buf, index, set, &len) < 0)
        {STOPERROR("BKG invalid second argument: namespace")}
    set[len] = '\0'; 
    logfile("CBKG3");


    if (ei_get_type(buf, index, &term_type, &term_size) < 0 || term_type != ERL_BINARY_EXT)
        {STOPERROR("BKG invalid key bytestring size")}
    char key[term_size + 1];
    if (ei_decode_binary(buf, index, key, &len) < 0)
        {STOPERROR("BKG invalid second argument: namespace")}
    key[len] = '\0'; 
    logfile("CBKG4");

    CHECK_ALL

    as_key askey;
    as_key_init_str(&askey, ns, set, key);
    as_error err;
    
    logfile("CBKG5");
    // Get the record from the database.
    if (aerospike_key_get(&as, &err, NULL, &askey, &p_rec) != AEROSPIKE_OK) {
       STOPERROR(err.message)
       logfile("CBKG6");
    }

    logfile("CBKG7");
    res = binary_dump_records(&res_buf, p_rec);

    logfile("CBKG8");
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }

    logfile("CBKG9");
    POST
}

int call_key_get(const char *buf, int *index, int arity, int fd_out) {
    PRE

    char vnamesapce[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    as_record* p_rec = NULL;    

    if (ei_decode_string(buf, index, vnamesapce) != 0) {
        ERROR("invalid first argument: namespace")
        goto end;
    } 
    if (ei_decode_string(buf, index, set) != 0) {
        ERROR("invalid second argument: set")
        goto end;
    } 
    if (ei_decode_string(buf, index, key_str) != 0) {
        ERROR("invalid third argument: key_str")
        goto end;
    } 

    CHECK_ALL

    as_key key;
	as_key_init_str(&key, vnamesapce, set, key_str);
	as_error err;

    // Get the record from the database.
	if (aerospike_key_get(&as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
        ERROR(err.message)
        goto end;
	}

    res = dump_records(&res_buf, p_rec);

    end:
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }

    POST
}
int call_key_select(const char *buf, int *index, int arity, int fd_out) {
    PRE

    char vnamesapce[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    as_record* p_rec = NULL;    
    int length;
    const char *bins[MAX_BINS_NUMBER];
    int i = 0;

    if (ei_decode_string(buf, index, vnamesapce) != 0) {
        ERROR("invalid first argument: namespace")
        goto end;
    } 
    if (ei_decode_string(buf, index, set) != 0) {
        ERROR("invalid second argument: set")
        goto end;
    } 
    if (ei_decode_string(buf, index, key_str) != 0) {
        ERROR("invalid third argument: key_str")
        goto end;
    } 
    if (ei_decode_list_header(buf, index, &length) != 0) {
        ERROR("invalid fourth argument: list")
        goto end;
    }

    CHECK_ALL
    if (length == 0) {
        OK0
        res = ei_x_encode_empty_list(&res_buf);
        goto end;
    }

    for (; i < length; i++) {
        char bin[AS_BIN_NAME_MAX_SIZE] = {0};
        if (ei_decode_string(buf, index, bin) != 0) {
            ERROR("invalid bin")
            goto end;
        }
        bins[i] = strdup(bin);
    }
    bins[i] = NULL;

    as_key key;
	as_key_init_str(&key, vnamesapce, set, key_str);
	as_error err;

    if (aerospike_key_select(&as, &err, NULL, &key, bins, &p_rec)  != AEROSPIKE_OK) {
        ERROR(err.message)
        goto end;
    }

    res = dump_records(&res_buf, p_rec);

    end:
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }
    for (int j = 0; j < i; j++) {
        free((void *)bins[j]);
    }

    POST
}

int call_key_generation(const char *buf, int *index, int arity, int fd_out) {
    PRE

    char vnamesapce[MAX_NAMESPACE_SIZE];
    char set[MAX_SET_SIZE];
    char key_str[MAX_KEY_STR_SIZE];
    as_record* p_rec = NULL;    

    if (ei_decode_string(buf, index, vnamesapce) != 0) {
        ERROR("invalid first argument: namespace")
        goto end;
    } 
    if (ei_decode_string(buf, index, set) != 0) {
        ERROR("invalid second argument: set")
        goto end;
    } 
    if (ei_decode_string(buf, index, key_str) != 0) {
        ERROR("invalid third argument: key_str")
        goto end;
    } 

    CHECK_ALL

    as_key key;
	as_key_init_str(&key, vnamesapce, set, key_str);
	as_error err;

    // Get the record from the database.
	if (aerospike_key_get(&as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
        ERROR(err.message)
        goto end;
	}

    OK0
    ei_x_encode_map_header(&res_buf, 2);
    ei_x_encode_string(&res_buf, "gen");
    ei_x_encode_ulong(&res_buf, p_rec->gen);
    ei_x_encode_string(&res_buf, "ttl");
    ei_x_encode_ulong(&res_buf, p_rec->ttl);

    end:
    if (p_rec != NULL) {
        as_record_destroy(p_rec);
    }

    POST
}

int call_node_random(const char *buf, int *index, int arity, int fd_out) {
    PRE
    CHECK_ALL
    as_node* node = as_node_get_random(as.cluster);
    if (! node) {
        ERROR("Failed to find server node.");
        goto end;
	}
    OK(as_node_get_address_string(node))
    as_node_release(node);

    end:
    POST
}

int call_node_names(const char *buf, int *index, int arity, int fd_out) {
    PRE
    CHECK_ALL
	as_nodes* nodes = as_nodes_reserve(as.cluster);
    uint32_t n_nodes = nodes->size;

    OK0
    ei_x_encode_list_header(&res_buf, n_nodes);
    for(unsigned int i = 0; i < n_nodes; i++){
        as_node* node = nodes->array[i];
        ei_x_encode_tuple_header(&res_buf, 2);
        ei_x_encode_string(&res_buf, node->name);
        ei_x_encode_string(&res_buf, as_node_get_address_string(node));
    }
    ei_x_encode_empty_list(&res_buf);
    as_nodes_release(nodes);
    POST
}

int call_node_get(const char *buf, int *index, int arity, int fd_out) {
    PRE
    char node_name[AS_NODE_NAME_MAX_SIZE];
    if (ei_decode_string(buf, index, node_name) != 0) {
        ERROR("invalid first argument: node_name")
        POST
    }
    CHECK_ALL

    as_node* node = as_node_get_by_name(as.cluster, node_name);
    if (! node) {
        ERROR("Failed to find server node.");
        POST
	}
    OK(as_node_get_address_string(node))
    as_node_release(node);

    POST
}

int call_node_info(const char *buf, int *index, int arity, int fd_out) {
    PRE
    char node_name[AS_NODE_NAME_MAX_SIZE];
    char item[1024];
    as_node* node = NULL;

    if (ei_decode_string(buf, index, node_name) != 0) {
        ERROR("invalid first argument: node_name")
        POST
    }
    if (ei_decode_string(buf, index, item) != 0) {
        ERROR("invalid second argument: item")
        POST
    }
    CHECK_ALL

	as_cluster* cluster = as.cluster;
    node = as_node_get_by_name(cluster, node_name);
    if (! node) {
        ERROR("Failed to find server node.");
        POST
	}

    char *info = NULL;
    as_error err;
    const as_policy_info* policy = &as.config.policies.info;
	uint64_t deadline = as_socket_deadline(policy->timeout);
    as_status status = AEROSPIKE_ERR_CLUSTER;

    status = as_info_command_node(&err, node, (char*)item, policy->send_as_is, deadline, &info);
    if (status != AEROSPIKE_OK) {
        ERROR(err.in_doubt == true ? "unknown error" : err.message)
        POST
    }

    if (info == NULL) {
        ERROR("no data")
        POST
    }

    OK0
    ei_x_encode_list_header(&res_buf, 1);
    ei_x_encode_string(&res_buf, &info[0]);
    ei_x_encode_empty_list(&res_buf);
    free(info);

    as_node_release(node);
    POST
}

int call_host_info(const char *buf, int *index, int arity, int fd_out) {
    PRE
    char hostname[AS_NODE_NAME_MAX_SIZE];
    char item[1024];
    long port;
    if (ei_decode_string(buf, index, hostname) != 0) {
        ERROR("invalid first argument: hostname")
        POST
    }
    if (ei_decode_long(buf, index, &port) != 0) {
        ERROR("invalid second argument: port")
        POST
    }
    if (ei_decode_string(buf, index, item) != 0) {
        ERROR("invalid third argument: item")
        POST
    }
    CHECK_ALL

    as_error err;
    as_address_iterator iter;

	as_status status = as_lookup_host(&iter, &err, hostname, port);
	
	if (status) {
        ERROR(err.in_doubt == true ? "unknown error" : err.message)
        POST
	}
    
	as_cluster* cluster = as.cluster;
    char *info = NULL;
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

    if(info == NULL){
        ERROR("no data")
        POST
    }

    OK0
    ei_x_encode_list_header(&res_buf, 1);
    ei_x_encode_string(&res_buf, &info[0]);
    ei_x_encode_empty_list(&res_buf);
    free(&info[0]);


    POST
}

int call_help(const char *buf, int *index, int arity, int fd_out) {
    PRE
    char item[1024];
    if (ei_decode_string(buf, index, item) != 0) {
        ERROR("invalid first argument: item")
        POST
    } 

    CHECK_ALL  

	as_error err;
	as_status rc = AEROSPIKE_OK;
    char * info = NULL;

    rc = aerospike_info_any(&as, &err, NULL, item, &info);
	if (rc != AEROSPIKE_OK) {
        ERROR(err.in_doubt == true ? "unknown error" : err.message)
        POST
	}

    if(info == NULL){
        ERROR("no data")
        POST
    }

    OK(info)
    free(info);

    POST
}

