# aspike-port
to start aspike client run these commands in erlang shell

```erlang
application:set_env(aspike_port, host, "Aerospike-discovery-node-ip").
application:set_env(aspike_port, psw, "Aerospike-password").
application:set_env(aspike_port, port, 3000).
application:set_env(aspike_port, user, "Aerospike-username").
aspike_nif:as_init().
aspike_nif:host_add().
aspike_nif:connect().
```
to run single writing process: 
```erlang
aspike_nif_perf:sp_insert(<<"global-store">>, <<"rtb-gateway-fcap-users">>, 1_000_000, 3600, 0, 1_000_000_000_000, 0, 0).
```
it will insert 1_000_000 keys to namespace=global-store, set=rtb-gateway-fcap-users. Data TTL=3600 seconds. Delay between operations = 0ms. Initial key value = 1_000_000_000_000. (key will be 1_000_001_000_000 ... 1_000_001_999_999).
Function will return: {Number_of_success_operations, Number_of_errors}.

to run single read process:
```erlang
aspike_nif_perf:sp_read(<<"global-store">>, <<"rtb-gateway-fcap-users">>, 1_000_000, 0, 1_000_000_000_000, 0, 0, 0).
```
it will read from namespace=global-store, set=rtb-gateway-fcap-users. Delay between operations = 0ms. Initial key value = 1_000_000_000_000. (key will be 1_000_001_000_000 ... 1_000_001_999_999).

To get raw stats:
```erlang
aspike_nif_perf:dump_stats().
```
it will produce 2 files: /tmp/read_stats.txt and /tmp/insert_stats.txt
