-module(aspike_tfuncs).

-export([
    init/1,
    test_insert/2
]).

init(Password) ->
    SeedIp = ?GET_ENV(aspike_seed_ip, "172.30.80.201"),
    SeedPort = ?GET_ENV(aspike_seed_port, 3000),
    User = ?GET_ENV(aspike_user, "gateway-service"),

    application:set_env(aspike_port, host, SeedIp),
    application:set_env(aspike_port, port, SeedPort),
    application:set_env(aspike_port, user, User),
    application:set_env(aspike_port, psw, Password),

    aspike_nif:as_init(),
    aspike_nif:host_add(),
    aspike_nif:connect().


test_insert(0, _) -> ok;
test_insert(N, TTL) ->
    Key = 
    Bins = [
        {<<"column1">>, base64:encode(crypto:strong_rand_bytes(20))},
        {<<"column2">>, base64:encode(crypto:strong_rand_bytes(10))},
        {<<"timestamps">>, base64:encode(crypto:strong_rand_bytes(24))}
    ],
    Key = base64:encode(crypto:strong_rand_bytes(30)),
    aspike_nif:binary_put(<<"rtb-gateway">>, <<"mkh_perf_test">>, Key, Bins, TTL),
    timer:sleep(10),
    test_insert(N - 1, TTL).
