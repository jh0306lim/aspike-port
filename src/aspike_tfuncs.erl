-module(aspike_tfuncs).

-export([
    init/1,
    test_insert/2
]).

init(Password) ->
    SeedIp = "172.30.80.201",
    SeedPort = 3000,
    User = "gateway-service",

    application:set_env(aspike_port, host, SeedIp),
    application:set_env(aspike_port, port, SeedPort),
    application:set_env(aspike_port, user, User),
    application:set_env(aspike_port, psw, Password),

    aspike_nif:as_init(),
    aspike_nif:host_add(),
    aspike_nif:connect().


test_insert(0, _) -> ok;
test_insert(N, TTL) ->
    %Bins = [
    %    {<<"column1">>, base64:encode(crypto:strong_rand_bytes(20))},
    %    {<<"column2">>, base64:encode(crypto:strong_rand_bytes(10))},
    %    {<<"timestamps">>, base64:encode(crypto:strong_rand_bytes(24))}
    %],
    %Key = base64:encode(crypto:strong_rand_bytes(30)),
    Key = <<"test">>,
    Bins = [
        {<<"column1">>, <<"column1">>},
        {<<"column2">>, <<"columb2">>},
        {<<"timestamps">>, <<"00000000000000000001">>}
    ],

    Ret = aspike_nif:binary_put(<<"rtb-gateway">>, <<"mkh_perf_test">>, Key, Bins, TTL), 
    io:format("Ret: ~p ~n", [Ret]),
    timer:sleep(10),
    test_insert(N - 1, TTL).
