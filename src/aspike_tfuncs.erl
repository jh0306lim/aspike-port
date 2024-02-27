-module(aspike_tfuncs).

-export([
    init/1,
    test_insert/2,
    test_insert_f/2
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
    Bins = [
        {<<"column1">>, base64:encode(crypto:strong_rand_bytes(200))},
        {<<"column2">>, base64:encode(crypto:strong_rand_bytes(100))},
        {<<"timestamps">>, base64:encode(crypto:strong_rand_bytes(240))}
    ],
    Key = base64:encode(crypto:strong_rand_bytes(40)),
    Ret = aspike_nif:binary_put(<<"rtb-gateway">>, <<"mkh_perf_test">>, Key, Bins, TTL), 
    case N rem 1000 of
	0 -> io:format("~p \t Ret: ~p ~n", [N, Ret]);
	_ -> ok
    end,
    timer:sleep(10),
    test_insert(N - 1, TTL).

test_insert_f(0, _) -> ok;
test_insert_f(N, TTL) ->
    Bins = [
        {<<"column1">>, base64:encode(crypto:strong_rand_bytes(20))},
        {<<"column2">>, base64:encode(crypto:strong_rand_bytes(10))},
        {<<"timestamps">>, base64:encode(crypto:strong_rand_bytes(24))}
    ],
    Key = base64:encode(crypto:strong_rand_bytes(30)),
    Ret = anbp(<<"rtb-gateway">>, <<"mkh_perf_test">>, Key, Bins, TTL), 
    case N rem 1000 of
	0 -> io:format("~p \t Ret: ~p ~n", [N, Ret]);
	_ -> ok
    end,
    timer:sleep(10),
    test_insert_f(N - 1, TTL).

anbp(_Db, _Table, Key, Bins, _TTL) ->
    C1 = proplists:get_value(<<"column1">>, Bins),
    C2 = proplists:get_value(<<"column2">>, Bins),
    C3 = proplists:get_value(<<"timestamps">>, Bins),
    {ok, <<Key/binary, "<->", C1/binary, "<->", C2/binary, "<->", C3/binary>>}.
