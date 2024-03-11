-module(aspike_nif_perf).

-export([
   sp_insert/8,
   sp_insert/5,
   sp_insert/2,
   sp_insert/1,
   mp_insert/3,
   sp_read/2,
   sp_read/8,
   mp_reads/3,

   rand_read/2,
   rand_read/8,
   mp_rand_reads/3,
   infinite_test/4
]).

% single process insert
sp_insert(N) ->
   sp_insert(<<"rtb-gateway">>, <<"nif_perf_set">>, N, 3600, 10).

sp_insert(N, Sleep) ->
   T1 = erlang:system_time(microsecond),
   sp_insert(<<"rtb-gateway">>, <<"nif_perf_set">>, N, 3600, Sleep),
   T = erlang:system_time(microsecond) - T1,
   Avg = (T - (Sleep * 1000)*N)/N,
   {T, Avg}.

sp_insert(Namespace, Set, N, TTL, Sleep) ->
	sp_insert(Namespace, Set, N, TTL, Sleep, 1_000_000_000_000, 0, 0).

sp_insert(_, _, 0, _, _, _, Oks, Errs) -> {Oks, Errs};
sp_insert(Namespace, Set, N, TTL, Sleep, AddP, Oks, Errs) ->
   case N rem 10000 of
     0 -> io:format("write N: ~p ~n", [N]);
     _ -> ok
   end,
   Key = integer_to_binary(N + AddP),
   Bins = [
      {<<"column1">>, <<"fcap">>},
      {<<"column2">>, <<"campaign.164206.3684975">>},
      {<<"timestamps">>, <<0,0,0,0,0,0,0,2,0,0,0,0,101,231,111,33,0,0,0,0,101,231,64,10>>}
   ],
   {O1, E1} = case aspike_nif:binary_put(Namespace, Set, Key, Bins, TTL) of
     {ok, _} -> {Oks+1, Errs};
     _ -> {Oks, Errs+1}
   end, 

   case Sleep of
     0 -> ok;
     _ -> timer:sleep(rand:uniform(Sleep))
   end,
   sp_insert(Namespace, Set, N-1, TTL, Sleep, AddP, O1, E1).

mp_insert(NProc, N, Sleep) ->
   lists:map(fun(E) -> 
     spawn(fun() ->
	Ret = sp_insert(<<"rtb-gateway">>, <<"nif_perf_set">>, N, 3600, Sleep, 1_000_000_000_000 * E, 0, 0),
	io:format("Insert Process ~p ret: ~p ~n", [E, Ret])
     end)
   end, lists:seq(1, NProc)).

mp_reads(NProc, N, Sleep) ->
   lists:map(fun(E) -> 
     spawn(fun() ->
	Ret = sp_read(<<"rtb-gateway">>, <<"nif_perf_set">>, N, Sleep, 1_000_000_000_000 * E, 0, 0, 0),
	io:format("Read Process ~p ret: ~p ~n", [E, Ret])
     end)
   end, lists:seq(1, NProc)).

sp_read(N, Sleep) ->
   T1 = erlang:system_time(microsecond),
   Ret = sp_read(<<"rtb-gateway">>, <<"nif_perf_set">>, N, Sleep, 1_000_000_000_000, 0, 0, 0),
   T = erlang:system_time(microsecond) - T1,
   Avg = (T - (Sleep * 1000)*N)/N,
   {Ret, {T, Avg}}.
sp_read(_, _, 0, _, _, Oks, Nfs, Errs) -> {Oks, Nfs, Errs};
sp_read(Namespace, Set, N, Sleep, AddP, Oks, Nfs, Errs) ->
   case N rem 10000 of
     0 -> io:format("read N: ~p ~n", [N]);
     _ -> ok
   end,
   Key = integer_to_binary(N + AddP),
   {Oks1, Nfs1, Errs1} = case aspike_nif:binary_get(Namespace, Set, Key) of
      {ok, Ret} ->
	  case check_ret(Ret) of
	     true -> {Oks+1, Nfs, Errs};
	     _ -> {Oks, Nfs, Errs+1}
          end; 
      {error, ERet} ->
	  case string:str(ERet, "AEROSPIKE_ERR_RECORD_NOT_FOUND") of
	    0 -> {Oks, Nfs, Errs+1};
            _ -> {Oks, Nfs+1, Errs}
          end
   end,
   case Sleep of
     0 -> ok;
     _ -> timer:sleep(rand:uniform(Sleep))
   end,
   sp_read(Namespace, Set, N-1, Sleep, AddP, Oks1, Nfs1, Errs1).


mp_rand_reads(NProc, N, Sleep) ->
   lists:map(fun(E) -> 
     spawn(fun() ->
	Ret = rand_read(<<"rtb-gateway">>, <<"nif_perf_set">>, N, Sleep, 1_000_000_000_000, 0, 0, 0),
	io:format("Process ~p ret: ~p ~n", [E, Ret])
     end)
   end, lists:seq(1, NProc)).

rand_read(N, Sleep) ->
   T1 = erlang:system_time(microsecond),
   Ret = sp_read(<<"rtb-gateway">>, <<"nif_perf_set">>, N, Sleep, 1_000_000_000_000, 0, 0, 0),
   T = erlang:system_time(microsecond) - T1,
   Avg = (T - (Sleep * 1000)*N)/N,
   {Ret, {T, Avg}}.
rand_read(_, _, 0, _, _, Oks, Nfs, Errs) -> {Oks, Nfs, Errs};
rand_read(Namespace, Set, N, Sleep, AddP, Oks, Nfs, Errs) ->
   case N rem 10000 of
     0 -> io:format("read N: ~p ~n", [N]);
     _ -> ok
   end,
   Rand = rand:uniform(1_000_000),
   Key = integer_to_binary(Rand + AddP),
   {Oks1, Nfs1, Errs1} = case aspike_nif:binary_get(Namespace, Set, Key) of
      {ok, Ret} ->
	  case check_ret(Ret) of
	     true -> {Oks+1, Nfs, Errs};
	     _ -> {Oks, Nfs, Errs+1}
          end; 
      {error, ERet} ->
	  case string:str(ERet, "AEROSPIKE_ERR_RECORD_NOT_FOUND") of
	    0 -> {Oks, Nfs, Errs+1};
            _ -> {Oks, Nfs+1, Errs}
          end
   end,
   case Sleep of
      SI when is_integer(SI), SI > 1 -> timer:sleep(rand:uniform(SI));
      _ -> ok
   end,
   rand_read(Namespace, Set, N-1, Sleep, AddP, Oks1, Nfs1, Errs1).

check_ret(Ret) ->
   Bins = [
      {<<"column1">>, <<"fcap">>},
      {<<"column2">>, <<"campaign.164206.3684975">>},
      {<<"timestamps">>, <<0,0,0,0,0,0,0,2,0,0,0,0,101,231,111,33,0,0,0,0,101,231,64,10>>}
   ],
   lists:all(fun(E) -> E =:= true end,  [proplists:get_value(K, Ret, undefined) == V || {K,V} <- Bins]).

infinite_test(NProc, Ttl, WSleep, _RSleep) ->
   lists:map(fun(E) -> 
     spawn(fun() ->
	Ret = sp_insert(<<"rtb-gateway">>, <<"nif_perf_set">>, 999_999_999_999, Ttl, WSleep, 1_000_000_000_000 * E, 0, 0),
	io:format("Insert Process ~p ret: ~p ~n", [E, Ret])
     end)
   end, lists:seq(1, NProc)).

