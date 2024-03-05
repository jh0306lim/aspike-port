-module(aspike_nif_perf).

-export([
   sp_insert/8,
   sp_insert/5,
   sp_insert/2,
   sp_insert/1,
   mp_insert/3
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
   case N rem 1000 of
     0 -> io:format("N: ~p ~n", [N]);
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
     _ -> timer:sleep(Sleep)
   end,
   sp_insert(Namespace, Set, N-1, TTL, Sleep, AddP, O1, E1).

mp_insert(NProc, N, Sleep) ->
   lists:map(fun(E) -> 
     spawn(fun() ->
	Ret = sp_insert(<<"rtb-gateway">>, <<"nif_perf_set">>, N, 3600, Sleep, 1_000_000_000_000 * E, 0, 0),
	io:format("Process ~p ret: ~p ~n", [E, Ret]) 
     end)
   end, lists:seq(1, NProc)).
