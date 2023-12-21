-module(tsl).

-export([t/4, tst/4]).

tst(Mod, Fn, Args) ->
    {Time, _} = timer:tc(Mod, Fn, Args),
    Time.


tst(Mod, Fn, N, R) ->
    Args = Mod:mk_args(Fn, N),
    io:format("~p:~p, N=~p, R=~p, Time=~p(msec)~n",[Mod, Fn, N, R, lists:sum([tst(Mod, Fn, Args) || _ <- lists:seq(1, R)])/R]).

t(Mod, Fn, Args, R) when is_list(Mod)->
    [t(M, Fn, Args, R) || M <- Mod],
    ok;
t(Mod, Fn, Args, R) when is_list(Fn)->
    [t(Mod, F, Args, R) || F <- Fn],
    ok;
t(Mod, Fn, Args, R) ->
    spawn(fun() -> tst(Mod, Fn, Args, R) end),
    ok.
