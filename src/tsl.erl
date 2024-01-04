-module(tsl).

-export([t/4, t/5, ts/3, ts/4, tst/4, tst/5]).

-define(DEFAULT_UNIT, microsecond).
% second | millisecond | microsecond | nanosecond | native | perf_counter
ts(Mod, Fn, Args) ->
    ts(Mod, Fn, Args, ?DEFAULT_UNIT).

ts(Mod, Fn, Args, Unit) ->
    {Time, _} = timer:tc(Mod, Fn, Args, Unit),
    Time.

tst(Mod, Fn, N, R) ->
    tst(Mod, Fn, N, R, ?DEFAULT_UNIT).

tst(Mod, Fn, N, R, Unit) ->
    Args = Mod:mk_args(Fn, N),
    io:format("~p:~p, N=~p, R=~p, Time=~p(~p)~n", [
        Mod,
        Fn,
        N,
        R,
        lists:sum([ts(Mod, Fn, Args, Unit) || _ <- lists:seq(1, R)]) / R,
        Unit
    ]).

t(Mod, Fn, Args, R) -> t(Mod, Fn, Args, R, ?DEFAULT_UNIT).

t(Mod, Fn, Args, R, Unit) when is_list(Mod) ->
    [t(M, Fn, Args, R, Unit) || M <- Mod],
    ok;
t(Mod, Fn, Args, R, Unit) when is_list(Fn) ->
    [t(Mod, F, Args, R, Unit) || F <- Fn],
    ok;
t(Mod, Fn, Args, R, Unit) ->
    spawn(fun() -> tst(Mod, Fn, Args, R, Unit) end),
    ok.
