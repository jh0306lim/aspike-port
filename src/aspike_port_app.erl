%%%-------------------------------------------------------------------
%% @doc aspike-port public API
%% @end
%%%-------------------------------------------------------------------

-module(aspike_port_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    aspike_port_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
