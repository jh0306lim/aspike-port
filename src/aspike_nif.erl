% -------------------------------------------------------------------------------

-module(aspike_nif).

-include("../include/defines.hrl").

-export([
    as_init/0,
    host_add/0,
    host_add/2,
    connect/0,
    connect/2,
    key_put/0,
    key_put/2,
    key_put/5,
    key_remove/0,
    key_remove/1,
    key_remove/3,
    key_get/0,
    key_get/1,
    key_get/3,
    key_generation/0,
    key_generation/1,
    key_generation/3,
    mk_args/2,
    node_random/0,
    node_names/0,
    node_get/1,
    node_info/2,
    help/0,
    help/1,
    host_info/1,
    host_info/3,
    b/0,
    % --------------------------------------
    foo/1,
    bar/1
]).

-nifs([
    as_init/0,
    host_add/2,
    connect/2,
    key_put/5,
    key_remove/3,
    key_get/3,
    key_generation/3,
    node_random/0,
    node_names/0,
    node_get/1,
    nif_node_info/2,
    nif_help/1,
    nif_host_info/3,
    % --------------------------------------
    foo/1,
    bar/1
]).

% -------------------------------------------------------------------------------

-on_load(init/0).

-define(LIBNAME, ?MODULE).

% -------------------------------------------------------------------------------

init() ->
    erlang:load_nif(utils:find_lib(?LIBNAME), 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

as_init() ->
    not_loaded(?LINE).

-spec host_add() -> {ok, string()} | {error, string()}.
host_add() ->
    host_add(?DEFAULT_HOST, ?DEFAULT_PORT).

-spec host_add(string(), non_neg_integer()) -> {ok, string()} | {error, string()}.
host_add(_, _) ->
    not_loaded(?LINE).

connect() ->
    connect(?DEFAULT_USER, ?DEFAULT_PSW).

connect(_, _) ->
    not_loaded(?LINE).

key_put() ->
    key_put("erl-bin-nif", 999).

key_put(Bin, N) ->
    key_put(Bin, N, "test", "erl-set", "erl-key").

key_put(_, _, _, _, _) ->
    not_loaded(?LINE).

key_remove() ->
    key_remove("erl-key").

key_remove(Key) ->
    key_remove("test", "erl-set", Key).

key_remove(_, _, _) ->
    not_loaded(?LINE).

key_get() ->
    key_get("erl-key").

key_get(Key) ->
    key_get("test", "erl-set", Key).

key_get(_, _, _) ->
    not_loaded(?LINE).

key_generation() ->
    key_generation("erl-key").

key_generation(Key) ->
    key_generation("test", "erl-set", Key).

key_generation(_, _, _) ->
    not_loaded(?LINE).

node_random() ->
    not_loaded(?LINE).

node_names() ->
    not_loaded(?LINE).

node_get(_) ->
    not_loaded(?LINE).

-spec help() -> {ok, string()} | {error, term()}.
help() ->
    help("namespaces").

help(Item) ->
    as_render:info_render(nif_help(Item), Item).

nif_help(_) ->
    not_loaded(?LINE).

node_info(Node, Item) ->
    as_render:info_render(nif_node_info(Node, Item), Item).

nif_node_info(_, _) ->
    not_loaded(?LINE).

-spec host_info(string()) -> {ok, [string()]} | {error, string()}.
host_info(Item) ->
    host_info(?DEFAULT_HOST, ?DEFAULT_PORT, Item).

-spec host_info(string(), non_neg_integer(), string()) -> {ok, [string()]} | {error, term()}.
host_info(HostName, Port, Item) ->
    as_render:info_render(nif_host_info(HostName, Port, Item), Item).

nif_host_info(_, _, _) ->
    not_loaded(?LINE).

b() ->
    as_init(),
    host_add(),
    connect().

% @doc Used in ${tsl.erl} to create argument list for testin function
-spec mk_args(atom(), non_neg_integer()) -> [term()].
mk_args(_, _) -> [].

% -----------------------------------------------------------------
foo(_X) ->
    exit(nif_library_not_loaded).

bar(_Y) ->
    exit(nif_library_not_loaded).

% -----------------------------------------------------------------
% 2> tsl:tst(aspike_nif, key_put, 0, 10000).
% aspike_nif:key_put, N=0, R=10000, Time=580.5629
% 3> tsl:tst(aspike_nif, key_put, 0, 100000).
% aspike_nif:key_put, N=0, R=100000, Time=588.68821
% -----------------------------------------------------------------
