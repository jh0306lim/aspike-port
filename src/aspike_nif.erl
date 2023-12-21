-module(aspike_nif).

-export([
    as_init/0,
    host_add/0,
    host_add/2,
    connect/0,
    connect/2,
    key_put/0,
    key_put/2,
    key_put/5,
    mk_args/2,
    b/0,
    % --------------------------------------
    foo/1, 
    bar/1]).

-nifs([
    as_init/0,
    host_add/2,
    connect/2,
    key_put/5,
    % --------------------------------------
    foo/1, 
    bar/1]).

-on_load(init/0).

-define(LIBNAME, ?MODULE).

init() ->
    erlang:load_nif(utils:find_lib(?LIBNAME), 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
    
as_init() ->
    not_loaded(?LINE).

-spec host_add() -> {ok, string()} | {error, string()}.
host_add() ->
    host_add("127.0.0.1", 3010).
-spec host_add(string(), non_neg_integer()) -> {ok, string()} | {error, string()}.
host_add(_, _) ->
    not_loaded(?LINE).

connect() ->
    connect("","").
connect(_, _) ->
    not_loaded(?LINE).

key_put() ->
    key_put("erl-bin-nif", 777).
key_put(Bin, N) ->
    key_put(Bin, N, "test", "erl-set", "erl-key").
key_put(_, _, _, _, _) ->
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
