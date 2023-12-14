% -------------------------------------------------------------------------------
% -------------------------------------------------------------------------------

-module(aspike_srv).

-behaviour(gen_server).

% -------------------------------------------------------------------------------

-define(EXT_PROC_NAME, "aspike_port").
-define(DEFAULT_TIMEOUT, 1000).

-ifndef(TEST).
-define(TEST, true).
-endif.

% -------------------------------------------------------------------------------

% API
-export([
    start_link/0
]).

-ifdef(TEST).
-export([
    start/0,
    b/0
]).
-endif.

% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    add_destination/0,
    add_destination/2,
    command/1,
    connect/0,
    connect/2,
    init_aerospike/0,
    config_info/0,
    cluster_info/0,
    key_put/0,
    key_put/2,
    key_put/5,
    key_remove/0,
    key_remove/1,
    key_remove/4,
    key_get/0,
    key_get/1,
    key_get/4,
    node_random/0,
    node_names/0,
    node_get/1,
% ------
    bar/1,
    foo/1]).

-record(state, {
    ext_prg :: string(),
    port :: port()
}).

-type state() :: #state{}.

% -------------------------------------------------------------------------------
%  API
% -------------------------------------------------------------------------------
 
-ifdef(TEST).

-spec b() -> ok.
b() ->
    start(),
    init_aerospike(),
    add_destination(),
    connect(),
    ok.

start() ->
    start(code:priv_dir(aspike_port), ?EXT_PROC_NAME).
start(Dir, Prg) ->
    start(Dir ++ "/" ++ Prg).
start(ExtPrg) ->
    gen_server:start({local, ?MODULE}, ?MODULE, ExtPrg, []).
-endif.

start_link() ->
    start_link(code:priv_dir(aerospike_port), ?EXT_PROC_NAME).
start_link(Dir, Prg) ->
    start_link(Dir ++ "/" ++ Prg).
start_link(ExtPrg) ->
    gen_server:start({local, ?MODULE}, ?MODULE, ExtPrg, []).

-spec command(term()) -> term().
command(Cmd) -> 
    gen_server:call(?MODULE, {command, Cmd}, ?DEFAULT_TIMEOUT + 10).

-spec init_aerospike() -> {ok, string()} | {error, string()}.
init_aerospike() ->
    command({init_aerospike}).

-spec add_destination() -> {ok, string()} | {error, string()}.
add_destination() ->
    add_destination("127.0.0.1", 3010).

-spec add_destination(string(), non_neg_integer()) -> {ok, integer()} | {error, term()}.
add_destination(Host, Port) when is_list(Host); is_integer(Port) -> 
    command({add_destination, Host, Port}).

-spec connect() -> {ok, string()} | {error, string()}.
connect() ->
    command({connect}).

-spec connect(string(), string()) -> {ok, string()} | {error, string()}.
connect(User, Pwd) when is_list(User); is_list(Pwd) -> 
    command({connect, User, Pwd}).

key_put() ->
    key_put("erl-bin-111", 1111).

key_put(Bin, N) ->
    key_put(Bin, N, "test", "erl-set", "erl-key").
    
-spec key_put(string(), integer(), string(), string(), string()) -> {ok, string()} | {error, string()}.
key_put(Bin, N, Namespace, Set, KeyStr) when is_list(Bin); is_integer(N); is_list(Namespace); is_list(Set); is_list(KeyStr) -> 
    command({key_put, Bin, N, Namespace, Set, KeyStr}).

key_remove() ->
    key_remove("erl-bin-111").

key_remove(Bin) ->
    key_remove(Bin, "test", "erl-set", "erl-key").
    
-spec key_remove(string(), string(), string(), string()) -> {ok, string()} | {error, string()}.
key_remove(Bin, Namespace, Set, KeyStr) when is_list(Bin); is_list(Namespace); is_list(Set); is_list(KeyStr) -> 
    command({key_remove, Bin, Namespace, Set, KeyStr}).

key_get() ->
    key_get("erl-bin-111").

key_get(Bin) ->
    key_get(Bin, "test", "erl-set", "erl-key").
    
-spec key_get(string(), string(), string(), string()) -> {ok, string()} | {error, string()}.
key_get(Bin, Namespace, Set, KeyStr) when is_list(Bin); is_list(Namespace); is_list(Set); is_list(KeyStr) -> 
    command({key_get, Bin, Namespace, Set, KeyStr}).
    

-spec config_info() -> {ok, map()} | {error, term()}.
config_info() ->
    command({config_info}).

-spec cluster_info() -> {ok, map()} | {error, term()}.
cluster_info() ->
    command({cluster_info}).

-spec node_random() -> {ok, string()} | {error, term()}.
node_random() ->
    command({node_random}).

-spec node_names() -> {ok, [string()]} | {error, term()}.
node_names() ->
    command({node_names}).

-spec node_get(string) -> {ok, string()} | {error, term()}.
node_get(NodeName) ->
    command({node_get, NodeName}).

-spec foo(integer()) -> {ok, integer()} | {error, term()}.
foo(X) when is_integer(X) -> 
    command({foo, X}).

-spec bar(integer()) -> {ok, integer()} | {error, term()}.
% bar(X) when is_integer(X) -> 
bar(X) -> 
    command({bar, X}).

% -------------------------------------------------------------------------------
% Callbacks
% -------------------------------------------------------------------------------

-spec init(string()) -> {ok, state()}.
init(ExtPrg) ->
    process_flag(trap_exit, true),
    Port = open_port({spawn_executable, ExtPrg}, [{packet, 2}, binary, nouse_stdio]),
    {ok, #state{ext_prg = ExtPrg, port = Port}}.

handle_call({command, Msg}, {Caller, _}, State=#state{port = Port}) ->
    ok, Res = call_port(Caller, Port, Msg),
    {reply, Res, State};
handle_call(Msg, _From, State) ->
    io:format("~p:~p Msg = ~p~n",[?MODULE, ?FUNCTION_NAME, Msg]),
    {noreply, State}.

handle_cast({command, Msg}, State=#state{port = Port}) ->
    Port ! {self(), {command, term_to_binary(Msg)}},
    {noreply, State};

handle_cast(Msg, State) ->
    io:format("~p:~p Msg = ~p~n",[?MODULE, ?FUNCTION_NAME, Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    io:format("~p:~p Msg = ~p~n",[?MODULE, ?FUNCTION_NAME, Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

% -------------------------------------------------------------------------------
% helpers
% -------------------------------------------------------------------------------
-spec call_port(pid(), pid(), term()) ->  {ok, term()} | {error, timeout}.
call_port(Caller, Port, Msg) ->
    Port ! {self(), {command, term_to_binary(Msg)}},
    receive
        {_Port, {data, Data}} -> Caller ! binary_to_term(Data)
        after ?DEFAULT_TIMEOUT -> {error, timeout_is_out}
    end.


% -------------------------------------------------------------------------------
    % aql -h 127.0.0.1:3010
    % asadm -e info
    % docker run -d --name aerospike -p 3010-3002:3011-3002 aerospike:ee-7.0.0.3 
    % 
    % 
% -------------------------------------------------------------------------------
% aspike_srv:node_random().
% {ok,"127.0.0.1:3010"}
% -------------------------------------------------------------------------------
