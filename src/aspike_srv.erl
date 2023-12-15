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
    aerospike_init/0,
    command/1,
    connect/0,
    connect/2,
    config_info/0,
    cluster_info/0,
    destination_add/0,
    destination_add/2,
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
    port_status/0,
    port_info/0,
    help/0,
    help/1,
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
    destination_add(),
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
    start_link(code:priv_dir(aspike_port), ?EXT_PROC_NAME).
start_link(Dir, Prg) ->
    start_link(Dir ++ "/" ++ Prg).
start_link(ExtPrg) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, ExtPrg, []).

-spec command(term()) -> term().
command(Cmd) -> 
    gen_server:call(?MODULE, {command, Cmd}, ?DEFAULT_TIMEOUT + 10).

-spec aerospike_init() -> {ok, string()} | {error, string()}.
aerospike_init() ->
    command({aerospike_init}).

-spec destination_add() -> {ok, string()} | {error, string()}.
destination_add() ->
    destination_add("127.0.0.1", 3010).

-spec destination_add(string(), non_neg_integer()) -> {ok, integer()} | {error, term()}.
destination_add(Host, Port) when is_list(Host); is_integer(Port) -> 
    command({destination_add, Host, Port}).

-spec connect() -> {ok, string()} | {error, string()}.
connect() ->
    connect("", "").

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

-spec port_status() -> {ok, map()} | {error, term()}.
port_status() ->
    command({port_status}).

-spec help() -> {ok, string()} | {error, term()}.
help() -> help("namespaces").

% -spec help("bins" | "sets" | "node" | "namespaces" | "udf-list" | "sindex-list:" | "edition" | "get-config:context=namespace;id=test") -> {ok, string()} | {error, term()}.
-spec help(string()) -> {ok, string()} | {error, term()}.
help(Item) ->
    command({help, Item}).

-spec port_info() -> [tuple()]| undefined.
port_info() ->
    gen_server:call(?MODULE, port_info).

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
    spawn(fun aerospike_init/0),
    {ok, #state{ext_prg = ExtPrg, port = Port}}.
    
handle_call({command, Msg}, {Caller, _}, State=#state{port = Port}) ->
    Res = call_port(Caller, Port, Msg),
    {reply, Res, State};
handle_call(port_info, _, State=#state{port = Port}) ->
    Res = erlang:port_info(Port),
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
% 5> aspike_srv:destination_add().
% {ok,"host and port added"}
% 6> aspike_srv:connect().
% {ok,"connected"}
% 7> aspike_srv:node_random().
% {ok,"127.0.0.1:3010"}
% 8> aspike_srv:node_names(). 
% {ok,["BB9020011AC4202"]}
% 9> aspike_srv:node_get("BB9020011AC4202").
% {ok,"127.0.0.1:3010"}
% 
% 8> aspike_srv:help("namespaces").
% {ok,"namespaces\ttest\n"}
% 9> aspike_srv:help("nodes").     
% {error,"no data"}
% 10> aspike_srv:help("node"). 
% {ok,"node\tBB9020011AC4202\n"}
% 13> aspike_srv:help("bins").
% {ok,"bins\ttest:bin_names=90,bin_names_quota=65535,bin1,bin2,bin3,bin4,test-bin-1,test-bin-2,test-bin-3,test-bin,test-bin-4,mapbin,numbers-bin,binint,binstr,loc,geofilterloc,geofilteramen,myBin,i,s,l,m,a,b,c,d,e,f,g,b1,b2,,bina,binb,binc,app,map,incr,pp,intbin,stringbin,test-list-1,temp,C,testmap,otherbin,map_keystr_bin,map_valstr_bin,bitbin,hllbin_1,hllbin_2,hllbin_3,hllbin_l,hllbin,A,B,D,E,New,binlist,binmap,B5,new_bin,new_bin[0],x,y,z,bigstr,NUMERIC,bn_STRING,int_bin,double_bin,qebin1,qebin2,a_int_bin,a_double_bin,foo,geobin,geolistbin,geomapbin,otherBin,scan-expop,listbin,bbb,list,erl-bin-1,erl_bin_2,Anatoly,bar,baz,erl-bin-111;\n"}
% -------------------------------------------------------------------------------
