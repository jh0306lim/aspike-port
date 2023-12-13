-module(complex2).
-export([start/0,start/1, start/2, stop/0, init/1]).
-export([foo/1, bar/1]).

-define(PROC_NAME, ?MODULE).

start() ->
	start(atom_to_list(?MODULE)).
start(Prg) ->
    start(code:priv_dir(aerospike_port), Prg).
start(Dir, Prg) ->
    spawn(?MODULE, init, [Dir ++ "/" ++ Prg]).

stop() ->
    ?PROC_NAME ! stop.

foo(X) ->
    call_port({foo, X}).
bar(Y) ->
    call_port({bar, Y}).

call_port(Msg) ->
    ?PROC_NAME ! {call, self(), Msg},
    receive
        {?PROC_NAME, Result} -> Result
    end.

init(ExtPrg) ->
    register(?PROC_NAME, self()),
    process_flag(trap_exit, true),
    Port = open_port({spawn_executable, ExtPrg}, [{packet, 2}, binary, nouse_stdio]),
    loop(Port).

loop(Port) ->
    receive
        {call, Caller, Msg} ->
            Port ! {self(), {command, term_to_binary(Msg)}},
            receive
                {_Port, {data, Data}} -> Caller ! {?PROC_NAME, binary_to_term(Data)}
            end,
            loop(Port);
        stop ->
            Port ! {self(), close},
            receive
                {_Port, closed} -> exit(normal)
            end;
        {'EXIT', _Port, _Reason} -> exit(port_terminated)
    end.
