-module(utils).

-include("../include/defines.hrl").

-export([
    find_lib/1,
    find_lib/2
]).

find_lib(LibName) ->
    find_lib(?APPNAME, LibName).

find_lib(AppName, LibName) ->
    case code:priv_dir(AppName) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, LibName]);
                _ ->
                    filename:join([priv, LibName])
            end;
        Dir ->
            filename:join(Dir, LibName)
    end.
