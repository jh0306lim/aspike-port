-module(as_render).

-export([info_render/2]).

info_render(Res = {error, _}, _) ->
    Res;
info_render({ok, Info}, Item) ->
    [F | Tail] = string:split(string:chomp(Info), "\t", all),
    Res =
        case Item of
            "sets" ->
                sets_render(Tail);
            "bins" ->
                bins_render(Tail);
            "sindex-list:" ->
                sindexes_render(Tail);
            % "get-config" -> config_render(Tail);
            _ ->
                case re:run(Item, "get-config") of
                    {match, _} -> config_render(Tail);
                    _ -> Tail
                end
        end,
    {ok, {F, Res}}.

sets_render(Sets) ->
    set_render([string:split(S, ";", all) || S <- Sets]).

set_render([]) ->
    [];
set_render([R | Tail]) ->
    X = [string:split(I, ":", all) || I <- R],
    Y = [[list_to_tuple(string:split(I, "=", leading)) || I <- L] || L <- X],
    Z = [maps:from_list([{A, value_render(B)} || {A, B} <- [T || T <- L, size(T) == 2]]) || L <- Y],
    [Z | set_render(Tail)].

bins_render(Bins) ->
    bin_render([string:split(B, ",", all) || B <- Bins]).

bin_render([]) ->
    [];
bin_render([[A, B | Bins] | Tail]) ->
    [A1, A2] = string:split(A, "="),
    [A11, A12] = string:split(A1, ":"),
    [B1, B2] = string:split(B, "="),
    [
        maps:from_list([
            {"ns", A11},
            {A12, value_render(A2)},
            {B1, value_render(B2)},
            {"names", Bins}
        ])
        | bin_render(Tail)
    ].

sindexes_render(Indexes) ->
    sindex_render([string:split(Ind, ":", all) || Ind <- Indexes]).

sindex_render([]) ->
    [];
sindex_render([F | Tail]) ->
    X = [string:split(E, "=", leading) || E <- F],
    Y = [list_to_tuple(S) || S <- X, length(S) == 2],
    Z = maps:from_list([{A, value_render(B)} || {A, B} <- Y]),
    [Z | sindex_render(Tail)].

config_render(Conf) ->
    X = string:split(Conf, ";", all),
    Y = [L || L <- [string:split(S, "=", leading) || S <- X], length(L) == 2],
    maps:from_list([{A, value_render(B)} || [A, B] <- Y]).

-spec value_render(string()) -> false | true | integer() | float() | string().
value_render("false") ->
    false;
value_render("true") ->
    true;
value_render("null") ->
    null;
value_render(V) ->
    case string:to_integer(V) of
        {N, []} ->
            N;
        _ ->
            case string:to_float(V) of
                {F, []} -> F;
                _ -> V
            end
    end.
