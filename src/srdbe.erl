-module(srdbe).

-export([init/0, test/0, lift_ii/2, lift_is/2]).

-on_load(init/0).

so_name() -> "srdbe".

init() ->
%  io:put_chars("On load\n"),
  SoName = case code:priv_dir(?MODULE) of
    {error, bad_name} ->
      case filelib:is_dir(filename:join(["..", "priv"])) of
        true ->
          filename:join(["..", "priv", so_name()]);
        false ->
          filename:join(["priv", so_name()])
      end;
    Dir ->
      filename:join(Dir, so_name())
  end,
  erlang:load_nif(SoName, 1).

test() ->
  exit(nif_not_loaded).


lift_ii(IDX, [D]) ->
  [ lift1_ii(I, D) || I<-IDX ].

lift1_ii(_, _) ->
  exit(nif_not_loaded).

lift_is(IDX, [D]) ->
  [ lift1_is(I, D) || I<-IDX ].

lift1_is(_, _) ->
  exit(nif_not_loaded).

