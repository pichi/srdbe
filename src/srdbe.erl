%%%-------------------------------------------------------------------
%%% @author Hynek Vychodil
%%% @copyright 2012 Hynek Vychodil
%%% @doc Low level srdbe engine
%%%
%%% srdbe works with binary streams. Module expects all input data has
%%% proper size.
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(srdbe).

-export([
  test/0,
  projection_ii/2,
  lift_ii/2,
  lift_is/2,
  sum_di/5]).

-compile(export_all).

-on_load(init/0).

-define(CHUNK_SIZE, 65536).

%%%===================================================================
%%% API
%%%===================================================================

-type stream() :: [binary()].

-type nullity() :: 'not_null' | 'default_null'.

-spec test() -> any().

test() ->
  exit(nif_not_loaded).

%% @doc Project Lu into Data stream
%%
%% The function makes projection index `Idx' which projects given data
%% `Data' into look-up `Lu'. `Lu' have to contain unique values. This
%% function project integer (i32) values into integer look-up and returns
%% integer index.
%%
%% @end

-spec projection_ii( Data, Lu ) -> Idx when
  Data :: stream(),
  Lu :: stream(),
  Idx :: {'stream', {'integer', nullity()}, stream()}.

projection_ii(Data, Lu) ->
  LuKV = projection_make_lu(Lu),
  projection_make_idx(LuKV, Data).

%% @doc Lift Data using Index
%%
%% The function makes new {@link stream().}. Result data are from `Data'
%% and are stored in place defined by index `Idx'. This function transforms
%% integer (i32) data indexed by integer index.
%%
%% The operation can be symbolically written as:
%% ```
%% for(i=0; i<Idx.size; i++)
%%   Result[i] = Data[Idx[i]];
%% '''
%% @end

-spec lift_ii( Idx, Data ) -> stream() when
  Idx :: stream(),
  Data :: stream().

lift_ii(IDX, [D]) ->
  [ lift1_ii(I, D) || I<-IDX ].

%% @doc Lift Data using Index
%%
%% The function works in the same manner as {@link lift_ii/2.} but
%% transforms binary (boolean) data indexed by integer index.
%%
%% @see lift_ii/2.
%% @end

-spec lift_is( Idx, Data ) -> stream() when
  Idx :: stream(),
  Data :: stream().

lift_is(IDX, [D]) ->
  [ lift1_is(I, D) || I<-IDX ].

%% @doc Make element filter
%%
%% Function makes new bit-mask (boolean) `Set' which is set true in same
%% place where value in `Data' exists in `Filter'. Values in `Data' and
%% `Filter' are integer (i32).
%%
%% @end

-spec element_filter_ii(Data, Filter) -> Set when
  Data :: stream(),
  Filter :: stream(),
  Set :: {'stream', {'set', 'not_null'}, stream()}.

element_filter_ii(A, S) ->
  LuKV = projection_make_lu(S),
  {stream, {set, not_null}, element_filter(LuKV, A)}.

%% @doc Create aggregation elements set
%%
%% The function makes stream of all unique `Key' values which are in `Set'.
%% `Key' values are integer (i32) and `Set' is bit-mask (boolean) of same
%% size. Resulting aggregation `Elements' are alphabetically ordered unique
%% values from `Key' where corresponding value in `Set' is true.
%%
%% @end

-spec aggregation(Key, Set) -> Elements when
  Key :: stream(),
  Set :: stream(),
  Elements :: {'stream', {'integer', 'not_null'}, stream()}.

aggregation(D, S) ->
  Aggr = scan(D, S),
  {stream, {integer, not_null}, gather_i(Aggr, ?CHUNK_SIZE)}.

%% @doc Sum data using index
%%
%% The function sums `Data' into stream indexed by `Idx' filtered by `Set'.
%% Function makes stream tuple with type `Type'. Function requires `Size' of
%% output stream.
%%
%% @see lift_ii/2.
%% @end

-spec sum_di(Type, Data, Idx, Set, Size) ->
  {'stream', {Type, nullity()}, Result} when
  Type :: any(),
  Data :: stream(),
  Idx :: stream(),
  Set :: stream(),
  Size :: integer(),
  Result :: stream().

sum_di(Type, Data, Idx, Set, Size) ->
  Accu = create_rw_bin_d(Size),
  sum_di(Accu, Data, Idx, Set),
  Result = make_bin(Accu, ?CHUNK_SIZE*8),
  {stream, {Type, has_null_d(Result)}, Result}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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

lift1_ii(_, _) ->
  exit(nif_not_loaded).

lift1_is(_, _) ->
  exit(nif_not_loaded).

create_rw_bin_i(_) ->
  exit(nif_not_loaded).

create_rw_bin_d(_) ->
  exit(nif_not_loaded).

sum_di(Accu, [HF|TF], [HI|TI], [HS|TS]) ->
  sum_di_(Accu, HF, HI, HS),
  sum_di(Accu, TF, TI, TS);
sum_di(Accu, [], [], []) -> Accu.

sum_di_(_, _, _, _) ->
  exit(nif_not_loaded).

make_bin(_, _) ->
  exit(nif_not_loaded).

has_null_i(_) ->
  exit(nif_not_loaded).

has_null_d(_) ->
  exit(nif_not_loaded).

projection_make_lu(L) ->
  projection_make_lu(create_trie_id(), L, 1).

projection_make_lu(LuKV, [], _) -> LuKV;
projection_make_lu(LuKV, [H|T], N) when is_binary(H) ->
  projection_make_lu(LuKV, T, projection_make_lu_(LuKV, H, N)).

create_trie_id() ->
  exit(nif_not_loaded).

projection_make_lu_(_, _, _) ->
  exit(nif_not_loaded).

projection_make_idx(LuKV, F) ->
  projection_make_idx(LuKV, not_null, lists:reverse(F), []).

projection_make_idx(_, Null, [], Acc) -> {stream, {integer, Null}, Acc};
projection_make_idx(LuKV, not_null, [H | T], Acc) when is_binary(H) ->
  {Null, NewH} = projection_make_idx_(LuKV, H),
  projection_make_idx(LuKV, Null, T, [NewH | Acc]);
projection_make_idx(LuKV, default_null, [H | T], Acc) when is_binary(H) ->
  {_, NewH} = projection_make_idx_(LuKV, H),
  projection_make_idx(LuKV, default_null, T, [NewH | Acc]).

projection_make_idx_(_, _) ->
  exit(nif_not_loaded).

scan(D, S) ->
  scan(create_trie_id(), D, S).

scan(Aggr, [], []) -> Aggr;
scan(Aggr, [DH|DT], [SH|ST]) ->
  scan_is(Aggr, DH, SH),
  scan(Aggr, DT, ST).

scan_is(_,_,_) ->
  exit(nif_not_loaded).

gather_i(_,_) ->
  exit(nif_not_loaded).

element_filter(_, []) -> [];
element_filter(LuKV, [H|T]) ->
  [element_filter_(LuKV, H) | element_filter(LuKV, T)].

element_filter_(_,_) ->
  exit(nif_not_loaded).
