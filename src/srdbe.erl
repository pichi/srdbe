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

-export([test/0,
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
