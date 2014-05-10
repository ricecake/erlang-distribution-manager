-module(dman_worker).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/3, behaviour_info/1]).

-export([add_task/2, remove_task/2, list_tasks/3, get_status/3, add_cohort/2, remove_cohort/2, call/3, cast/2]).
%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Module, Args, Options) ->
    gen_server:start_link({local, Module}, ?MODULE, {Module, Args}, Options).


behaviour_info(callbacks) -> 
	[
		{handle_add, 2},
		{handle_remove, 2},
		{handle_list, 2},

		{handle_status, 2},

		{handle_quorum_change, 2},

		{init, 2},
		{terminate,2},
		{code_change,3},
		{handle_call,3},
		{handle_cast,2},
		{handle_info,2}
	];
behaviour_info(_Other) -> undefined.

add_task(_,_) -> undef.
remove_task(_,_) -> undef.
list_tasks(_,_,_) -> undef.
get_status(_,_,_) -> undef.
add_cohort(_,_) -> undef.
remove_cohort(_,_) -> undef.
call(_,_,_) -> undef.
cast(_,_) -> undef.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-record(wstate, {module, moduleState}).

init({Module, Args}) ->
	{ok, MState} = apply(Module, init, Args),
    {ok, #wstate{module=Module, moduleState=MState}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
