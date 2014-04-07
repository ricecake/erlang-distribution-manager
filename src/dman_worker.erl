-module(dman_worker).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, behaviour_info/1]).

-export([add_task/2, remove_task/2, list_tasks/3, get_status/3, add_cohort/2, remove_cohort/2]).
%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).


behaviour_info(callbacks) -> 
	[
		{handle_add, 2},
		{handle_remove, 2},
		{handle_list, 2},

		{handle_status, 2},

		{handle_add_cohort, 2},
		{handle_remove_cohort,2},

		{init, 2},
		{terminate,2},
		{code_change,3},
		{handle_call,3},
		{handle_cast,2},
		{handle_info,2}
	];
behaviour_info(_Other) -> undefined.
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

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

