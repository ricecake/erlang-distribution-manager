-module(dman_transfer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, transfer/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

transfer(DataSpec) -> supervisor:start_child(?MODULE, [DataSpec]).
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	{ok, { {simple_one_for_one, 5, 10}, [?CHILD(dman_transfer_worker, worker)]} }.
