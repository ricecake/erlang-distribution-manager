-module(dman_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, create/1, create/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

create(Module) -> create(Module, []).
create(Module, Args) -> supervisor:start_child(?MODULE, [Module, Args, []]).
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	{ok, { {simple_one_for_one, 5, 10}, [?CHILD(dman_worker,worker)]} }.

