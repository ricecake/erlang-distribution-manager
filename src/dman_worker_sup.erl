-module(dman_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, create/1, create/2, list/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Args), {I, {I, start_link, Args}, permanent, 5000, worker, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

create(Module) -> create(Module, []).
create(Module, Args) -> supervisor:start_child(?MODULE, ?CHILD(Module, Args)).

list() -> lists:sort([ Module ||{Module, _Pid, _Type, _Module} <- supervisor:which_children(?MODULE)]).
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	{ok, { {one_for_one, 5, 10}, []} }.

