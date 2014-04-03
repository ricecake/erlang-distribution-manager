%% Simple epidemic based protocol. Gossips an ever increasing epoch
%% value around the cluster
%%
%% Usage:
%%
%%   (a@machine1)> gen_gossip_epidemic:start_link().
%%   (b@machine1)> gen_gossip_epidemic:start_link().
%%   (b@machine1)> net_adm:ping('a@machine1').
%%
-module(dman_router).
-behaviour(gen_gossip).

%% api
-export([start_link/0, handle_cast/2, code_change/3]).

%% gen_gossip callbacks
-export([init/1,
         gossip_freq/1,
         digest/1,
         join/2,
         expire/2,
         handle_gossip/4]).

-record(state, {
    epoch = 0,
    data  = undef
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_gossip:register_handler(?MODULE, [], epidemic).

%%%===================================================================
%%% gen_gossip callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

% how often do we want to send a message? in milliseconds.
gossip_freq(State) ->
    {reply, 500, State}.

% defines what we're gossiping
digest(#state{epoch=Epoch0, data=Data} = State) ->
    HandleToken = push,
    {Mega, Secs, Micro} = erlang:now(),  
    Stamp = Mega*1000*1000*1000*1000 + Secs * 1000 * 1000 + Micro,
    io:format("~p: ~p~n", [Stamp, State#state.data]),
    {reply, {Epoch0, Data}, HandleToken, State}.

handle_cast(Message, #state{epoch = Epoch} = State) ->
	{noreply, State#state{epoch = Epoch+1, data=Message}}.
% received a push
handle_gossip(push, {Epoch, Message}, _From, State) when Epoch >= State#state.epoch ->
	    {noreply, State#state{epoch=Epoch, data=Message}};
handle_gossip(push, _Epoch, _From, State) ->
	{reply, {State#state.epoch, State#state.data }, _HandleToken = pull, State};


% received a symmetric push
handle_gossip(pull, {Epoch, Data}, _From, State) ->
    {noreply, State#state{epoch=Epoch, data=Data}}.

% joined cluster
join(Nodelist, State) ->
	io:format("JOIN: ~p~n",[Nodelist]),
    {noreply, State}.

% node left
expire(Node, State) ->
	io:format("LEAVE: ~p~n",[Node]),
    {noreply, State}.

code_change(_Oldvsn, State, Extra) -> io:format("~p~n",[{State, Extra}]), {ok, State}.
