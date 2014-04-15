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

-define(HASH_RING_FUNCTION_MD5, 2).

-record(state, {
    epoch = 0,
    peers = [],
    buckets = [],
    systems = [],
    localBuckets = [],
    stateData = []
}).

% stateData = [{node, {epoch, 
%		[
%		  {buckets, []}, 
%		  {peers, [{node, statusPerception}]},
%                 {systems, [{system, state}]}
%		]}
%	 }]
% peers = [{node, statusPerception}]
% buckets = [ {bucket, {epoch, []}}]
% systems = [system]

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_gossip:register_handler(?MODULE, [], epidemic).

%%%===================================================================
%%% gen_gossip callbacks
%%%===================================================================

init([]) ->
	hash_ring:create_ring(<<"buckets">>, 128, ?HASH_RING_FUNCTION_MD5),
	Buckets = [binary:encode_unsigned(Bucket) || Bucket <- lists:seq(0,128)],
	[hash_ring:add_node(<<"buckets">>, Bucket) || Bucket <- Buckets],
	hash_ring:create_ring(<<"nodes">>, 128, ?HASH_RING_FUNCTION_MD5),
	hash_ring:add_node(<<"nodes">>, erlang:atom_to_binary(node(), latin1)),
	NodeState = {node(), {0, [{buckets, Buckets}, {peers, []}, {systems, []}]}},
	{ok, #state{stateData=[NodeState], localBuckets=Buckets, buckets=[{Bucket, {0, [node()]}} || Bucket <- Buckets] }}.

% how often do we want to send a message? in milliseconds.
gossip_freq(State) ->
    {reply, 5000, State}.

% defines what we're gossiping
digest(#state{epoch=Epoch, systems=Systems, localBuckets=Buckets, buckets=BucketData, peers=Peers, stateData=StateData} = State) ->
	NewEpoch = Epoch+1,
	HandleToken = push,
	Status = [{System, dman_worker:get_state(System)} || System <- Systems],
	NewPeers = lists:foldl(fun({Node, _} = New, Acc)-> lists:keystore(Node, 1, Acc, New) end, Peers, [{Node, 'UP'} || Node <- nodes()]),
	NodeState = {node(), {NewEpoch, [{buckets, Buckets}, {peers, NewPeers}, {systems, Status}]}},
	NewStateData = lists:keystore(node(), 1, StateData, NodeState),
	io:format("~p~n", [{NewEpoch, {peers, NewPeers}, {systems, Status}}]),
	{reply, {NewEpoch, NewStateData, BucketData}, HandleToken, State#state{epoch=NewEpoch, stateData=NewStateData, peers=NewPeers}}.

handle_cast(_Message, #state{epoch = Epoch} = State) ->
	{noreply, State#state{epoch = Epoch+1}};

handle_cast({debug, Node}, State) -> Node! State.
% received a push
handle_gossip(push, TheirState, From, #state{epoch=MyEpoch, stateData=MyStateData, buckets=MyBuckets} = State) ->
	MergedState = mergeState({MyEpoch, MyStateData, MyBuckets}, TheirState),
	{NewEpoch, NewState, NewBuckets} = MergedState,
%	_Node = node(From),
	{reply, MergedState, pull, State#state{epoch=NewEpoch, stateData=NewState, buckets=NewBuckets}};


% received a symmetric push
handle_gossip(pull, {NewEpoch, NewState, NewBuckets}, _From, State) ->
	{noreply, State#state{epoch=NewEpoch, stateData=NewState, buckets=NewBuckets}}.

% joined cluster
join(Nodelist, #state{peers=Peers, epoch=Epoch} = State) ->
	NewPeers = lists:foldl(fun(Node, List)-> lists:keystore(Node, 1, List, {Node, 'UP'}) end, Peers, Nodelist),
	{noreply, State#state{peers=NewPeers, epoch=Epoch+1}}.

% node left
expire(Node, #state{peers=Peers, epoch=Epoch} = State) ->
	NewPeers = lists:keystore(Node, 1, Peers, {Node, 'DOWN'}),
	{noreply, State#state{peers=NewPeers, epoch=Epoch+1}}.

code_change(_Oldvsn, State, _Extra) -> {ok, State}.

mergeState({MyEpoch, MyNodes, MyBuckets},{FEpoch, FNodes, FBuckets}) ->
	NewEpoch   = lists:max([MyEpoch, FEpoch])+1,
	NewNodes   = mergeList(MyNodes,   FNodes),
	NewBuckets = mergeList(MyBuckets, FBuckets),
	{NewEpoch, NewNodes, NewBuckets}.

mergeList(MyList, FList) ->
	Sort = fun({AName,{AEpoch, _AList}},{BName, {BEpoch, _BList}}) when AName == BName -> 
		AEpoch >= BEpoch; 
	          ({AName,{_AEpoch, _AList}},{BName, {_BEpoch, _BList}}) -> BName >= AName
	end,
	MergedList = lists:merge(Sort, lists:sort(Sort, MyList),lists:sort(Sort, FList)),
	lists:usort(fun({A,_}, {B,_})-> B>=A end, MergedList).


listDifference(OurState, TheirState) -> 
	MyNodes = [Node || {Node, _} <- OurState],
	TheirNodes = [Node || {Node, _} <- TheirState],
	NewNodes = sets:to_list(sets:subtract(sets:from_list(MyNodes), sets:from_list(TheirNodes))),
	case NewNodes of 
		[] -> {nonew, []};
		_  -> {new, NewNodes}
	end.

handleNewNodes(NewNodes, State) ->
	[hash_ring:add_node(<<"nodes">>, erlang:atom_to_binary(Node, latin1)) || Node <- NewNodes],
	balanceBuckets(State#state.localBuckets, 3).	

balanceBuckets(Buckets, Count) -> 
	[{Bucket, [hash_ring:find_node(<<"nodes">>, <<Bucket, N:8>>) || N <- lists:seq(0,Count)]} || Bucket <- Buckets].

