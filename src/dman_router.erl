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
	NodeState = {node(), {0, [{buckets, Buckets}, {peers, [{node(), 'UP'}]}, {systems, []}]}},
	{ok, #state{stateData=[NodeState], peers=[{node(), 'UP'}], localBuckets=Buckets, buckets=[{Bucket, {0, [node()]}} || Bucket <- Buckets] }}.

% how often do we want to send a message? in milliseconds.
gossip_freq(State) ->
    {reply, 5000, State}.

% defines what we're gossiping
digest(#state{epoch=Epoch, systems=Systems, localBuckets=Buckets, buckets=BucketData, peers=Peers, stateData=StateData} = State) ->
	NewEpoch = Epoch+1,
	HandleToken = push,
	Status = [{System, dman_worker:get_state(System)} || System <- Systems],
	NodeState = {node(), {NewEpoch, [{buckets, Buckets}, {peers, Peers}, {systems, Status}]}},
	NewStateData = lists:keystore(node(), 1, StateData, NodeState),
	{reply, {NewEpoch, NewStateData, BucketData}, HandleToken, State#state{epoch=NewEpoch, stateData=NewStateData, peers=Peers}}.

handle_cast({debug, Node}, State) ->
	Node! State,
	{noreply, State};

handle_cast({rebalance, NewNodes}, State) when is_list(NewNodes) ->
	io:format("Rebalancing: ~p~n", [NewNodes]),
	{noreply, State};

handle_cast({rebalance, NewNode}, State) when is_tuple(NewNode) ->
	io:format("Rebalancing single: ~p~n", [NewNode]),
	{noreply, State};

handle_cast(_Message, State) ->
	{noreply, State}.

% received a push
handle_gossip(push, TheirState, _From, #state{epoch=MyEpoch, peers=Peers, stateData=MyStateData, buckets=MyBuckets} = State) ->
	MergedState = mergeState({MyEpoch, MyStateData, MyBuckets}, TheirState),
	{NewEpoch, NewState, NewBuckets} = MergedState,
	NewPeers = rectifyPeerList(Peers, MyStateData, NewState),
	{reply, MergedState, pull, State#state{epoch=NewEpoch, peers=NewPeers, stateData=NewState, buckets=NewBuckets}};

% received a symmetric push
handle_gossip(pull, {NewEpoch, NewState, NewBuckets}, _From, #state{stateData=MyStateData, peers=Peers} = State) ->
	NewPeers = rectifyPeerList(Peers, MyStateData, NewState),
	{noreply, State#state{epoch=NewEpoch, peers=NewPeers, stateData=NewState, buckets=NewBuckets}}.

determineLiveness(Node) ->
	case net_adm:ping(Node) of
		pong -> {Node, 'UP'};
		pang -> {Node, 'DOWN'}
	end.

rectifyPeerList(Peers, MyStateData, NewState) ->
	FoundNodes = findNewNodes(MyStateData, NewState),
	NewNodeStatus = [ determineLiveness(Node) || Node <- FoundNodes],
	case NewNodeStatus of
		[] -> ok;
		_  -> gen_gossip:cast(self(), {rebalance, NewNodeStatus}),
		      ok
	end,
	CombinedPeers = lists:usort(fun({A,_}, {B,_})-> B>=A end, lists:append(Peers, NewNodeStatus)),
	[ checkDowned(Node, MyStateData, NewState) || Node <- CombinedPeers].

checkDowned({_Node, 'UP'} = Short, _MyState, _NewState) -> Short;
checkDowned({Node, 'DOWN'}, MyState, NewState) ->
	{LastEpoch, _Meta} = proplists:get_value(Node, MyState),
	{NewEpoch, _NewMeta} = proplists:get_value(Node, NewState),
	case NewEpoch > LastEpoch of
		true -> NodeState = {Node, Status} = determineLiveness(Node),
			case Status of
				'UP' -> gen_gossip:cast(self(), {rebalance, NodeState});
				'DOWN' -> ok
			end,
			NodeState;
		false-> {Node, 'DOWN'}
	end.


% joined cluster
join(Nodelist, #state{peers=Peers, epoch=Epoch} = State) ->
	NewPeers = lists:foldl(fun(Node, List)-> lists:keystore(Node, 1, List, {Node, 'UP'}) end, Peers, Nodelist),
	{noreply, State#state{peers=NewPeers, epoch=Epoch+1}}.

% node left
expire(Node, #state{peers=Peers, epoch=Epoch} = State) ->
	NewPeers = lists:keystore(Node, 1, Peers, {Node, 'DOWN'}),
	hash_ring:remove_node(<<"nodes">>, erlang:atom_to_binary(Node, latin1)),
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


listDifference(MyNodes, TheirNodes) -> 
	lists:usort(fun(A, B)-> B>=A end, sets:to_list(sets:subtract(sets:from_list(MyNodes), sets:from_list(TheirNodes)))).

handleNewNodes(NewNodes, State) ->
	[hash_ring:add_node(<<"nodes">>, erlang:atom_to_binary(Node, latin1)) || Node <- NewNodes],
	balanceBuckets(State#state.localBuckets, 3).	

balanceBuckets(Buckets, Count) -> 
	[{Bucket, [hash_ring:find_node(<<"nodes">>, <<Bucket, N:8>>) || N <- lists:seq(0,Count)]} || Bucket <- Buckets].

findNewNodes(MyState, TheirState) ->
	MyNodes = extractPeers([proplists:lookup(node(), MyState)]),
	TheirNodes = extractPeers(TheirState),
	listDifference(TheirNodes, MyNodes).

extractPeers(NewState) ->
		lists:usort([ Node || {Node, _status} <-lists:flatten([proplists:get_all_values(peers, List) || List <-[Properties || {_, {_, Properties}} <- NewState]])]).
