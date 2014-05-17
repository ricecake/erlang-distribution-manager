-module(dman_router).
-behaviour(gen_gossip).

%% api
-export([start_link/0, handle_call/3, handle_cast/2, code_change/3, attach/1, dumpState/0]).

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


attach(Node) when is_atom(Node) ->
	gen_gossip:cast(dman_router, {attach, Node}).

dumpState() ->
	gen_gossip:cast(dman_router, {debug, self()}),
	receive
		Message -> io:format("~p~n", [Message])
	end.

%%%===================================================================
%%% gen_gossip callbacks
%%%===================================================================

init([]) ->
	initRingBucket(),
	initRingNode(),
	Buckets = [addRingBucket(Bucket) || Bucket <- lists:seq(0,32)],
	addRingNode(node()),
	NodeState = {node(), {0, [{buckets, Buckets}, {peers, [{node(), 'UP'}]}, {systems, []}]}},
	{ok, #state{stateData=[NodeState], peers=[{node(), 'UP'}], localBuckets=Buckets, buckets=[{Bucket, {0, [node()]}} || Bucket <- Buckets] }}.

% how often do we want to send a message? in milliseconds.
gossip_freq(State) ->
    {reply, 1000, State}.

% defines what we're gossiping
digest(#state{epoch=Epoch, localBuckets=Buckets, buckets=BucketData, peers=Peers, stateData=StateData} = State) ->
	NewEpoch = Epoch+1,
	HandleToken = push,
	NodeState = {node(), {NewEpoch, [{buckets, Buckets}, {peers, Peers}, {system, getSystemStatus()}]}},
	NewStateData = lists:keystore(node(), 1, StateData, NodeState),
	{reply, {NewEpoch, NewStateData, BucketData, dman_worker_sup:list()}, HandleToken, State#state{epoch=NewEpoch, stateData=NewStateData, peers=Peers}}.

handle_cast({debug, Node}, State) ->
	Node! State,
	{noreply, State};

handle_cast({rebalance, NewNodes}, State) when is_list(NewNodes) ->
	NewState = handleNewNodes(NewNodes, State),
	{noreply, NewState};

handle_cast({rebalance, NewNode}, State) when is_tuple(NewNode) ->
	NewState = handleNewNodes([NewNode], State),
	{noreply, NewState};

handle_cast({attach, SNode}, State) -> 
	net_adm:ping(SNode),
	{noreply, State};

handle_cast({add, {Key, {_SubSystem, _Details} = JobDetails}}, #state{buckets=Buckets} = State) ->
	Bucket = getBucketForKey(Key),
	{_epoch, Nodes}  = proplists:get_value(Bucket, Buckets),
	[gen_gossip:cast({dman_router, Node}, {do_add, {Bucket, Key, JobDetails}}) || Node <- Nodes],
	{noreply, State};

handle_cast({do_add, {Bucket, Key, {SubSystem, Details}}}, State) ->
	dman_worker:add_task(SubSystem, {Bucket, Key, Details}),
	{noreply, State};
	
handle_cast(_Message, State) ->
	{noreply, State}.

handle_call(bootstrap, _From, #state{buckets=Buckets, epoch=Epoch} = State) ->
	{reply, {Epoch, Buckets, dman_worker_sup:list()}, State};

handle_call(_Request, _From, State) ->
	{noreply, State}.
% received a push
handle_gossip(push, TheirState, _From, #state{epoch=MyEpoch, peers=Peers, stateData=MyStateData, buckets=MyBuckets} = State) ->
	MergedState = mergeState({MyEpoch, MyStateData, MyBuckets, dman_worker_sup:list()}, TheirState),
	{NewEpoch, NewState, NewBuckets, NewWorkers} = MergedState,
	NewPeers = rectifyPeerList(Peers, MyStateData, NewState),
	{reply, MergedState, pull, State#state{epoch=NewEpoch, peers=NewPeers, stateData=NewState, buckets=NewBuckets}};

% received a symmetric push
handle_gossip(pull, {NewEpoch, NewState, NewBuckets, NewWorkers}, _From, #state{stateData=MyStateData, peers=Peers} = State) ->
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
	{LastEpoch, _Meta} = proplists:get_value(Node, MyState, {-1, []}),
	{NewEpoch, _NewMeta} = proplists:get_value(Node, NewState, {infinity, []}),
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
join([SNode|_] = Nodelist, #state{peers=Peers} = State) ->
	NewPeers = lists:foldl(fun(Node, List)-> lists:keystore(Node, 1, List, {Node, 'UP'}) end, Peers, Nodelist),
	{NewEpoch, Buckets, NewWorkers} = gen_gossip:call({dman_router, SNode}, bootstrap),
        [addRingNode(Node) || {Node, NState} <- NewPeers, NState =:= 'UP'],
        RebalancedBuckets = balanceBuckets([Bucket||{Bucket, _Data} <- Buckets], lists:min([3, length([ Node ||{Node, NState}<- NewPeers, NState =:= 'UP'])])),
	NewBucketData = [{Bucket, {NewEpoch, Blist}} || {Bucket, Blist} <- RebalancedBuckets],
	NewLocalBuckets = localBucketTransform(node(), NewBucketData),
	ok = transferData(NewLocalBuckets, [], NewBucketData, Buckets, NewPeers),
	{noreply, State#state{buckets=NewBucketData, localBuckets=NewLocalBuckets, peers=NewPeers, epoch=NewEpoch}}.

% node left
expire(Node, #state{peers=Peers, epoch=Epoch} = State) ->
	NewPeers = lists:keystore(Node, 1, Peers, {Node, 'DOWN'}),
	NewState = handleNewNodes([{Node, 'DOWN'}], State#state{peers=NewPeers, epoch=Epoch+1}),
	{noreply, NewState}.

code_change(_Oldvsn, State, _Extra) -> {ok, State}.

mergeState({MyEpoch, MyNodes, MyBuckets, MyWorkers},{FEpoch, FNodes, FBuckets, FWorkers}) ->
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
listStaticElements(MyNodes, TheirNodes) ->
	New = sets:from_list(MyNodes),
	Old = sets:from_list(TheirNodes),
	All = sets:union(New, Old),
	Both= sets:intersection(New, Old),
	Static = sets:union(sets:subtract(All, Old), Both),
	lists:usort(fun(A, B)-> B>=A end, sets:to_list(Static)).


handleNewNodes(NewNodes, #state{epoch=Epoch, peers=Peers, localBuckets=LBuckets, buckets=BucketData} = State) ->
	[addRingNode(Node) || {Node, NState} <- NewNodes, NState =:= 'UP'],
	[delRingNode(Node) || {Node, NState} <- NewNodes, NState =:= 'DOWN'],
	RebalancedBuckets = balanceBuckets(LBuckets, lists:min([3, length([ Node ||{Node, NState}<-Peers, NState =:= 'UP'])])),
	NewBucketData = lists:foldl(fun({Bucket, Blist}, List) -> lists:keystore(Bucket, 1, List, {Bucket, {Epoch+1, Blist}}) end, BucketData, RebalancedBuckets),
	NewLocalBuckets = localBucketTransform(node(), NewBucketData),
	ok = transferData(NewLocalBuckets, LBuckets, NewBucketData, BucketData, Peers),
	State#state{epoch=Epoch+1, buckets=NewBucketData, localBuckets=NewLocalBuckets}.


balanceBuckets(Buckets, Count) ->
	[buildBucketList(Bucket, Count, 0, [], 0) || Bucket <- Buckets].

buildBucketList(Bucket, Needs, Has, Accum, _Index) when Needs == Has -> {Bucket, Accum};
buildBucketList(Bucket, Needs, _Has, Accum, Index) -> 
	Node = getNodeForBucket(Index, Bucket),
	NewList = lists:usort([Node|Accum]),
	buildBucketList(Bucket, Needs, length(NewList), NewList, Index+1).

findNewNodes(MyState, TheirState) ->
	MyNodes = extractPeers([proplists:lookup(node(), MyState)]),
	TheirNodes = extractPeers(TheirState),
	listDifference(TheirNodes, MyNodes).

extractPeers(NewState) ->
		lists:usort([ Node || {Node, _status} <-lists:flatten([proplists:get_all_values(peers, List) || List <-[Properties || {_, {_, Properties}} <- NewState]])]).

localBucketTransform(TopicNode, NewBucketData) ->
	Transformed = lists:foldl(fun({Node, Bucket}, Dict)-> dict:append(Node, Bucket, Dict) end, dict:new(), lists:flatten([ [{Node, Bucket} || Node <- NodeList] || {Bucket, {_Epoch, NodeList}} <- NewBucketData])),
	case dict:find(TopicNode, Transformed) of
		{ok, Buckets} -> Buckets;
		error         -> []
	end.

dnode(Node) when is_binary(Node) -> Node;
dnode(Node) when is_pid(Node) -> node(Node);
dnode(Node) when is_atom(Node) -> erlang:atom_to_binary(Node, latin1).

getSystemStatus() ->
	Cpu = {cpu, [{Method, apply(cpu_sup, Method, [])}|| Method <- [avg1, avg5, avg15, nprocs, util]]},
	Mem = {memory, memsup:get_system_memory_data()},
	Disk= {disk, disksup:get_disk_data()},
	Alarms = {alarms, alarm_handler:get_alarms()},
	[Cpu, Mem, Disk, Alarms].

extractBucketNodes(Bucket, BucketData) ->
	{_Epoch, Nodes} = proplists:get_value(Bucket, BucketData, {0, []}),
	Nodes.

getNodeForBucket(Prefix, Bucket) ->
	{ok, Node} = hash_ring:find_node(<<"nodes">>, << (binary:encode_unsigned(Prefix))/bits, (binary:encode_unsigned(Bucket))/bits>>),
	binary_to_atom(Node, latin1).

getBucketForKey(Key) when is_binary(Key) ->
	{ok, <<"B", Bucket/bits>>} = hash_ring:find_node(<<"buckets">>, Key),
	binary:decode_unsigned(Bucket).

addRingNode(Node) -> hash_ring:add_node(<<"nodes">>, dnode(Node)), Node.

addRingBucket(Bucket) when is_integer(Bucket) -> addRingBucket(binary:encode_unsigned(Bucket));
addRingBucket(Bucket) when is_binary(Bucket) -> hash_ring:add_node(<<"buckets">>, <<"B", Bucket/bits>>), binary:decode_unsigned(Bucket).

delRingNode(Node) -> hash_ring:remove_node(<<"nodes">>, dnode(Node)).

delRingBucket(Bucket) when is_integer(Bucket) -> delRingBucket(binary:encode_unsigned(Bucket));
delRingBucket(Bucket) when is_binary(Bucket) -> hash_ring:remove_node(<<"buckets">>, <<"B", Bucket/bits>>).

initRingNode() -> hash_ring:create_ring(<<"nodes">>, 64, ?HASH_RING_FUNCTION_MD5), ok.
initRingBucket() -> hash_ring:create_ring(<<"buckets">>, 256, ?HASH_RING_FUNCTION_MD5), ok.


% this is where I will put a function that tells us what node we lost what buckets too.
% essentiall, calculate list difference on the set of local buckets, and then look up what
% nodes have those buckets now, and compare that to what we used to know was the ownership of 
% each bucket.  so list difference on local nodes, and then list difference on bucket nodes.

transferData(NewBuckets, OldBuckets, NewBucketData, OldBucketData, Peers) -> 
	GainedBuckets =  listDifference(NewBuckets, OldBuckets),
	Me = node(),
	Sources = [{Bucket, 
			lists:delete(Me, listStaticElements(extractBucketNodes(Bucket, NewBucketData), extractBucketNodes(Bucket, OldBucketData)))}
		||Bucket <- GainedBuckets],
	ValidSources = [
		{Bucket, lists:filter(
				fun(Node)->
					case proplists:get_value(Node, Peers, 'DOWN') of 
						'DOWN' -> false;
						'UP'   -> true
					end
				end,
				NodeList)}
	|| {Bucket, NodeList} <- Sources],
	[dman_transfer_sup:transfer(DataSpec) || DataSpec <- ValidSources],
	io:format("gotBuckets: ~p~nfrom: ~p~n", [GainedBuckets, ValidSources]),
	ok.
