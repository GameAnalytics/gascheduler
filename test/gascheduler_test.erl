-module(gascheduler_test).

-include_lib("eunit/include/eunit.hrl").

-export([sleep_100/1,
         fail/0,
         kill_if/1]).

gascheduler_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      %%{timeout, 10, ?_test(execute_tasks())},
      %%{timeout, 60, ?_test(max_workers())},
      %%{timeout, 10, ?_test(max_retries())},
      %%{timeout, 10, ?_test(node_down())},
      {timeout, 10, ?_test(all_nodes_down())}
    ]}.

%%
%% Setup
%%

get_master() ->
    master@localhost.


setup() ->
    _ = os:cmd("epmd -daemon"),
    {ok, _Master} = net_kernel:start([get_master(), shortnames]),
    ok.


setup_slaves(Num) ->
    setup_slaves(1, Num).


setup_slaves(Begin, End) ->
    Slaves = [ Slave || {ok, Slave}
                <- [ slave:start_link(localhost, "slave" ++ integer_to_list(N))
                     || N <- lists:seq(Begin, End) ] ],
    ?assertEqual(length(Slaves), End - Begin + 1),
    Slaves.


kill_slaves(Slaves) ->
    lists:foreach(fun slave:stop/1, Slaves).


teardown(_) ->
    _ = net_kernel:stop(),
    ok.

%%
%% Utilities
%%

sleep_100(Id) ->
   timer:sleep(100),
   Id.


fail() ->
    throw(testing_max_retries).


kill_if(Node) ->
    %% Allow Node to be full before killing it.
    timer:sleep(100),
    case node() of
        Node -> slave:stop(Node);
        _ -> ok
    end.


test_tasks(NumTasks, Nodes) ->
    Tasks = lists:seq(1, NumTasks),
    ok = lists:foreach(
        fun(Id) ->
            ok = gascheduler:execute({gascheduler_test, sleep_100, [Id]})
        end, Tasks),
    Received = lists:map(
        fun(_) ->
            receive
                {{ok, Id}, Node, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(sleep_100, Fun),
                    ?assertEqual(length(Args), 1),
                    ?assertEqual(hd(Args), Id),
                    {Id, Node};
                _ ->
                    ?assert(false),
                    {-1, no_node}
            end
         end, Tasks),
    {ReceivedIds, ReceivedNodes} = lists:unzip(Received),
    ?assertEqual(lists:usort(Nodes), lists:usort(ReceivedNodes)),
    ?assertEqual(lists:sort(ReceivedIds), Tasks).


%% Sort nodes according to the number of workers they have, in ascending order
sort_nodes(Running, Nodes) ->
    AccFun = fun ({Pid, _MFA}, Acc) ->
                 Node = node(Pid),
                 Sum = proplists:get_value(Node, Acc, 0),
                 lists:keystore(Node, 1, Acc, {Node, Sum+1})
             end,
    Acc = lists:map(fun (Node) -> {Node, 0} end, Nodes),
    NodeCount = lists:foldl(AccFun, Acc, Running),
    lists:keysort(2, NodeCount).


check_nodes(Running, Nodes, MaxWorkers) ->
    lists:foreach(
        fun({_Node, Count}) ->
            ?assert(Count =< MaxWorkers),
            ok
        end,
        sort_nodes(Running, Nodes)),
    ok.

%%
%% Tests
%%

%% Start two nodes
%% Start the scheduler
%% Execute tasks through the scheduler
%% assert that work is finished
%% assert all stats are updated
execute_tasks() ->
    Nodes = [MasterNode, SlaveNode] = [get_master() | setup_slaves(1)],
    MaxWorkers = 10,
    MaxRetries = 10,
    Client = self(),
    {ok, _} = gascheduler:start_link(Nodes, Client, MaxWorkers, MaxRetries),

    NumTasks = 25,
    test_tasks(NumTasks, Nodes),

    Stats = gascheduler:stats(),
    ?assertEqual(proplists:get_value(ticks, Stats), NumTasks),
    ?assertEqual(proplists:get_value(pending, Stats), 0),
    ?assertEqual(proplists:get_value(running, Stats), 0),
    ?assertEqual(proplists:get_value(max_workers, Stats), MaxWorkers),
    ?assertEqual(proplists:get_value(max_retries, Stats), MaxRetries),
    ?assertEqual(proplists:get_value(worker_node_count, Stats), 2),
    ?assertEqual(proplists:get_value(worker_nodes, Stats), [{MasterNode, 0},
                                                            {SlaveNode, 0}]),

    gascheduler:stop(),
    kill_slaves(tl(Nodes)),

    ok.

%% Start 10 nodes
%% Start the scheduler
%% Run 5000 tasks
%% Intercept calls to ensure max workers is not violated
max_workers() ->
    NumNodes = 10,
    Nodes = [get_master() | setup_slaves(NumNodes - 1)],
    MaxWorkers = 10,
    MaxRetries = 10,
    Client = self(),

    ok = meck:new(gascheduler, [passthrough]),
    ok = meck:expect(gascheduler, pending_to_running,
        fun (State) ->
            ok = check_nodes(element(7, State), Nodes, MaxWorkers),
            NewState = meck:passthrough([State]),
            ok = check_nodes(element(7, NewState), Nodes, MaxWorkers),
            NewState
        end),
    true = meck:validate(gascheduler),

    {ok, _} = gascheduler:start_link(Nodes, Client, MaxWorkers, MaxRetries),

    NumTasks = 5000,
    test_tasks(NumTasks, Nodes),

    gascheduler:stop(),
    kill_slaves(tl(Nodes)),

    ok = meck:unload(gascheduler),
    ok.

%% Start 10 nodes
%% Start the scheduler
%% Run 100 tasks who fail 10 times causing permanent failure
%% Ensure client is notified 100 times
max_retries() ->
    NumNodes = 10,
    Nodes = [get_master() | setup_slaves(NumNodes - 1)],
    MaxWorkers = 10,
    MaxRetries = 10,
    NumTasks = 100,
    Client = self(),

    {ok, _} = gascheduler:start_link(Nodes, Client, MaxWorkers, MaxRetries),

    Tasks = lists:seq(1, NumTasks),
    ok = lists:foreach(
        fun(_) ->
            ok = gascheduler:execute({gascheduler_test, fail, []})
        end, Tasks),
    %% This will only succeed if we receive 100 errors.
    lists:foreach(
        fun(_) ->
            receive
                {{error, {gascheduler, max_retries}}, _Node, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(fail, Fun),
                    ?assertEqual(length(Args), 0)
            end
         end, Tasks),

    gascheduler:stop(),
    kill_slaves(tl(Nodes)),

    ok.

%% Start 3 nodes
%% Start the scheduler
%% The first task that runs on node 1 kills node 1
%% Ensure this task is scheduled on another node
node_down() ->
    NumNodes = 3,
    Nodes = [_Master, Slave1, _Slave2]
          = [get_master() | setup_slaves(NumNodes - 1)],
    MaxWorkers = 10,
    MaxRetries = 10,
    NumTasks = 100,
    Client = self(),

    {ok, _} = gascheduler:start_link(Nodes, Client, MaxWorkers, MaxRetries),

    net_kernel:monitor_nodes(true),

    Tasks = lists:seq(1, NumTasks),
    ok = lists:foreach(
        fun(_) ->
            ok = gascheduler:execute({gascheduler_test, kill_if, [Slave1]})
        end, Tasks),

    receive
        {nodedown, Node} -> ?assertEqual(Node, Slave1)
    end,

    %% This will only succeed if we receive 100 successes.
    lists:foreach(
        fun(_) ->
            receive
                {{ok, ok}, ReceivedNode, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(kill_if, Fun),
                    ?assertEqual(length(Args), 1),
                    ?assertEqual(hd(Args), Slave1),
                    ?assertNotEqual(ReceivedNode, Slave1)
            end
         end, Tasks),

    gascheduler:stop(),
    kill_slaves(tl(Nodes)),

    ok.


%% Start 10 nodes with a separate master that does not execute tasks
%% Start the scheduler on the master
%% Kill all nodes
%% Add worker nodes
%% Ensure all tasks complete
all_nodes_down() ->
    NumNodes = 10,
    Nodes = setup_slaves(NumNodes),
    MaxWorkers = 10,
    MaxRetries = 10,
    NumTasks = 100,
    Client = self(),

    {ok, _} = gascheduler:start_link(Nodes, Client, MaxWorkers, MaxRetries),

    net_kernel:monitor_nodes(true),

    Tasks = lists:seq(1, NumTasks),
    lists:foreach(
        fun(Id) ->
            ok = gascheduler:execute({gascheduler_test, sleep_100, [Id]})
        end, Tasks),

    kill_slaves(Nodes),

    lists:foreach(
        fun(_) ->
            receive
                {nodedown, Node} -> ?assert(lists:member(Node, Nodes))
            end
        end, Nodes),

    NewNodes = setup_slaves(11, 20),
    lists:foreach(fun gascheduler:add_worker_node/1, NewNodes),
    lists:foreach(
        fun(_) ->
            receive
                {nodeup, Node} -> ?assert(lists:member(Node, NewNodes))
            end
        end, NewNodes),

    Received = lists:map(
        fun(_) ->
            receive
                {{ok, Id}, Node, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(sleep_100, Fun),
                    ?assertEqual(length(Args), 1),
                    ?assertEqual(hd(Args), Id),
                    {Id, Node};
                _ ->
                    ?assert(false)
            end
         end, Tasks),
    {ReceivedIds, ReceivedNodes} = lists:unzip(Received),
    ?assertEqual(lists:usort(NewNodes), lists:usort(ReceivedNodes)),
    ?assertEqual(lists:sort(ReceivedIds), Tasks),

    gascheduler:stop(),
    kill_slaves(tl(Nodes)),

    ok.
