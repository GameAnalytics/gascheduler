-module(gascheduler_test).

-include_lib("eunit/include/eunit.hrl").

-export([sleep_100/1,
         sleep_1000/1,
         fail/0,
         kill_if/1]).

gascheduler_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      {spawn, {timeout, 10, ?_test(execute_tasks())}},
      {spawn, {timeout, 60, ?_test(max_workers())}},
      {spawn, {timeout, 10, ?_test(max_retries())}},
      {spawn, {timeout, 10, ?_test(all_nodes_down())}},
      {spawn, {timeout, 10, ?_test(node_down())}}
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


teardown(_) ->
    _ = net_kernel:stop(),
    ok.


setup_slaves(Num) ->
    setup_slaves(1, Num).


setup_slaves(Begin, End) ->
    Slaves = [ Slave || {ok, Slave}
                <- [ slave:start_link(localhost, "slave" ++ integer_to_list(N))
                     || N <- lists:seq(Begin, End) ] ],
    ?assertEqual(length(Slaves), End - Begin + 1),
    Slaves.


%%
%% Utilities
%%

kill_slaves(Slaves) ->
    lists:foreach(fun(Slave) -> ok = slave:stop(Slave) end, Slaves).


receive_nodeatom(Atom, Nodes) ->
    error_logger:info_msg("waiting for: ~p from ~p", [Atom, Nodes]),
    Result = lists:foreach(
        fun(_) ->
            receive
                {Atom, Node} ->
                    case lists:member(Node, Nodes) of
                        true -> error_logger:info_msg("received ~p from ~p",
                                                      [Atom, Node]);
                        false -> error_logger:error_msg(
                                     "received ~p from UNKNOWN node ~p",
                                     [Atom, Node])
                    end
            end
        end, Nodes),
    error_logger:info_msg("waiting for ~p SUCCESS", [Atom]),
    Result.


receive_nodeup(Nodes) ->
    receive_nodeatom(nodeup, Nodes).


receive_nodedown(Nodes) ->
    receive_nodeatom(nodedown, Nodes).


sleep_100(Id) ->
   timer:sleep(100),
   Id.


sleep_1000(Id) ->
   timer:sleep(1000),
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
            ok = gascheduler:execute(test, {gascheduler_test, sleep_100, [Id]})
        end, Tasks),
    Received = lists:map(
        fun(_) ->
            receive
                {{ok, Id}, Node, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(sleep_100, Fun),
                    ?assertEqual(length(Args), 1),
                    ?assertEqual(hd(Args), Id),
                    ?assert(lists:member(Node, Nodes)),
                    ?assert(lists:member(Id, Tasks)),
                    {Id, Node};
                Msg ->
                    error_logger:error_msg("unexpected message: ~p", [Msg]),
                    ?assert(false),
                    {-1, fail}
            end
         end, Tasks),

    {ReceivedTasks, ReceivedNodes} = lists:unzip(Received),

    %% Ensure all tasks were completed.
    ?assertEqual(lists:usort(ReceivedTasks), Tasks),

    %% Ensure we used all compute nodes.
    ?assertEqual(lists:usort(ReceivedNodes), lists:usort(Nodes)),

    ok.

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
    ok = net_kernel:monitor_nodes(true),
    Slaves = setup_slaves(1),
    receive_nodeup(Slaves),
    Nodes = [MasterNode, SlaveNode] = [get_master() | Slaves],
    MaxWorkers = 10,
    MaxRetries = 10,
    Client = self(),
    {ok, _} = gascheduler:start_link(test, Nodes, Client, MaxWorkers, MaxRetries),

    NumTasks = 25,
    test_tasks(NumTasks, Nodes),

    Stats = gascheduler:stats(test),
    ?assertEqual(proplists:get_value(ticks, Stats), NumTasks),
    ?assertEqual(proplists:get_value(pending, Stats), 0),
    ?assertEqual(proplists:get_value(running, Stats), 0),
    ?assertEqual(proplists:get_value(max_workers, Stats), MaxWorkers),
    ?assertEqual(proplists:get_value(max_retries, Stats), MaxRetries),
    ?assertEqual(proplists:get_value(worker_node_count, Stats), 2),
    ?assertEqual(proplists:get_value(worker_nodes, Stats), [{MasterNode, 0},
                                                            {SlaveNode, 0}]),

    gascheduler:stop(test),
    kill_slaves(Slaves),
    receive_nodedown(Slaves),

    ok.


%% Start 10 nodes
%% Start the scheduler
%% Run 5000 tasks
%% Intercept calls to ensure max workers is not violated
max_workers() ->
    ok = net_kernel:monitor_nodes(true),
    NumNodes = 10,
    Slaves = setup_slaves(NumNodes - 1),
    receive_nodeup(Slaves),
    Nodes = [get_master() | Slaves],
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

    {ok, _} = gascheduler:start_link(test, Nodes, Client, MaxWorkers, MaxRetries),

    NumTasks = 5000,
    test_tasks(NumTasks, Nodes),

    gascheduler:stop(test),
    kill_slaves(Slaves),
    receive_nodedown(Slaves),

    ok = meck:unload(gascheduler),
    ok.


%% Start 10 nodes
%% Start the scheduler
%% Run 100 tasks who fail 10 times causing permanent failure
%% Ensure client is notified 100 times
max_retries() ->
    ok = net_kernel:monitor_nodes(true),
    NumNodes = 10,
    Slaves = setup_slaves(NumNodes - 1),
    receive_nodeup(Slaves),
    Nodes = [get_master() | Slaves],
    MaxWorkers = 10,
    MaxRetries = 10,
    NumTasks = 100,
    Client = self(),

    {ok, _} = gascheduler:start_link(test, Nodes, Client, MaxWorkers, MaxRetries),

    Tasks = lists:seq(1, NumTasks),
    ok = lists:foreach(
        fun(_) ->
            ok = gascheduler:execute(test, {gascheduler_test, fail, []})
        end, Tasks),
    %% This will only succeed if we receive 100 errors.
    lists:foreach(
        fun(_) ->
            receive
                {{error, {gascheduler, max_retries}}, _Node, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(fail, Fun),
                    ?assertEqual(length(Args), 0);
                Msg ->
                    error_logger:error_msg("unexpected message: ~p", [Msg]),
                    ?assert(false)
            end
         end, Tasks),

    gascheduler:stop(test),
    kill_slaves(Slaves),
    receive_nodedown(Slaves),

    ok.


%% Start 10 nodes with a separate master that does not execute tasks
%% Start the scheduler on the master
%% Kill all nodes
%% Add worker nodes
%% Ensure all tasks complete
all_nodes_down() ->
    ok = net_kernel:monitor_nodes(true),
    NumNodes = 10,
    MaxWorkers = 10,
    MaxRetries = 10,
    NumTasks = 100,
    Client = self(),

    Nodes = setup_slaves(NumNodes),
    receive_nodeup(Nodes),

    {ok, _} = gascheduler:start_link(test, Nodes, Client, MaxWorkers, MaxRetries),

    Tasks = lists:seq(1, NumTasks),
    lists:foreach(
        fun(Id) ->
            ok = gascheduler:execute(test, {gascheduler_test, sleep_1000, [Id]})
        end, Tasks),

    kill_slaves(Nodes),
    receive_nodedown(Nodes),

    NewNodes = setup_slaves(11, 20),
    receive_nodeup(NewNodes),
    lists:foreach(fun(Node) -> gascheduler:add_worker_node(test, Node) end,
                  NewNodes),

    Received = lists:map(
        fun(_) ->
            receive
                {{ok, Id}, Node, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(sleep_1000, Fun),
                    ?assertEqual(length(Args), 1),
                    ?assertEqual(hd(Args), Id),
                    ?assert(lists:member(Node, NewNodes)),
                    ?assert(lists:member(Id, Tasks)),
                    Id;
                Msg ->
                    error_logger:error_msg("unexpected message: ~p", [Msg]),
                    ?assert(false),
                    -1
            end
         end, Tasks),

    %% Ensure all tasks were completed. While we received 100 ok messages above
    %% we could have received a particular task twice.
    ?assertEqual(lists:usort(Received), Tasks),

    gascheduler:stop(test),
    kill_slaves(NewNodes),
    receive_nodedown(NewNodes),

    ok.


%% Start 3 nodes
%% Start the scheduler
%% The first task that runs on node 1 kills node 1
%% Ensure this task is scheduled on another node
node_down() ->
    ok = net_kernel:monitor_nodes(true),
    NumNodes = 3,
    MaxWorkers = 10,
    MaxRetries = 10,
    NumTasks = 100,
    Client = self(),

    Slaves = setup_slaves(NumNodes - 1),
    Nodes = [_Master, Slave1, Slave2]
          = [get_master() | Slaves],
    receive_nodeup(Slaves),

    {ok, _} = gascheduler:start_link(test, Nodes, Client, MaxWorkers, MaxRetries),

    Tasks = lists:seq(1, NumTasks),
    ok = lists:foreach(
        fun(_) ->
            ok = gascheduler:execute(test, {gascheduler_test, kill_if, [Slave1]})
        end, Tasks),
    receive_nodedown([Slave1]),

    %% This will only succeed if we receive 100 successes.
    lists:foreach(
        fun(_) ->
            receive
                {{ok, ok}, ReceivedNode, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(kill_if, Fun),
                    ?assertEqual(length(Args), 1),
                    ?assertEqual(hd(Args), Slave1),
                    ?assertNotEqual(ReceivedNode, Slave1);
                Msg ->
                    error_logger:error_msg("unexpected message: ~p", [Msg]),
                    ?assert(false)
            end
         end, Tasks),

    gascheduler:stop(test),
    kill_slaves([Slave2]),
    receive_nodedown([Slave2]),

    ok.
