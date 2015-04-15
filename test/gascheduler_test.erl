-module(gascheduler_test).

-include_lib("eunit/include/eunit.hrl").

-export([do_some_work/1]).

gascheduler_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      {timeout, 10, ?_test(execute_tasks())},
      {timeout, 60, ?_test(max_workers())}
    ]}.

-define(MASTER, master@localhost).

%%
%% Setup
%%

setup() ->
    _ = os:cmd("epmd -daemon"),
    {ok, _Master} = net_kernel:start([?MASTER, shortnames]),
    ok.


setup_slaves(Num) ->
    Slaves = [ Slave || {ok, Slave}
                <- [ slave:start_link(localhost, "slave" ++ integer_to_list(N))
                     || N <- lists:seq(1, Num) ] ],
    ?assertEqual(length(Slaves), Num),
    [?MASTER | Slaves].


kill_slaves(Slaves) ->
    lists:foreach(fun slave:stop/1, Slaves).


teardown(_) ->
    _ = net_kernel:stop(),
    ok.

%%
%% Utilities
%%

do_some_work(Id) ->
   timer:sleep(100),
   Id.


test_tasks(NumTasks, Nodes) ->
    Tasks = lists:seq(1, NumTasks),
    ok = lists:foreach(
        fun(Id) ->
            ok = gascheduler:execute({gascheduler_test, do_some_work, [Id]})
        end, Tasks),
    Received = lists:map(
        fun(_) ->
            receive
                {{ok, Id}, Node, {Mod, Fun, Args}} ->
                    ?assertEqual(gascheduler_test, Mod),
                    ?assertEqual(do_some_work, Fun),
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
        fun({Node, Count}) ->
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
    Nodes = [MasterNode, SlaveNode] = setup_slaves(1),
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
    Nodes = setup_slaves(NumNodes - 1),
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
