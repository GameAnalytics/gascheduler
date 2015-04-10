-module(gascheduler_test).

-include_lib("eunit/include/eunit.hrl").

-export([do_some_work/1]).

gascheduler_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      {timeout, 10, ?_test(execute_tasks())}
    ]}.

-define(MASTER, master@localhost).

%%
%% Setup
%%

setup() ->
    _ = os:cmd("epmd -d"),
    {ok, _Master} = net_kernel:start([?MASTER, shortnames]),
    ok.

setup_slaves(Num) ->
    Slaves = [ Slave || {ok, Slave} 
                <- [ slave:start_link(localhost, "slave" ++ integer_to_list(N))
                     || N <- lists:seq(1, Num) ] ],
    ?assertEqual(length(Slaves), Num),
    [?MASTER | Slaves].

teardown(_) ->
    _ = net_kernel:stop(),
    ok.

%%
%% Utilities
%%

test_jobs(N, Nodes) ->
    Jobs = lists:seq(1, N),
    ok = lists:foreach(
        fun(Id) ->
            ok = gascheduler:execute({gascheduler_test, do_some_work, [Id]})
        end, Jobs), 
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
         end, Jobs), 
    {ReceivedIds, ReceivedNodes} = lists:unzip(Received),     
    ?assertEqual(lists:usort(Nodes), lists:usort(ReceivedNodes)),
    ?assertEqual(lists:sort(ReceivedIds), Jobs).   

%%
%% Tests
%%

%% Start two nodes
%% Start the scheduler
%% Execute jobs through the scheduler
%% assert that work is finished
%% assert all stats are updated
execute_tasks() ->
    Nodes = [MasterNode, SlaveNode] = setup_slaves(1),
    MaxWorkers = 10,
    MaxRetries = 10,
    Client = self(),
    {ok, _} = gascheduler:start_link(Nodes, Client, MaxWorkers, MaxRetries),

    NumJobs = 25,
    test_jobs(NumJobs, Nodes),
    
    Stats = gascheduler:stats(),
    ?assertEqual(proplists:get_value(ticks, Stats), NumJobs),
    ?assertEqual(proplists:get_value(pending, Stats), 0),
    ?assertEqual(proplists:get_value(running, Stats), 0),
    ?assertEqual(proplists:get_value(max_workers, Stats), MaxWorkers),
    ?assertEqual(proplists:get_value(max_retries, Stats), MaxRetries),
    ?assertEqual(proplists:get_value(worker_node_count, Stats), 2),
    ?assertEqual(proplists:get_value(worker_nodes, Stats), [{MasterNode, 0},
                                                            {SlaveNode, 0}]),

    ok.

do_some_work(Id) ->
   timer:sleep(100),
   Id.

