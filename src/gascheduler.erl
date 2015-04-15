%% This module schedules tasks on 1 or more compute nodes in a cluster.
%% It assumes there is only 1 client creating tasks to be executed.
-module(gascheduler).

-behaviour(gen_server).

%% API
-export([start_link/4,
         stop/0,
         execute/1,
         add_worker_node/1,
         stats/0]).

%% For workers
-export([notify_client/4,
         worker_fun/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% For testing
-export([get_node/1,
         pending_to_running/1]).

-type worker_nodes() :: [node()].
-type max_workers() :: non_neg_integer().
-type max_retries() :: pos_integer().
-type client() :: pid().
-type pending() :: queue:queue(mfa()).
-type running() :: [{pid(), mfa()}].
-type stats_integer() :: {ticks | pending | running | worker_node_count
                          | max_workers | max_retries, non_neg_integer()}.
-type stats_nodes() :: {worker_nodes, {node(), Running::non_neg_integer()}}.
-type stats() :: [stats_integer() | stats_nodes()].

%% The result of executing a worker which is also passed to the client.
-type result() :: {ok | error, any()}.

-record(state, {%% a set of all nodes that can run workers
                nodes :: worker_nodes(),

                %% maximum workers on an individual node
                max_workers :: max_workers(),

                %% how many times to restart a worker before giving up
                max_retries :: max_retries(),

                %% the process that generates tasks and receives status
                %% messages from the scheduler
                client :: client(),

                %% pending tasks to be run
                pending :: pending(),

                %% tasks currently running
                %% mfa is needed to restart failed processes
                running :: running(),

                %% the number of times the scheduler has been run
                ticks :: non_neg_integer()}).

%%% API

-spec start_link(worker_nodes(), client(), max_workers(), max_retries())
          -> {ok, pid()}.
start_link(Nodes, Client, MaxWorkers, MaxRetries) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                           [Nodes, Client, MaxWorkers, MaxRetries], []).

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop).

-spec execute(mfa()) -> ok.
execute(MFA) ->
    gen_server:call(?MODULE, {execute, MFA}).

-spec add_worker_node(node()) -> ok | node_not_found.
add_worker_node(Node) ->
    gen_server:call(?MODULE, {add_worker_node, Node}).

-spec stats() -> stats().
stats() ->
  gen_server:call(?MODULE, stats).

%%% For workers

-spec notify_client(pid(), result(), pid(), mfa()) -> ok.
notify_client(Scheduler, Result, Worker, MFA) ->
    gen_server:cast(Scheduler, {Result, Worker, MFA}).

%%% gen_server callbacks

init([Nodes, Client, MaxWorkers, MaxRetries]) ->
    process_flag(trap_exit, true),
    ok = net_kernel:monitor_nodes(true),

    AliveNodes = ping_nodes(Nodes),
    error_logger:info_msg("scheduler: will use these nodes = ~p", [AliveNodes]),

    {ok, #state{nodes = AliveNodes,
                client = Client,
                max_workers = MaxWorkers,
                max_retries = MaxRetries,
                pending = queue:new(),
                running = [],
                ticks = 0}}.

handle_call({execute, MFA}, _From, State) ->
    {reply, ok, execute_try(MFA, State)};

handle_call({add_worker_node, Node}, _From, State = #state{nodes = Nodes}) ->
    {Reply, NewNodes} = case net_adm:ping(Node) of
                            pong ->
                                case lists:member(Node, Nodes) of
                                    true -> {ok, Nodes};
                                    false -> {ok, [Node | Nodes]}
                                end;
                            _    -> {node_not_found, Nodes}
                        end,
    {reply, Reply, State#state{nodes = NewNodes}};

handle_call(stats, _From, State = #state{nodes = Nodes,
                                         pending = Pending,
                                         running = Running,
                                         max_workers = MaxWorkers,
                                         max_retries = MaxRetries,
                                         ticks = Ticks}) ->
    Reply = [{ticks, Ticks},
             {pending, queue:len(Pending)},
             {running, length(Running)},
             {worker_node_count, length(Nodes)},
             {max_workers, MaxWorkers},
             {max_retries, MaxRetries},
             {worker_nodes, sort_nodes(Running, Nodes)}],
    {reply, Reply, State};

handle_call(stop, _From, State) ->
    %% TODO(cdevries): kill workers
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% A tick is sent when one or more tasks are removed from the running queue.
handle_cast(tick, State = #state{ticks = Ticks, pending = Pending}) ->
    NewState = case queue:is_empty(Pending) of
                   true -> State;
                   false -> ?MODULE:pending_to_running(State)
               end,
    {noreply, NewState#state{ticks = Ticks + 1}};

handle_cast({Result, Worker, MFA}, State = #state{running = Running,
                                                  client = Client}) ->
    Client ! {Result, node(Worker), MFA},
    {noreply, State#state{running = remove_worker(Worker, Running)}};

handle_cast(_Msg, State) ->
    {noreply, State}.

% pre: notify_client/3 was called
handle_info({'EXIT', _Worker, normal}, State) ->
    {noreply, State};

handle_info({'EXIT', Worker, _Reason}, State = #state{pending = Pending,
                                                      running = Running}) ->
    %% Even though we catch all exceptions this is still required because
    %% exceptions are not raised when a node becomes unavailable.
    %% We move the task back to pending at the front of queue.
    {_, MFA} = lists:keyfind(Worker, 1, Running),
    {noreply, State#state{pending = queue:in_r(MFA, Pending),
                          running = remove_worker(Worker, Running)}};

handle_info({nodedown, NodeDown}, State = #state{nodes = Nodes}) ->
    error_logger:warning_msg("scheduler: removing node ~p because it is down",
                             [NodeDown]),
    {noreply, State#state{nodes = lists:delete(NodeDown, Nodes)}};

handle_info(Info, State) ->
    error_logger:warning_msg("scheduler: unexpected message ~p", [Info]),
    {noreply, State}.


terminate(Reason, #state{running = Running, pending = Pending} = _State) ->
    error_logger:warning_msg("scheduler: terminating with reason ~p and "
                             "~p running tasks and ~p pending tasks",
                             [Reason, length(Running), queue:len(Pending)]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%%% Internal functions

%% Returns the first free node from Nodes, returns undefined if no node is free
-spec get_free_node(worker_nodes(), max_workers(), running()) -> node() | undefined.
get_free_node(_Nodes, _MaxWorkers = 0, []) ->
    undefined;

get_free_node(Nodes, _MaxWorkers, []) ->
    hd(Nodes);

get_free_node(Nodes, MaxWorkers, Running) ->
    SortedNodes = sort_nodes(Running, Nodes),
    {FirstNode, WorkerCount} = hd(SortedNodes),
    case WorkerCount < MaxWorkers of
        true  -> FirstNode;
        false -> undefined
    end.


%% Sort nodes according to the number of workers they have, in ascending order
-spec sort_nodes(running(), worker_nodes()) -> [{node(), non_neg_integer()}].
sort_nodes(Running, Nodes) ->
    AccFun = fun ({Pid, _MFA}, Acc) ->
                 Node = ?MODULE:get_node(Pid),
                 Sum = proplists:get_value(Node, Acc, 0),
                 lists:keystore(Node, 1, Acc, {Node, Sum+1})
             end,
    Acc = lists:map(fun (Node) -> {Node, 0} end, Nodes),
    NodeCount = lists:foldl(AccFun, Acc, Running),
    lists:keysort(2, NodeCount).


%% Meck cannot mock the erlang module, hence this indirection
-spec get_node(pid()) -> node().
get_node(Pid) ->
    erlang:node(Pid).


-spec ping_nodes([node()]) -> [node()] | [].
ping_nodes(Nodes) ->
    lists:filter(fun(Node) ->
                     case net_adm:ping(Node) of
                         pong -> true;
                         _    -> false
                     end
                 end,
                 Nodes).


%% Executes MFA MaxRetries times
-spec execute_do(mfa(), non_neg_integer()) -> result().
execute_do(_MFA, 0) ->
    {error, failed_max_retries_times};
execute_do(MFA = {Mod, Fun, Args}, MaxRetries) ->
    try
        {ok, apply(Mod, Fun, Args)}
    catch _:_ ->
        execute_do(MFA, MaxRetries - 1)
    end.


-spec worker_fun(pid(), mfa(), max_retries()) -> ok.
worker_fun(Scheduler, MFA, MaxRetries) ->
    Worker = self(),
    Result = execute_do(MFA, MaxRetries),
    gascheduler:notify_client(Scheduler, Result, Worker, MFA).


%% Tries to execute MFA, otherwise queues MFA in pending.
-spec execute_try(mfa(), #state{}) -> #state{}.
execute_try(MFA, State = #state{nodes = Nodes,
                                pending = Pending,
                                running = Running,
                                max_workers = MaxWorkers,
                                max_retries = MaxRetries}) ->
    Scheduler = self(),
    case get_free_node(Nodes, MaxWorkers, Running) of
        undefined -> State#state{pending = queue:in(MFA, Pending)};
        Node -> Args = [Scheduler, MFA, MaxRetries],
                WorkerPid = spawn_link(Node, ?MODULE, worker_fun, Args),
                State#state{running = [{WorkerPid, MFA} | Running]}
    end.


%% Move as many pending tasks as possible to running.
-spec pending_to_running(#state{}) -> #state{}.
pending_to_running(State = #state{pending = Pending,
                                  running = Running,
                                  max_workers = MaxWorkers,
                                  nodes = Nodes}) ->
    %% The execute_try will succeed each time because we first calculate the
    %% number of free slots.
    FreeWorkers = (MaxWorkers * length(Nodes)) - length(Running),
    {Execute, KeepPending} =
        case FreeWorkers > queue:len(Pending) of
            true -> {Pending, queue:new()};
            false -> queue:split(FreeWorkers, Pending)
        end,
    Fun = fun(MFA, AccState) ->
              execute_try(MFA, AccState)
          end,
    lists:foldl(Fun, State#state{pending = KeepPending}, queue:to_list(Execute)).

%% Remove a Worker from Running and send a scheduling tick because slots are
%% free in the run queue.
-spec remove_worker(pid(), running()) -> running().
remove_worker(Worker, Running) ->
    ok = gen_server:cast(?MODULE, tick),
    lists:keydelete(Worker, 1, Running).
