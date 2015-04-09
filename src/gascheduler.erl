%% This module schedules tasks on 1 or more compute nodes in a cluster. 
%% It assumes there is only 1 client creating tasks to be executed.
-module(gascheduler).

-behaviour(gen_server).

%% API
-export([start_link/4,
         execute/1,
         worker_nodes/0,
         add_worker_node/1]).

%% For workers
-export([notify_done/3,
         notify_failed/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([get_node/1]).

-type worker_nodes() :: [node()].
-type max_workers() :: non_neg_integer().
-type max_retries() :: pos_integer().
-type client() :: pid().
-type pending() :: queue:queue(mfa()).
-type running() :: [{pid(), mfa()}].

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
                running :: running()}).

%%% API

-spec start_link(worker_nodes(), client(), max_workers(), max_retries())
          -> {ok, pid()}.
start_link(Nodes, Client, MaxWorkers, MaxRetries) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                           [Nodes, Client, MaxWorkers, MaxRetries], []).

-spec execute(mfa()) -> ok.
execute(MFA) ->
    gen_server:call(?MODULE, {execute, MFA}).

-spec worker_nodes() -> worker_nodes().
worker_nodes() ->
    gen_server:call(?MODULE, worker_nodes).

-spec add_worker_node(node()) -> ok | node_not_found.
add_worker_node(Node) ->
    gen_server:call(?MODULE, {add_worker_node, Node}).

%%% For workers

-spec notify_done(pid(), pid(), mfa()) -> ok.
notify_done(Scheduler, Worker, MFA) ->
    gen_server:cast(Scheduler, {done, Worker, MFA}).

-spec notify_failed(pid(), pid(), mfa()) -> ok.
notify_failed(Scheduler, Worker, MFA) ->
    gen_server:cast(Scheduler, {failed, Worker, MFA}).

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
                running = []}}.

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

handle_call(worker_nodes, _From, State = #state{nodes = Nodes}) ->
    {reply, Nodes, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% A tick is sent when one or more tasks are removed from the running queue.
handle_cast(tick, State) ->
    {noreply, pending_to_running(State)};

handle_cast({done, Worker, MFA}, State = #state{running = Running,
                                                 client = Client}) ->
    error_logger:info_msg("Worker ~p (pid = ~p) done", [Worker, MFA]),
    ok = gen_server:cast(Client, {done, MFA}),
    {noreply, State#state{running = remove_worker(Worker, Running)}};

handle_cast({failed, Worker, MFA}, State = #state{running = Running,
                                                   client = Client}) ->
    error_logger:warning_msg("sheduler: worker ~p (pid = ~p) failed permanently",
                             [Worker, MFA]),
    ok = gen_server:cast(Client, {failed, MFA}),
    {noreply, State#state{running = remove_worker(Worker, Running)}};

handle_cast(_Msg, State) ->
    {noreply, State}.

% pre: notify_done/3 was called 
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
    error_logger:info_msg("scheduler: removing node ~p because it seems to be down",
                          [NodeDown]),
    {noreply, State#state{nodes = lists:delete(NodeDown, Nodes)}};

handle_info(Info, State) ->
    error_logger:info_msg("scheduler: unexpected message ~p", [Info]),
    {noreply, State}.


terminate(Reason, #state{running = Running, pending = Pending} = _State) ->
    error_logger:error_msg("scheduler: terminating with reason ~p and "
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
    AccFun = fun (Pid, Acc) ->
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
-spec execute_do(mfa(), non_neg_integer()) -> done | failed.
execute_do(_MFA, 0) ->
    failed;
execute_do(MFA = {Mod, Fun, Args}, MaxRetries) ->
    try
        _ = apply(Mod, Fun, Args),
        done 
    catch _ ->
        execute_do(MFA, MaxRetries - 1)
    end.

%% Tries to execute MFA, otherwise queues MFA in pending.
-spec execute_try(mfa(), #state{}) -> #state{}. 
execute_try(MFA, State = #state{nodes = Nodes,
                                pending = Pending,
                                running = Running,
                                max_workers = MaxWorkers,
                                max_retries = MaxRetries}) ->
    Scheduler = self(),
    WorkerFun = fun() ->
                    Worker = self(),
                    case execute_do(MFA, MaxRetries) of
                        done -> gascheduler:notify_done(Scheduler, Worker, MFA);
                        failed -> gascheduler:notify_failed(Scheduler, Worker, MFA)
                    end
                end,
    case get_free_node(Nodes, MaxWorkers, Running) of
        undefined -> State#state{pending = queue:in(MFA, Pending)};
        Node -> State#state{running = [{spawn_link(Node, WorkerFun), MFA} | Running]}
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
