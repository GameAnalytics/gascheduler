%% This module schedules tasks on 1 or more compute nodes in a cluster.
%% It assumes there is only 1 client creating tasks to be executed.
-module(gascheduler).

-behaviour(gen_server).

%% API
-export([start_link/5,
         start_link/4,
         stop/1,
         execute/2,
         add_worker_node/2,
         stats/1,
         unfinished/1,
         set_retry_timeout/2,
         set_max_workers/2,
         get_max_workers/1 
        ]).

%% For workers
-export([notify_client/4,
         worker_fun/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% For testing
-export([pending_to_running/1]).

%% Types
-type worker_nodes()  :: [node()].
-type max_workers()   :: non_neg_integer().
-type max_retries()   :: pos_integer() | infinity.
-type client()        :: pid().
-type pending()       :: queue:queue(mfa()).
-type running()       :: [{pid(), mfa()}].
-type stats_integer() :: {ticks | pending | running | worker_node_count
                          | max_workers | max_retries, non_neg_integer()}.
-type stats_nodes()   :: {worker_nodes, {node(), Running::non_neg_integer()}}.
-type stats()         :: [stats_integer() | stats_nodes()].

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
                ticks :: non_neg_integer(),

                %% wait this long before retrying a task on failure
                retry_timeout :: non_neg_integer()}).


%%% API
-spec start_link(atom(), worker_nodes(), client(), max_workers(), max_retries())
          -> {ok, pid()}.
start_link(Name, Nodes, Client, MaxWorkers, MaxRetries) ->
    gen_server:start_link({local, Name}, ?MODULE,
                           [Nodes, Client, MaxWorkers, MaxRetries], []).

-spec start_link(worker_nodes(), client(), max_workers(), max_retries())
          -> {ok, pid()}.
start_link(Nodes, Client, MaxWorkers, MaxRetries) ->
    gen_server:start_link(?MODULE,
                           [Nodes, Client, MaxWorkers, MaxRetries], []).

-spec stop(atom()) -> ok.
stop(Name) ->
    gen_server:call(Name, stop).

-spec execute(atom(), {module(), atom(), [term()]}) -> ok.
execute(Name, MFA) ->
    gen_server:call(Name, {execute, MFA}).

-spec add_worker_node(atom(), node()) -> ok | node_not_found.
add_worker_node(Name, Node) ->
    %% If all worker nodes were down then a tick needs to be sent to restart
    %% scheduling of pending tasks.
    ok = gen_server:cast(Name, tick),
    gen_server:call(Name, {add_worker_node, Node}).

-spec stats(atom()) -> stats().
stats(Name) ->
  gen_server:call(Name, stats).

%% Return all tasks that are not yet finished.
-spec unfinished(atom()) -> list(mfa()).
unfinished(Name) ->
    gen_server:call(Name, unfinished).

-spec set_retry_timeout(atom(), non_neg_integer()) -> ok.
set_retry_timeout(Name, RetryTimeout) ->
    gen_server:call(Name, {set_retry_timeout, RetryTimeout}).

-spec set_max_workers(atom(), non_neg_integer()) -> ok.
set_max_workers(Name, Workers) ->
    gen_server:call(Name, {set_max_workers, Workers}).

-spec get_max_workers(atom()) -> non_neg_integer().
get_max_workers(Name) ->
    gen_server:call(Name, get_max_workers).
    


%%% For workers

-spec notify_client(pid(), result(), pid(), mfa()) -> ok.
notify_client(Scheduler, Result, Worker, MFA) ->
    gen_server:cast(Scheduler, {Result, Worker, MFA}).


%%% gen_server callbacks

init([Nodes, Client, MaxWorkers, MaxRetries]) ->
    process_flag(trap_exit, true),
    ok = net_kernel:monitor_nodes(true),

    {ok, #state{nodes         = ping_nodes(Nodes),
                client        = Client,
                max_workers   = MaxWorkers,
                max_retries   = MaxRetries,
                retry_timeout = 1000,
                pending       = queue:new(),
                running       = [],
                ticks         = 0}}.

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

handle_call(stop, _From, State = #state{running = Running, pending = Pending}) ->
    case Running =:= [] andalso queue:is_empty(Pending) of
        true ->
            {stop, normal, ok, State};
        false ->
            {stop, normal, {error, {not_empty, Running, Pending}}, State}
    end;

handle_call(unfinished, _From, State = #state{pending = Pending,
                                              running = Running}) ->
    Reply = queue:to_list(Pending) ++ [MFA || {_Pid, MFA} <- Running],
    {reply, Reply, State};

handle_call({set_retry_timeout, Timeout}, _From, State) ->
    {reply, ok, State#state{retry_timeout = Timeout}};

handle_call({set_max_workers, Workers}, _From, State) ->
    {reply, ok, State#state{max_workers = Workers}};

handle_call(get_max_workers, _From, State) ->
    {reply, ok, State#state.max_workers};

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
    Name = case process_info(self(), registered_name) of
               {registered_name, N} -> N;
               _                    -> self() %% no name, just use pid
           end,
    Client ! {Name, Result, node(Worker), MFA},
    {noreply, State#state{running = remove_worker(Worker, Running)}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% pre: notify_client/3 was called
handle_info({'EXIT', Client, Reason}, State = #state{client = Client,
                                                     pending = Pending,
                                                     running = Running}) ->
    error_logger:warning_msg("gascheduler: exit ~p from client running=~p,"
                             " pending=~p", [Reason, length(Running),
                                             queue:len(Pending)]),
    {stop, Reason, State};
handle_info({'EXIT', _Worker, normal}, State) ->
    {noreply, State};
handle_info({'EXIT', Worker, Reason}, State = #state{pending = Pending,
                                                      running = Running}) ->
    case get_running_worker(Worker, Running) of
        {ok, MFA} ->
            %% Even though we catch all exceptions this is still required
            %% because exceptions are not raised when a node becomes
            %% unavailable. We move the task back to pending at the front of
            %% queue.
            error_logger:warning_msg("gascheduler: exit ~p from worker ~p "
                                     "running=~p, pending=~p",
                                     [Reason, Worker, length(Running),
                                      queue:len(Pending)]),
            {noreply, State#state{pending = queue:in_r(MFA, Pending),
                                  running = remove_worker(Worker, Running)}};
        not_found ->
            %% We received exit form other pid that worker or client and
            %% for that we are ignoring it
            error_logger:warning_msg("gascheduler: exit ~p from ~p running=~p,"
                                     " pending=~p", [Reason, Worker,
                                                     length(Running),
                                                     queue:len(Pending)]),
            {stop, Reason, State}
    end;
handle_info({nodedown, NodeDown}, State = #state{nodes = Nodes}) ->
    error_logger:warning_msg("gascheduler: removing node ~p because it is down",
                             [NodeDown]),
    %% Note that nodedown messages for nodes can appear before EXIT messages
    %% from workers. Therefore, we can have a state where there are tasks still
    %% in the running queue but no nodes in the nodes list.
    {noreply, State#state{nodes = lists:delete(NodeDown, Nodes)}};
handle_info(Info, State) ->
    error_logger:warning_msg("gascheduler: unexpected message ~p", [Info]),
    {noreply, State}.

terminate(Reason, #state{running = Running, pending = Pending} = _State) ->
    case Reason =:= normal andalso Running =:= [] andalso queue:is_empty(Pending) of
        true ->
            %% No logging for normal shut down
            ok;
        false ->
            error_logger:warning_msg(
              "gascheduler: terminating with reason ~p and "
              "~p running tasks and ~p pending tasks",
              [Reason, length(Running), queue:len(Pending)]),
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%%% Internal functions
get_running_worker(From, RunningWorkers) ->
    case lists:keyfind(From, 1, RunningWorkers) of
        {_, MFA} ->
            {ok, MFA};
        false ->
            not_found
    end.

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
                 Node = node(Pid),
                 case orddict:find(Node, Acc) of
                    error -> Acc;
                    {ok, _Value} -> orddict:update_counter(Node, 1, Acc)
                 end
             end,
    Acc = orddict:from_list(lists:map(fun (Node) -> {Node, 0} end, Nodes)),
    NodeCount = lists:foldl(AccFun, Acc, Running),
    lists:keysort(2, orddict:to_list(NodeCount)).


-spec ping_nodes([node()]) -> [node()] | [].
ping_nodes(Nodes) ->
    lists:filter(fun(Node) when Node =:= node() -> true;
                    (Node) ->
                     case net_adm:ping(Node) of
                         pong -> true;
                         _    -> false
                     end
                 end,
                 Nodes).


-spec log_retry_wait(any(), any(), mfa(), non_neg_integer()) -> ok.
log_retry_wait(Type, Error, MFA, RetryTimeout) ->
    error_logger:warning_msg("gascheduler: caught ~p:~p in ~p -> retrying: stacktrace:~n~p",
                             [Type, Error, MFA, erlang:get_stacktrace()]),
    %% We wait here because otherwise we can spawn new Erlang processes faster
    %% than we clean them up.
    timer:sleep(RetryTimeout).


-spec log_permanent_failure(any(), any(), mfa()) -> ok.
log_permanent_failure(Type, Error, MFA) ->
    error_logger:error_msg("gascheduler: caught ~p:~p in ~p -> giving up, permanent failure",
                            [Type, Error, MFA]).


%% Executes MFA MaxRetries times
-spec execute_do(mfa(), non_neg_integer() | infinity, non_neg_integer()) -> result().
execute_do(_MFA, 0, _RetryTimeout) ->
    {error, max_retries};
execute_do(MFA = {Mod, Fun, Args}, infinity, RetryTimeout) ->
    try
        {ok, apply(Mod, Fun, Args)}
    catch
        throw:gascheduler_permanent_failure ->
            log_permanent_failure(throw, gascheduler_permanent_failure, MFA),
            {error, permanent_failure};
        Type:Error ->
            log_retry_wait(Type, Error, MFA, RetryTimeout),
            execute_do(MFA, infinity, RetryTimeout)
    end;
execute_do(MFA = {Mod, Fun, Args}, MaxRetries, RetryTimeout) ->
    try
        {ok, apply(Mod, Fun, Args)}
    catch
        throw:gascheduler_permanent_failure ->
            log_permanent_failure(throw, gascheduler_permanent_failure, MFA),
            {error, permanent_failure};
        Type:Error ->
            log_retry_wait(Type, Error, MFA, RetryTimeout),
            execute_do(MFA, MaxRetries - 1, RetryTimeout)
    end.

-spec worker_fun(pid(), mfa(), max_retries(), non_neg_integer()) -> ok.
worker_fun(Scheduler, MFA, MaxRetries, RetryTimeout) ->
    Worker = self(),
    Result = execute_do(MFA, MaxRetries, RetryTimeout),
    gascheduler:notify_client(Scheduler, Result, Worker, MFA).


%% Tries to execute MFA, otherwise queues MFA in pending.
-spec execute_try(mfa(), #state{}) -> #state{}.
execute_try(MFA, State = #state{nodes = Nodes,
                                pending = Pending,
                                running = Running,
                                max_workers = MaxWorkers,
                                max_retries = MaxRetries,
                                retry_timeout = RetryTimeout}) ->
    Scheduler = self(),
    case get_free_node(Nodes, MaxWorkers, Running) of
        undefined ->
            State#state{pending = queue:in(MFA, Pending)};
        Node ->
            Args = [Scheduler, MFA, MaxRetries, RetryTimeout],
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
    %% number of available slots.
    AvailableSlots = (MaxWorkers * length(Nodes)) - length(Running),
    %% see handle_cast({nodedown... for why this can be negative
    FreeWorkers = case AvailableSlots < 0 of
                      true -> 0;
                      false -> AvailableSlots
                  end,
    {Execute, KeepPending} = case FreeWorkers > queue:len(Pending) of
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
    ok = gen_server:cast(self(), tick),
    lists:keydelete(Worker, 1, Running).
