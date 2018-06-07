GameAnalytics Cluster Scheduler
-------------------------------

[![Build Status](https://travis-ci.org/GameAnalytics/gascheduler.svg?branch=master)](https://travis-ci.org/GameAnalytics/gascheduler)

This library implements a generic scheduler for processing tasks in a cluster.
The generation and processing of tasks is specialized for a particular
application. The client passes a callback to execute and a message is returned
indicating the status of the task.

Tasks are controlled by a state machine depicted in the following diagram.

```
  execute(SchedulerName, MFA = {Mod, Fun, Args})
             |
             |
             v
    .--->[ Pending ]---.
    |                  |
node down         spawn worker
    |                  |
    `---[ Running ]<---'-----------.
        |         |                 |
        |     exception           retry
        |         |                 |
        |         `--->[ Failed ]---'
     success           |        |
        |        max retries  MFA called
        |         exceeded    throw(gascheduler_permanent_failure)
        |              |        |
        |              v        |
        |  {error, max_retries} |
        |                       v
        |                   {error, permanent_failure}
        v
{ok, Result = apply(Mod, Fun, Args)}
```

Only the states Pending and Running are represented inside the scheduler.
Retries are handled on the worker nodes. Once tasks have finished they are no
longer tracked by the scheduler.

A cluster consists of one or more nodes. Nodes have worker processes available
to execute tasks. Typically the master node also has worker processes.

Example Usage
-------------
To start the scheduler some configuration is required to be passed in.

```
    %% Each gascheduler has its own name. There can be multiple gaschedulers.
    Name = test,

    %% A list of nodes to execute work on. See also erlang:nodes().
    Nodes = [...],

    %% Maximum number of workers per node.
    MaxWorkers = 10,

    %% Maximum number of retries for a worker, i.e. it throws some exception.
    MaxRetries = 10,

    %% Where to send scheduler status messages to.
    Client = self(),

    %% Start the scheduler.
    {ok, _} = gascheduler:start_link(Name, Nodes, Client, MaxWorkers, MaxRetries),
```

We can then run a task on the scheduler.

```
    %% Execute hello:world(1) asynchronously.
    %% In the hello module exists, world(N) -> N.
    ok = gascheduler:execute(Name, {hello, world, [1]}),
```

We are notified of task status asynchronously.

```
    %% Receive a single status message from a particular scheduler.
    receive
        {Name, {ok, Result}, Node, MFA = {Mod, Fun, Args}} ->
            io:format(“hello world ~p from ~p~n”, [Result, Node]);
        {Name, {error, Reason}, Node, MFA = {Mod, Fun, Args}} ->
            io:format(“task ~p failed on ~p because ~p”, [MFA, Node, Reason])
    end
```

When the task completes successfully.

```
    hello world 1 from slave1@worker1
```

Possible failure cases.

```
    task {hello, world, [1]} failed on slave1@worker1 because max_retries
    task {hello, world, [1]} failed on slave1@worker1 because permanent_failure
```

Caveats
-------

The scheduler is a single master without redundancy. If the master is
unavailable the entire system is unavailable.

The callback being executed by the scheduler may be executed more than once
if communication is lost to a node but is still running. In this case the
scheduler will receive a node down message due to loss of communication. It will
then reschedule the task on another available node. However, the task may
continue running on the node which has lost communication. Therefore, the
callback has to be safe if it is run more than once.

Do we actually use this code?
-----------------------------

The gascheduler code is in production use at Game Analytics. We definitely find
it useful.

Slides
------

You can find slides about gascheduler
[here](http://www.slideshare.net/cmmdevries/erlang-meetup-gascheduler).
This was presented at the
[Erloung Berlin Erlang Meetup](http://www.meetup.com/erlounge-berlin/).
We open sourced the project at the end of the presentation.

License
-------

The gascheduler library is licensed under the MIT license.
