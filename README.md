GameAnalytics Cluster Scheduler
-------------------------------

This library implements a generic scheduler for processing tasks in a cluster.
The generation and processing of tasks is specialized for a particular
application.

Tasks are controlled by a state machine depicted in the following picture.

```
  execute(MFA = {Mod, Fun, Args})
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
External events such as files being written trigger the implicit starting
state. Once tasks are finished they are no longer tracked by the scheduler.

A cluster consists of 2 or more nodes. Nodes have worker processes available to
execute tasks.  Typically the master node also has worker processes.
