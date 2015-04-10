GameAnalytics Cluster Scheduler
-------------------------------

This library implements a generic scheduler for processing tasks in a cluster.
The generation and processing of tasks is specialized for a particular
application. For example, the gatransform application generates tasks by
reading files from S3. It processes them by reading relevant meta data from
DynamoDB and writes an annotated result to S3.

Tasks are controlled by a state machine depicted in the following picture.

```       
  execute({Mod, Fun, Args}) 
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
     success               |
        |         max retries exceeded
        |                  |
        |                  v
        |         {error, failed_max_retries_times}
        v
{ok, Result = apply(Mod, Fun, Args)}       
```

Only the states Pending and Running are represented inside the scheduler.
External events such as files being written to S3 trigger the implicit starting
state. Once tasks are finished they are no longer tracked by the scheduler.

A cluster consists of 2 or more nodes. The master node is the node on which the
scheduler is running. Nodes have worker processes available to execute tasks.
Typically the master node also has worker processes.
