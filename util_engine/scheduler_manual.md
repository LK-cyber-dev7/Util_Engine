# Scheduler Manual

## Overview

The Util Engine scheduler manages repeated execution of Python callables
with tracking and control features.

A scheduler consists of:

-   Branch
-   Tasks
-   Registers
-   Execution backend

## Execution Modes

### Threading (Completed)

Threading runs tasks using ThreadPoolExecutor.

Advantages: - Shared memory - Good for IO tasks - Simple integration

Example:

``` python
branch = ue.initiate_branch(
    "main",
    method="threading"
)
```

### Multiprocessing (Work in Progress)

Uses ProcessPoolExecutor.

Important: - Tasks must be pickleable - Memory is not shared between
processes

### Asyncio (Work in Progress)

Uses an asyncio event loop with task scheduling.

## Creating a Branch

``` python
branch = ue.initiate_branch(
    "branch_name",
    method="threading",
    mutable=True
)
```

Parameters:

-   name: branch identifier
-   method: threading / multiprocessing / asyncio
-   mutable: whether tasks can be changed while running
-   exit_on_completion: stop branch when tasks complete

## Adding Tasks

``` python
branch.assign_task(
    "task_name",
    function,
    interval,
    *args,
    config={}
)
```

Example:

``` python
def work(x):
    return x*x

branch.assign_task(
    "square",
    work,
    1,
    5,
    config={"target_runs": 10}
)
```

## Task Configuration

Available configuration options include:

-   target_runs
-   max_tries
-   max_fails
-   max_fail_streak
-   pass_rate
-   catch_up
-   strict_target
-   pause_on_error
-   priority

## Target Runs

`target_runs` controls successful executions required.

Example:

``` python
config={
    "target_runs": 5
}
```

The scheduler continues until enough successful executions occur.

## Strict Target

When enabled:

-   Scheduler prevents unnecessary overshooting
-   Running tasks are considered before launching more

When disabled:

-   Scheduler may start extra executions
-   Can improve throughput

## Starting the Scheduler

Blocking mode:

``` python
ue.start_loop(branch)
```

Background mode:

``` python
ue.start_loop(
    branch,
    background=True
)
```

## Getting Results

``` python
branch.get_results("task_name")
```

Returns stored task outputs.

## Getting Statistics

``` python
branch.get_stats("task_name")
```

Returns information such as:

-   runs
-   failures
-   retry streak

## Task Control

Available operations:

``` python
branch.pause_task(name)
branch.resume_task(name)
branch.terminate_task(name)
branch.complete_task(name)
branch.replace_task(name, function)
```

## Notes

Threading is the currently completed execution engine.

Multiprocessing and asyncio support exist in the architecture but are
still under development.
