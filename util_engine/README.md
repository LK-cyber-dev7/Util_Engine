# Util Engine

Util Engine is a Python utility package containing two main components:

1.  **Scheduler Engine** - A task execution framework for running and
    managing repeated tasks.
2.  **Custom Iterable** - A lazy, chainable iterable system for
    filtering and transforming sequences.

The package focuses on making complex operations simple through
higher-level abstractions.

------------------------------------------------------------------------

# Features

## Scheduler Engine

The scheduler provides:

-   Task scheduling
-   Repeated execution
-   Result collection
-   Failure tracking
-   Retry and limit controls
-   Branch-based task management
-   Multiple execution backends

Execution backends:

-   Threading: Completed
-   Multiprocessing: Work in progress
-   Asyncio: Work in progress

## Custom Iterable

The iterable system provides:

-   Lazy evaluation
-   Chainable filters
-   Numeric property checks
-   Mapping
-   Slicing
-   Sequence transformations

It is designed for expressive operations without creating unnecessary
intermediate lists.

------------------------------------------------------------------------

# Installation

Import the package:

``` python
import util_engine as ue
```

------------------------------------------------------------------------

# Scheduler Example

Create a branch and assign a task:

``` python
import util_engine as ue

def square(x):
    return x * x

branch = ue.initiate_branch(
    "math_tasks",
    method="threading"
)

branch.assign_task(
    "square_task",
    square,
    0.5,
    5,
    config={
        "target_runs": 3
    }
)

ue.start_loop(branch)

print(branch.get_results("square_task"))
```

Output:

``` text
[25, 25, 25]
```

------------------------------------------------------------------------

# Quick Scheduler Execution

For simple repeated execution:

``` python
result = ue.run_n_times(
    "test",
    square,
    10,
    n=5
)

print(result)
```

------------------------------------------------------------------------

# Custom Iterable Example

Create a lazy iterable:

``` python
from util_engine import CustomIterable


numbers = CustomIterable(1, 101)

# prime numbers between 1 and 100 with digit sum equal to 11
result = numbers.is_prime().has_digit_sum(11) 

print(list(result))
# Output: [29, 47, 83]


# perfect squares between 1 and 2000, skipping those divisible by 3,
  keeping the first 5, and computing cumulative sum
perfect_squares = (
    CustomIterable(1, 2000)
    .is_square()
    .skip_divisible_by(3)
    .keep_first(5)
    .cum_sum()
)

print(list(perfect_squares))
# Output: [0, 1, 5, 21, 46]


abundant_nums = CustomIterable(1, 51).is_abundant()

print(list(abundant_nums))
# Output: [12, 18, 20, 24, 30, 36, 40, 42, 48]


# example of a custom mapping function to square numbers
processed = (
    CustomIterable(0, 50)
    .is_even()
    .keep_first(10)
    .map(lambda x: x ** 2)
    .is_palindromic()
)

print(list(processed))
# Output: [0, 4, 121, 484]


fib = CustomIterable.fibonacci(10)

print(list(fib))
# Output: [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]

# a custom sliding sequence generator for Tribonacci numbers
tribonacci = CustomIterable.sliding_seq(
    lambda x, y, z: x + y + z,
    7,
    0, 1, 1
)

print(list(tribonacci))
# Output: [2, 4, 7, 13, 24, 44, 81]
```

The operations are chained and evaluated lazily.

------------------------------------------------------------------------

# Package Overview

## Scheduler

Main objects:

-   Branch
-   Task
-   Registers

Useful operations:

``` python
branch.pause_task(name)
branch.resume_task(name)
branch.get_results(name)
branch.get_stats(name)
```

## Custom Iterable

Supports operations such as:

``` python
iterable.is_prime()
iterable.is_square()
iterable.filter(func)
iterable.map(func)
```

------------------------------------------------------------------------

# Development Status

The current stable feature is:

-   Custom Iterable
-   Threading scheduler backend

In development:

-   Multiprocessing scheduler backend
-   Asyncio scheduler backend

------------------------------------------------------------------------

# License

MIT License
