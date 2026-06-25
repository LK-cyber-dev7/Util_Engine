"""
CustomIterable Package
-----------------------

A Python package providing a flexible, chainable, and memory-efficient
custom iterable class for numbers and sequences. 

Features include:
- Filtering based on numeric properties: primes, composites, powers, palindromes, digit sums, etc.
- Chainable filters for expressive queries.
- Lazy evaluation using generators for large sequences.
- Mapping, slicing, and selective element access.

Example usage:

    from custom_iterable import CustomIterable

    c = CustomIterable(range(100))
    primes_squared = c.is_prime().is_square()
    for n in primes_squared:
        print(n)

"""

# Standard metadata
__title__ = "custom_iterable"
__description__ = "Chainable, filterable, generator-based iterable class for numbers"
__url__ = "https://github.com/yourusername/custom_iterable"
__version__ = "0.1.0"
__author__ = "Lakshya"
__license__ = "MIT"
__copyright__ = "2026 Lakshya"

from .iterator_file import  CustomIterable
from .task_manager_file import start_asyncio_branch, start_loop, initiate_branch, run_async_scheduler, run_thread_scheduler, run_process_scheduler, async_task_runner, get_cluster, run_n_times
from .task_resource_file import Method, Status

# Optional: define __all__ for explicit exports
__all__ = ["CustomIterable","start_asyncio_branch","start_loop","run_async_scheduler","run_process_scheduler","initiate_branch","run_thread_scheduler","async_task_runner","get_cluster","Method","Status", "run_n_times"]