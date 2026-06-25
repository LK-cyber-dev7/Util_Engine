import concurrent
import threading
import multiprocessing
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Literal, Callable, Any

from .task_resource_file import Branch, Method, Task, MutableRegister, ImmutableRegister
from .error_manager_file import SchedulerOverloadError
import time

branch_cluster:dict[str,Branch] = {}
TICK: float = 0.05
async_engine_started = False
async_engine_loop = None
async_branches: dict[str, asyncio.Task] = {}

async def _async_engine_manager():
    while True:
        # remove finished branches
        finished = [name for name, task in async_branches.items() if task.done()]
        for name in finished:
            del async_branches[name]

        await asyncio.sleep(0.05)

def _start_async_engine_thread():
    global async_engine_started, async_engine_loop

    async_engine_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(async_engine_loop)
    async_engine_started = True

    async_engine_loop.run_until_complete(_async_engine_manager())

def start_asyncio_branch(name: str):
    global async_engine_started, async_engine_loop

    if not async_engine_started:
        threading.Thread(
            target=_start_async_engine_thread,
            daemon=True
        ).start()

        while async_engine_loop is None:
            time.sleep(0.01)

    fut = asyncio.run_coroutine_threadsafe(
        run_async_scheduler(name),
        async_engine_loop
    )

    async_branches[name] = fut

    return fut

def initiate_branch(name: str, method: Literal['multiprocessing','threading','asyncio'], mutable: bool =False, exit_on_completion: bool = True) -> Branch:
    tmp = Branch(name, method, mutable, exit_on_completion)
    branch_cluster[name] = tmp
    return tmp

def get_cluster() -> dict:
    return branch_cluster

def run_thread_scheduler(name):
    register: MutableRegister | ImmutableRegister = branch_cluster[name]._get_register()
    futures:dict[concurrent.futures.Future,Task] = {}
    with ThreadPoolExecutor() as executor:
        while branch_cluster[name].is_running():
            start = time.monotonic()
            for i in register:
                current = time.monotonic()
                if current - i.last_run_time >= i.interval:

                    if i._running + i.stats['runs'] >= i.max_tries or (i._running + i.stats['runs'] - i.stats['failures'] >= i.target_runs and i.strict_target):
                        continue

                    try:
                        future = executor.submit(i.work, *i.args, **i.kwargs)
                        i._running += 1
                    except RuntimeError:
                        raise SchedulerOverloadError(f'Running tasks overloaded ThreadPoolExecutor. Branch name {name}')
                    else:
                        if i.catch_up:
                            i.last_run_time += i.interval
                        else:
                            i.last_run_time = current

                        futures[future] = i
            for f, t in futures.copy().items():
                if f.done():
                    t._running -= 1
                    if f.exception():
                        register.task_failure(t.task_name)
                    else:
                        register.task_success(t.task_name, f.result())
                    del futures[f]

            if register.check_completion() and branch_cluster[name].exit_on_completion:
                branch_cluster[name]._stop_run()

            elapsed: float = time.monotonic() - start
            time.sleep(max(0, TICK - elapsed))

def run_process_scheduler(name):
    register: MutableRegister | ImmutableRegister = branch_cluster[name]._get_register()
    futures: dict[concurrent.futures.Future, Task] = {}
    with ProcessPoolExecutor() as executor:
        while branch_cluster[name].is_running():
            start = time.monotonic()
            for i in register:
                current = time.monotonic()
                if current - i.last_run_time >= i.interval:

                    if i._running + i.stats['runs'] >= i.max_tries or (i._running + i.stats['runs'] - i.stats['failures'] >= i.target_runs and i.strict_target):
                        continue

                    try:
                        future = executor.submit(i.work, *i.args, **i.kwargs)
                        i._running += 1
                    except RuntimeError:
                        raise SchedulerOverloadError(f'Running tasks overloaded ThreadPoolExecutor. Branch name {name}')
                    else:
                        if i.catch_up:
                            i.last_run_time += i.interval
                        else:
                            i.last_run_time = current

                        futures[future] = i
            for f, t in futures.copy().items():
                if f.done():
                    i._running -= 1
                    if f.exception():
                        register.task_failure(t.task_name)
                    else:
                        register.task_success(t.task_name, f.result())
                    del futures[f]

            if register.check_completion() and branch_cluster[name].exit_on_completion:
                branch_cluster[name]._stop_run()

            elapsed: float = time.monotonic() - start
            time.sleep(max(0, TICK - elapsed))

async def async_task_runner(task,register,executor):
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            executor,
            task.work,
            *task.args,
            **task.kwargs
        )
    except Exception:
        register.task_failure(task.task_name)
    else:
        register.task_success(task.task_name, result)

async def run_async_scheduler(name):
    register: MutableRegister | ImmutableRegister = branch_cluster[name]._get_register()
    executor: ThreadPoolExecutor = ThreadPoolExecutor()
    while branch_cluster[name].is_running():
        start = time.monotonic()
        for i in register:
            current = time.monotonic()
            if current - i.last_run_time >= i.interval:
                try:
                    asyncio.create_task(async_task_runner(i, register, executor))
                except RuntimeError:
                    raise SchedulerOverloadError(f'Running tasks overloaded ThreadPoolExecutor. Branch name {name}')
                else:
                    if i.catch_up:
                        i.last_run_time += i.interval
                    else:
                        i.last_run_time = current

        elapsed: float = time.monotonic() - start
        await asyncio.sleep(max(0, TICK - elapsed))
    executor.shutdown(wait=True)

def start_loop(branch: Branch, background: bool = False) -> None:
    branch._start_run()
    if branch.get_method() == Method.MULTIPROCESSING:
        try:
            tmp = multiprocessing.Process(
                target=run_process_scheduler,
                args=(branch.name,)
            )
            tmp.start()

            if not background:
                tmp.join()

        except SchedulerOverloadError as e:
            branch._error = e
            branch._kill()

    elif branch.get_method() == Method.THREADING:
        try:
            tmp = threading.Thread(
                target=run_thread_scheduler,
                args=(branch.name,)
            )
            tmp.start()

            if not background:
                tmp.join()

        except SchedulerOverloadError as e:
            branch._error = e
            branch._kill()

    elif branch.get_method() == Method.ASYNCIO:
        try:
            fut = start_asyncio_branch(branch.name)

            if not background:
                fut.result()

        except SchedulerOverloadError as e:
            branch._error = e
            branch._kill()

def run_n_times(name:str, task: Callable, *args, interval: int | float = 0.1, n: int = 1, method: Literal['multiprocessing','threading','asyncio'] = 'threading', **kwargs):
    tmp = initiate_branch(f"__default_{name}", method=method)
    tmp.assign_task(name, task, interval,config={"target_runs":n}, *args, **kwargs)
    start_loop(tmp)
    return tmp.get_results(name)
