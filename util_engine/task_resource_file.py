import functools
import multiprocessing
import pickle
import threading
import asyncio
import time
import enum
from collections.abc import Callable
from typing import Literal
from .error_manager_file import SchedulerBranchError,TaskNotFoundError, InvalidConfigError
from sortedcontainers import SortedList

class Status(enum.Enum):
    ACTIVE = 'A'
    PAUSED = 'P'
    RETRYING = 'R'
    TERMINATED = 'T'
    ERROR = 'E'
    COMPLETED = 'C'
    FAILED = 'F'

class Method(enum.Enum):
    THREADING = 'threading'
    MULTIPROCESSING = 'multiprocessing'
    ASYNCIO = 'asyncio'

def flag_verify(value: bool) -> bool:
    return isinstance(value, bool)

def numerical_verify(value: int | float) -> bool:
    if isinstance(value, bool):
        return False
    if value == float('inf'):
        return True
    if isinstance(value, int) and value > 0:
        return True
    return False

def time_verify(value: int | float) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    if value <= 0 or value == float('inf'):
        return False
    return True

def pass_rate_verify(value: int | float) -> bool:
    if isinstance(value, bool):
        return False
    elif isinstance(value, int) and 1 >= value >= 0:
        return True
    elif isinstance(value, float) and 1 >= value >= 0:
        return True
    return False

def priority_verify(value: int | float) -> bool:
    if isinstance(value, bool):
        return False
    elif value == float('inf') or value == float('-inf'):
        return True
    elif isinstance(value, int):
        return True
    return False

class Task:

    id_counter: int = 0

    def __init__(self, task_name:str, work, interval: int | float, *args, priority:int=float('inf'), paused:bool=False, max_fails:int=100, pause_on_error:bool=False, catch_up:bool=False, strict_target:bool=True, max_fail_streak:int=10, max_tries:int = float('inf'), target_runs:int = float('inf'),pass_rate:float = 0.5, **kwargs) -> None:
        self.task_name:str = task_name

        self.id: int = Task.id_counter
        Task.id_counter += 1

        self.args = args
        self.kwargs = kwargs

        self.last_run_time = time.monotonic()

        if callable(work):
            self.work = work
        else:
            raise TypeError('work must be callable')

        if time_verify(interval):
            self.interval = interval
        else:
            raise ValueError('INVALID INTERVAL')

        if priority_verify(priority):
            self.priority = priority
        else:
            raise ValueError('INVALID PRIORITY')

        if paused:
            self.status: Status = Status.PAUSED
        else:
            self.status: Status = Status.ACTIVE

        if numerical_verify(max_tries):
            self.max_tries = max_tries
        else:
            raise ValueError('INVALID MAX_RUNS')

        if numerical_verify(target_runs):
            self.target_runs = target_runs
        else:
            raise ValueError('INVALID TARGET_RUNS')

        if numerical_verify(max_fails):
            self.max_fails = max_fails
        else:
            raise ValueError('INVALID MAX_FAILS')

        if numerical_verify(max_fail_streak):
            self.max_fail_streak = max_fail_streak
        else:
            raise ValueError('INVALID MAX_FAILS_STREAK')

        if pass_rate_verify(pass_rate):
            self.pass_rate: float = pass_rate
        else:
            raise ValueError('INVALID PASS_RATE')

        if flag_verify(catch_up):
            self.catch_up: bool = catch_up
        else:
            raise ValueError('INVALID CATCHUP')

        if flag_verify(pause_on_error):
            self.pause_on_error: bool = pause_on_error
        else:
            raise ValueError('INVALID PAUSE ON ERRR') # easter egg intentional spelling error

        if flag_verify(strict_target):
            self.strict_target: bool = strict_target
        else:
            raise ValueError('INVALID STRICT TARGET')

        self.results = []
        self.stats:dict[str,float | int] = {
            'failures':0,
            'runs':0,
            'retry_streak':0,
        }
        self._running = 0

    def is_alive(self) -> bool:
        return self.status != Status.TERMINATED and self.status != Status.COMPLETED and self.status != Status.FAILED

    def pause_task(self) -> None:
        """
        Pause this task.

        The scheduler will no longer run the task until `resume_task()` is called.
        Does not reset internal statistics.

        Effects:
            - Marks the task as paused.
        """
        if self.is_alive():
            self.status = Status.PAUSED

    def resume_task(self) -> None:
        """
        Resume this task if previously paused.

        Allows the scheduler to begin scheduling this task again from the next tick.
        """
        if self.is_alive():
            self.status = Status.ACTIVE

    def terminate_task(self) -> None:
        """
          Permanently stop this task.

          The task will no longer run, cannot be resumed, and will be treated
          as finished by the scheduler. Used when max failures, streak, or tries
          have been exceeded, or when manually terminated.
        """
        self.status = Status.TERMINATED # needs to be updated when register class is set up. WORK IN PROGRESS

    def complete_task(self) -> None:
        """
        Mark this task as successfully completed.

        Once completed, the task becomes inactive and is removed from future
        scheduling. This is typically triggered when the task reaches its
        target run count with sufficient success rate.
        """
        self.status = Status.COMPLETED

    def change_priority(self,priority:int) -> None:
        """
        Update the task's scheduling priority.

        Lower values represent higher scheduling priority.
        The register will automatically reinsert the task into the sorted list.
        """
        if self.is_alive():
            if priority_verify(priority):
                self.priority = priority
            else:
                raise ValueError('priority must be positive')

    def set(self, interval=None, max_fails=None, pause_on_error=None, catch_up=None, max_fail_streak=None, max_tries=None,target_runs=None) -> None:
        """
        Update selected configuration fields for this task.

        Any parameter not provided or failing validation is ignored.
        After updating, the task automatically checks whether failure/
        termination criteria have been reached.
        """
        self.interval = interval if time_verify(interval) else self.interval
        self.max_fails = max_fails if numerical_verify(max_fails) else self.max_fails
        self.max_tries = max_tries if numerical_verify(max_tries) else self.max_tries
        self.target_runs = target_runs if numerical_verify(target_runs) else self.target_runs
        self.max_fail_streak = max_fail_streak if numerical_verify(max_fail_streak) else self.max_fail_streak
        self.pause_on_error = pause_on_error if flag_verify(pause_on_error) else self.pause_on_error
        self.catch_up = catch_up if flag_verify(catch_up) else self.catch_up
        if self.is_alive():
            self.check_failures()

    def replace(self,work,*args,**kwargs) -> None:
        """
        Replace the callable associated with the task.

        Parameters:
            work (callable): New function to run.
            *args, **kwargs: Arguments passed to the function.

        Raises:
            TypeError: If work is not callable.
        """
        if callable(work):
            self.work = work
        else:
            raise TypeError('work must be callable')
        self.args = args
        self.kwargs = kwargs

    def is_active(self) -> bool:
        """
        Return True if the task is active and eligible to be run.

        Active means:
            - Not paused
            - Not permanently disabled
        """
        self.check_failures()
        return self.status in {Status.RETRYING, Status.ACTIVE}

    def failed(self) -> None:
        """
        Register a failure for this task.

        Effects:
            - Increments failure counters.
            - Updates retry streak.
            - Switches status to ERROR or RETRYING.
            - Triggers failure‐policy evaluation.
        """
        if self.is_alive():
            self.stats['failures'] += 1
            self.stats['runs'] += 1
            self.stats['retry_streak'] += 1
            if self.pause_on_error:
                self.status = Status.ERROR # the task resumes when resumed by user so it gets reset to active in that func
            else:
                self.status = Status.RETRYING
            self.check_failures()

    def succeeded(self,result) -> None:
        """
        Register a successful run of the task.

        Effects:
            - Increments run counter.
            - Appends result to history.
            - Resets retry streak (if any).
            - Triggers completion evaluation.
        """
        if self.is_alive():
            self.stats['runs'] += 1
            self.results.append(result)
            if self.status == Status.RETRYING:
                self.stats['retry_streak'] = 0
                self.status = Status.ACTIVE
            self.check_failures()

    def check_failures(self) -> None:
        """
        Evaluate whether this task should be completed, failed, or terminated.

        This method enforces:
            - Target run logic
            - Pass rate requirement
            - Max failure limit
            - Max retry streak
            - Max tries limit
        """
        if self.is_alive():
            if self.stats['runs'] - self.stats['failures'] >= self.target_runs:
                suc_rate = (self.stats['runs'] - self.stats['failures']) / self.stats['runs']
                if suc_rate >= self.pass_rate:
                    self.status = Status.COMPLETED
                else:
                    self.status = Status.FAILED
            elif self.stats['failures'] >= self.max_fails:
                self.terminate_task()
            elif self.stats['retry_streak'] >= self.max_fail_streak:
                self.terminate_task()
            elif self.stats['runs'] >= self.max_tries:
                self.terminate_task()

    def revive(self, yes=False) -> None:
        """
        Reset this task to a clean initial state.

        Parameters:
            yes (bool): If True, revive even if the task was terminated.

        Effects:
            - Resets statistics
            - Resets status to ACTIVE
            - Resets last run time
        """
        if not self.is_alive() or yes:
            self.stats: dict[str, float | int] = {
                'failures': 0,
                'runs': 0,
                'retry_streak': 0,
            }
            self.last_run_time = time.monotonic()
            self.status = Status.ACTIVE

class MutableRegister:

    id_counter: int = 0

    def __init__(self, name: str, lock):
        self.name:str = name
        self.lock = lock
        self.id: int = MutableRegister.id_counter
        MutableRegister.id_counter += 1
        self.data:dict[str, Task] = {}
        self.active_tasks = SortedList(key=lambda t: (t.priority, t.id))

    def add(self, task:Task) -> None:
        """
        Add a task to the register.

        Parameters:
            task (Task): The task instance to add.
        """
        with self.lock:
            self.data[task.task_name] = task
            if task.is_active():
                self.active_tasks.add(task)

    def remove_task(self, name: str) -> None:
        """
        Remove a task from the register.

        Parameters:
            name (str): Name of the task to remove.
        """
        if name in self.data:
            with self.lock:
                if self.data[name] in self.active_tasks:
                    self.active_tasks.remove(self.data[name])
                del self.data[name]

    def check_task(self, name: str) -> None:
        """
        Reevaluate the task’s active state and update the sorted list.

        Ensures that when a task changes status or priority, the internal
        priority queue correctly reflects its new state.
        """
        task = self.data[name]
        with self.lock:
            if task in self.active_tasks:
                self.active_tasks.remove(task)
            if task.is_active():
                self.active_tasks.add(task)


    def set_config(self, name:str, interval=None, max_fails=None, pause_on_error: bool=None, catch_up: bool=None, max_fail_streak=None, max_tries=None,target_runs=None) -> None:
        """
        Update configuration for a specific task.

        After modification, the task is rechecked so that scheduling order
        remains accurate.
        """
        if name in self.data:
            self.data[name].set(interval=interval,max_fails=max_fails,pause_on_error=pause_on_error,catch_up=catch_up,max_fail_streak=max_fail_streak,max_tries=max_tries,target_runs=target_runs)
            self.check_task(name)

    def set_priority(self, name:str, priority:int) -> None:
        """
        Update the scheduling priority of a task.

        After updating, the task is repositioned in the internal sorted list.
        """
        if name in self.data:
            self.data[name].change_priority(priority)
            self.check_task(name)

    def revive_task(self, name: str) -> None:
        """
        Reset a task to its initial state and reintegrate it into the scheduler.
        """
        if name in self.data:
            self.data[name].revive()
            self.check_task(name)

    def pause_task(self, name: str) -> None:
        """
        Pause a task inside this register and refresh its scheduling state.
        """
        if name in self.data:
            self.data[name].pause_task()
            self.check_task(name)

    def terminate_task(self, name: str) -> None:
        """
        Permanently terminate a task inside this register.
        """
        if name in self.data:
            self.data[name].terminate_task()
            self.check_task(name)

    def complete_task(self, name: str) -> None:
        """
        Mark a task as completed and update its presence in the active list.
        """
        if name in self.data:
            self.data[name].complete_task()
            self.check_task(name)

    def replace_task(self, name: str,work: Callable,*args,**kwargs) -> None:
        """
        Replace the callable associated with a task stored in this register.
        """
        if name in self.data:
            self.data[name].replace(work,*args,**kwargs)

    def resume_task(self, name: str) -> None:
        """
        Resume a paused task and update the active-task ordering.
        """
        if name in self.data:
            self.data[name].resume_task()
            self.check_task(name)

    def task_success(self, name:str, result) -> None:
        """
        Handle successful completion of a task.

        Parameters:
            name (str): Name of the task that succeeded.
            result: Result of the task.

        Effects:
            - Increments success counter.
            - Updates per-task statistics.
            - Rearranges internal ordering if priority or interval changed.
        """
        if name in self.data:
            self.data[name].succeeded(result)
            self.check_task(name)

    def task_failure(self, name:str) -> None:
        """
        Handle failure of a task.

        Parameters:
            name (str): Name of the failed task.

        Effects:
            - Increments error counter.
            - Applies failure policy (pause / disable / retry logic).
            - Updates priority if your design uses dynamic priority.
        """
        if name in self.data:
            self.data[name].failed()
            self.check_task(name)

    def get_task(self, name:str) -> Task:
        """
        Retrieve a task by name.

        Returns:
            Task | None: The task instance, or None if not found.
        """
        return self.data.get(name)

    def restart(self) -> None:
        """
        Revive every task in the register, resetting all statistics.
        """
        with self.lock:
            for i in self.data.values():
                i.revive(yes=True)

    def check_completion(self) -> bool:
        return len(self.active_tasks) == 0

    def __iter__(self):
        """
        Iterate over active tasks in the correct scheduling order.

        Yield:
            Task: One task at a time, according to the internal sorted order.

        Notes:
            The scheduler relies on this iteration for task scanning.
        """
        with self.lock:
            return iter([t for t in self.active_tasks if t.is_active()])

    def __contains__(self, item: Task) -> bool:
        return item.task_name in self.data

class ImmutableRegister:

    id_counter: int = 0

    def __init__(self, name: str):
        self.name:str = name
        self.id: int = MutableRegister.id_counter
        self.locked:bool  = False
        MutableRegister.id_counter += 1
        self.data:dict[str, Task] = {}
        self.active_tasks = SortedList(key=lambda t: (t.priority, t.id))

    def lock_register(self) -> None:
        """
        Prevent further structural modifications to this register.

        Once locked, tasks cannot be added, removed, or changed.
        """
        self.locked = True

    def unlock_register(self) -> None:
        """
        Allow modifications to this register again.
        """
        self.locked = False

    def add(self, task:Task) -> None:
        """
        Add a task to the register.

        Parameters:
            task (Task): The task instance to add.

        Raises:
            ValueError: If a task with the same name already exists.
        """
        if not self.locked:
            self.data[task.task_name] = task
            if task.is_active():
                self.active_tasks.add(task)

    def remove_task(self, name: str) -> None:
        """
        Remove a task from the register.

        Parameters:
            name (str): Name of the task to remove.
        """
        if not self.locked:
            if name in self.data:
                if self.data[name] in self.active_tasks:
                    self.active_tasks.remove(self.data[name])
                del self.data[name]

    def check_task(self, name: str) -> None:
        """
        Update internal active task ordering if the register is unlocked.
        """
        if not self.locked:
            task = self.data[name]
            if task in self.active_tasks:
                self.active_tasks.remove(task)
            if task.is_active():
                self.active_tasks.add(task)

    def set_config(self, name:str, interval=None, max_fails=None, pause_on_error: bool=None, catch_up: bool=None, max_fail_streak=None, max_tries=None,target_runs=None) -> None:
        """
        Update configuration for a specific task if register is not locked

        After modification, the task is rechecked so that scheduling order
        remains accurate.
        """
        if not self.locked and name in self.data:
            self.data[name].set(interval=interval,max_fails=max_fails,pause_on_error=pause_on_error,catch_up=catch_up,max_fail_streak=max_fail_streak,max_tries=max_tries,target_runs=target_runs)
            self.check_task(name)

    def set_priority(self, name:str, priority:int) -> None:
        """
        Update the scheduling priority of a task if register is not locked.

        After updating, the task is repositioned in the internal sorted list.
        """
        if not self.locked and name in self.data:
            self.data[name].change_priority(priority)
            self.check_task(name)

    def revive_task(self, name: str) -> None:
        """
        Works only if register is not locked.
        Reset a task to its initial state and reintegrate it into the scheduler.
        """
        if not self.locked and name in self.data:
            self.data[name].revive()
            self.check_task(name)

    def pause_task(self, name: str) -> None:
        """
        Works only if register is not locked.
        Pause a task inside this register and refresh its scheduling state.
        """
        if not self.locked and name in self.data:
            self.data[name].pause_task()
            self.check_task(name)

    def terminate_task(self, name: str) -> None:
        """
        Works only if register is not locked.
        Permanently terminate a task inside this register.
        """
        if not self.locked and name in self.data:
            self.data[name].terminate_task()
            self.check_task(name)

    def complete_task(self, name: str) -> None:
        """
            Works only if register is not locked.
            Mark a task as completed and update its presence in the active list.
        """
        if not self.locked and name in self.data:
            self.data[name].complete_task()
            self.check_task(name)

    def replace_task(self, name: str,work: Callable,*args,**kwargs) -> None:
        """
        Works only if register is not locked.
        Replace the callable associated with a task stored in this register.
        """
        if not self.locked and name in self.data:
            self.data[name].replace(work,*args,**kwargs)

    def resume_task(self, name: str) -> None:
        """
        Works only if register is not locked.
        Resume a paused task and update the active-task ordering.
        """
        if not self.locked and name in self.data:
            self.data[name].resume_task()
            self.check_task(name)

    def __check_for_task_state(self, name:str) -> None:
        """
        Internal helper that synchronizes a task’s state with the active list.

        Used only for success/failure updates, where immutable rules allow
        state changes but not structural edits.
        """
        task = self.data[name]
        active = task.is_active()
        in_list = task in self.active_tasks
        if active and not in_list:
            self.active_tasks.add(task)
        elif not active and in_list:
            self.active_tasks.remove(task)

    def task_success(self, name:str, result) -> None:
        if name in self.data:
            self.data[name].succeeded(result)
            self.__check_for_task_state(name)

    def task_failure(self, name:str) -> None:
        if name in self.data:
            self.data[name].failed()
            self.__check_for_task_state(name)

    def get_task(self, name:str) -> Task:
        return self.data.get(name)

    def restart(self) -> None:
        """
        Works only if register is not locked.
        Revive every task in the register, resetting all statistics.
        """
        if not self.locked:
            for i in self.data.values():
                i.revive(yes=True)

    def check_completion(self) -> bool:
        return len(self.active_tasks) == 0

    def __iter__(self):
        """
        Iterate over active tasks in priority order.

        Immutable registers produce a static but filtered view based on
        current task status.
        """
        return iter([t for t in self.active_tasks if t.is_active()])

    def __contains__(self, item: Task) -> bool:
        return item.task_name in self.data

def verify_name_status(action_name="do something"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, name, *args, **kwargs):
            if name not in self.__names:
                raise TaskNotFoundError(f"User tried to {action_name} with INVALID name")

            if self.__running.is_set and not self.__mutable:
                raise SchedulerBranchError(
                    f"User tried to {action_name} while branch was running. Branch is Immutable can not {action_name}."
                )
            return func(self, name, *args, **kwargs)
        return wrapper
    return decorator

class State(enum.Enum):
    ALIVE = 1
    CRASHED = 0

class Branch:
    __id_counter: int = 0
    def __init__(self, name: str, method: Literal['multiprocessing','threading','asyncio'], mutable=False, exit_on_completion:bool=True) -> None:
        self.name:str = name
        self.exit_on_completion:bool = exit_on_completion
        self.__mutable:bool = mutable
        self.__id:int = Branch.__id_counter
        self.__state: State = State.ALIVE
        self._error = None
        Branch.__id_counter += 1

        if method not in {'multiprocessing','threading','asyncio'}:
            raise ValueError('method must be one of "multiprocessing","threading","asyncio"')
        elif method == 'multiprocessing' and mutable:
            raise SchedulerBranchError("User tried to create a mutable multiprocessing Branch")
        __lock = threading.Lock() if method == "threading" else asyncio.Lock() if method == "asyncio" else None
        if mutable:
            self.__register = MutableRegister(name,__lock)
        else:
            self.__register = ImmutableRegister(name)

        self.__running = threading.Event() if method == "threading" else asyncio.Event() if method == "asyncio" else multiprocessing.Event()
        self.__names:set[str] = set()
        self.__method:Method = Method.THREADING if method == "threading" else Method.ASYNCIO if method == "asyncio" else Method.MULTIPROCESSING

    def assign_task(self, name:str, work: Callable, interval: int | float, *args, config:dict = None, **kwargs) -> None:
        """
        Create and assign a new task to this branch.

        Restrictions:
            - Immutable branches cannot receive tasks while running.
            - Multiprocessing branches require all arguments to be pickleable.
        """
        if self.__running.is_set() and not self.__mutable:
            raise SchedulerBranchError(
                f"User tried to assign task while branch was running. Branch is Immutable can not assign task."
            )

        if not callable(work):
            raise TypeError(
                f"User tried to assign task with INVALID work. Work must be a python callable."
            )

        if self.__method == 'multiprocessing':
            try:
                pickle.dumps((work, args, kwargs))
            except:
                raise SchedulerBranchError(f"User tried to assign task with INVLAID work.Branch uses multiprocessing, work must be pickleable.")

        config = config if config is not None else {}

        try:
            t = Task(name, work, interval, *args, **kwargs, **config)
        except TypeError:
            raise InvalidConfigError(f"User tried to assign task with INVALID config")

        self.__register.add(t)
        self.__names.add(t.task_name)

    def _start_run(self) -> None:
        """
        Sets the is_running event and locks the register if branch is immutable.
        """
        self.__running.set()
        if not self.__mutable:
            self.__register.lock_register()

    def _stop_run(self) -> None:
        """
        Clears the is_running event and unlocks the register if branch is immutable.
        """
        self.__running.clear()
        if not self.__mutable:
            self.__register.unlock_register()

    @verify_name_status('remove task')
    def remove_task(self, name:str) -> None:
        """
        Remove an existing task from this branch.
        """
        self.__register.remove_task(name)
        self.__names.remove(name)

    @verify_name_status('pause task')
    def pause_task(self, name:str) -> None:
        """
        Pause a task assigned to this branch.
        """
        self.__register.pause_task(name)

    @verify_name_status('terminate task')
    def terminate_task(self, name:str) -> None:
        """
        Permanently terminate a task in this branch.
        """
        self.__register.terminate_task(name)

    @verify_name_status('complete task')
    def complete_task(self, name:str) -> None:
        """
        Mark a task in this branch as completed.
        """
        self.__register.complete_task(name)

    @verify_name_status('replace task')
    def replace_task(self, name:str,work:Callable,*args,**kwargs) -> None:
        """
        Replace the callable for a task assigned to this branch.
        """
        if not callable(work):
            raise TypeError(
                f"User tried to replace task with INVALID work. Work must be a python callable"
            )

        self.__register.replace_task(name,work,*args,**kwargs)

    @verify_name_status('resume task')
    def resume_task(self, name:str) -> None:
        """
        Resume a paused task inside this branch.
        """
        self.__register.resume_task(name)

    @verify_name_status('revive task')
    def revive_task(self, name:str) -> None:
        """
        Reset a task to its initial state.
        """
        self.__register.revive_task(name)

    @verify_name_status('change task configuration')
    def change_config(self, name,config:dict) -> None:
        """
        Update the configuration of a task in this branch.

        Raises:
            InvalidConfigError: If configuration keys or types are invalid.
        """
        try:
            self.__register.set_config(name,**config)
        except TypeError:
            raise InvalidConfigError(f"User tried to change task configuration with INVALID config")

    @verify_name_status('change task priority')
    def change_task_priority(self,name,priority:int) -> None:
        """
        Update the priority of a task inside this branch.
        """
        if not priority_verify(priority):
            raise InvalidConfigError(f"User tried to change task priority with INVALID priority")

        self.__register.set_priority(name,priority)

    def is_running(self) -> bool:
        """
        Return True if the branch is currently active (event set).
        """
        return self.__running.is_set()

    def get_id(self) -> int:
        """
        Return the unique identifier of this branch.
        """
        return self.__id

    def is_mutable(self) -> bool:
        """
        Return True if this branch uses a mutable register.
        """
        return self.__mutable

    def get_method(self) -> Method:
        """
        Return the execution method used by this branch.

        Returns:
            Method: THREADING, MULTIPROCESSING, or ASYNCIO.
        """

        return self.__method

    def _get_register(self) -> ImmutableRegister | MutableRegister:
        """
        Internal accessor returning the underlying register object.
        """
        return self.__register

    def crashed(self) -> bool:
        """
        Return True if the branch encountered a fatal error.
        """
        return self.__state == State.CRASHED

    def _kill(self) -> None:
        """
        Forcefully mark this branch as crashed and stop execution.
        """
        self.__state = State.CRASHED
        self.__running.clear()

    def get_error(self):
        """
        Retrieve the last recorded error for this branch.
        """
        return self._error

    def get_stats(self, t_name:str) -> dict:
        """
        Return a summary of statistics for a specific task in this branch.
        """
        return self.__register.get_task(t_name).stats

    def get_results(self, t_name:str) -> list:
        """
        Return a summary of results for a specific task in this branch.
        """
        return self.__register.get_task(t_name).results

    def restart(self, reset_tasks: bool=False) -> None:
        """
        Restart this branch after a crash or stop.

        Parameters:
            reset_tasks (bool): If True, all tasks are revived.
        """
        if reset_tasks:
            self.__register.restart()
        self.__state = State.ALIVE
        self.__running.clear()
        self._error = None

    def __copy__(self):
        raise NotImplementedError()

    def __deepcopy__(self, memo):
        raise NotImplementedError()





