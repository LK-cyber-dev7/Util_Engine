import functools
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

def flag_verify(value):
    return isinstance(value, bool)

def numerical_verify(value):
    if isinstance(value, bool):
        return False
    if value == float('inf'):
        return True
    if isinstance(value, int) and value > 0:
        return True
    return False

def time_verify(value):
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    if value <= 0 or value == float('inf'):
        return False
    return True

def pass_rate_verify(value):
    if isinstance(value, bool):
        return False
    elif isinstance(value, int) and 1 >= value >= 0:
        return True
    elif isinstance(value, float) and 1 >= value >= 0:
        return True
    return False

def priority_verify(value):
    if isinstance(value, bool):
        return False
    elif value == float('inf') or value == float('-inf'):
        return True
    elif isinstance(value, int):
        return True
    return False

class Task:

    id_counter = 0

    def __init__(self, task_name:str, work, interval: int | float, priority:int=float('inf'), paused:bool=False, max_fails:int=100, pause_on_error:bool=False, catch_up:bool=False, max_fail_streak:int=10, max_tries:int = float('inf'), target_runs:int = float('inf'),pass_rate:float = 0.5,*args, **kwargs):
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

        self.results = []
        self.stats:dict[str,float | int] = {
            'failures':0,
            'runs':0,
            'non_zero_results':0,
            'retry_streak':0,
        }

    def is_alive(self):
        return self.status != Status.TERMINATED and self.status != Status.COMPLETED and self.status != Status.FAILED

    def pause_task(self):
        if self.is_alive():
            self.status = Status.PAUSED

    def resume_task(self):
        if self.is_alive():
            self.status = Status.ACTIVE

    def terminate_task(self):
        self.status = Status.TERMINATED # needs to be updated when register class is set up. WORK IN PROGRESS

    def complete_task(self):
        self.status = Status.COMPLETED

    def change_priority(self,priority:int):
        if self.is_alive():
            if priority_verify(priority):
                self.priority = priority
            else:
                raise ValueError('priority must be positive')

    def set(self, interval=None, max_fails=None, pause_on_error=None, catch_up=None, max_fail_streak=None, max_tries=None,target_runs=None):
        self.interval = interval if time_verify(interval) else self.interval
        self.max_fails = max_fails if numerical_verify(max_fails) else self.max_fails
        self.max_tries = max_tries if numerical_verify(max_tries) else self.max_tries
        self.target_runs = target_runs if numerical_verify(target_runs) else self.target_runs
        self.max_fail_streak = max_fail_streak if numerical_verify(max_fail_streak) else self.max_fail_streak
        self.pause_on_error = pause_on_error if flag_verify(pause_on_error) else self.pause_on_error
        self.catch_up = catch_up if flag_verify(catch_up) else self.catch_up
        if self.is_alive():
            self.check_failures()

    def replace(self,work,*args,**kwargs):
        if callable(work):
            self.work = work
        else:
            raise TypeError('work must be callable')
        self.args = args
        self.kwargs = kwargs

    def is_active(self):
        self.check_failures()
        return self.status in {Status.RETRYING, Status.ACTIVE}

    def failed(self):
        if self.is_alive():
            self.stats['failures'] += 1
            self.stats['runs'] += 1
            self.stats['retry_streak'] += 1
            if self.pause_on_error:
                self.status = Status.ERROR # the task resumes when resumed by user so it gets reset to active in that func
            else:
                self.status = Status.RETRYING
            self.check_failures()

    def succeeded(self,result):
        if self.is_alive():
            self.stats['runs'] += 1
            self.results.append(result)
            if self.status == Status.RETRYING:
                self.stats['retry_streak'] = 0
                self.status = Status.ACTIVE
            self.check_failures()

    def check_failures(self):
        if self.is_alive():
            if self.stats['runs'] - self.stats['failures'] >= self.target_runs:
                if self.stats['success_rate'] >= self.pass_rate:
                    self.status = Status.COMPLETED
                else:
                    self.status = Status.FAILED
            elif self.stats['failures'] >= self.max_fails:
                self.terminate_task()
            elif self.stats['retry_streak'] >= self.max_fail_streak:
                self.terminate_task()
            elif self.stats['runs'] >= self.max_tries:
                self.terminate_task()

    def revive(self):
        if not self.is_alive():
            self.stats: dict[str, float | int] = {
                'failures': 0,
                'runs': 0,
                'non_zero_results': 0,
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
        with self.lock:
            self.data[task.task_name] = task
            if task.is_active():
                self.active_tasks.add(task)

    def remove_task(self, name: str) -> None:
        if name in self.data:
            with self.lock:
                if self.data[name] in self.active_tasks:
                    self.active_tasks.remove(self.data[name])
                del self.data[name]

    def check_task(self, name: str) -> None:
        task = self.data[name]
        with self.lock:
            if task in self.active_tasks:
                self.active_tasks.remove(task)
            if task.is_active():
                self.active_tasks.add(task)


    def set_config(self, name:str, interval=None, max_fails=None, pause_on_error: bool=None, catch_up: bool=None, max_fail_streak=None, max_tries=None,target_runs=None) -> None:
        if name in self.data:
            self.data[name].set(interval=interval,max_fails=max_fails,pause_on_error=pause_on_error,catch_up=catch_up,max_fail_streak=max_fail_streak,max_tries=max_tries,target_runs=target_runs)
            self.check_task(name)

    def set_priority(self, name:str, priority:int) -> None:
        if name in self.data:
            self.data[name].change_priority(priority)
            self.check_task(name)


    def revive_task(self, name: str) -> None:
        if name in self.data:
            self.data[name].revive()
            self.check_task(name)

    def pause_task(self, name: str) -> None:
        if name in self.data:
            self.data[name].pause_task()
            self.check_task(name)

    def terminate_task(self, name: str) -> None:
        if name in self.data:
            self.data[name].terminate_task()
            self.check_task(name)

    def complete_task(self, name: str) -> None:
        if name in self.data:
            self.data[name].complete_task()
            self.check_task(name)

    def replace_task(self, name: str,work: Callable,*args,**kwargs) -> None:
        if name in self.data:
            self.data[name].replace(work,*args,**kwargs)

    def resume_task(self, name: str) -> None:
        if name in self.data:
            self.data[name].resume_task()
            self.check_task(name)

    def task_success(self, name:str, result) -> None:
        if name in self.data:
            self.data[name].succeeded(result)
            self.check_task(name)

    def task_failure(self, name:str) -> None:
        if name in self.data:
            self.data[name].failed()
            self.check_task(name)

    def get_task(self, name:str) -> Task:
        return self.data.get(name)

    def __iter__(self):
        with self.lock:
            return iter(self.active_tasks)

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

    def lock_register(self):
        # called before loop starts
        self.locked = True

    def unlock_register(self):
        # called after loop finishes
        self.locked = False

    def add(self, task:Task) -> None:
        if not self.locked:
            self.data[task.task_name] = task
            if task.is_active():
                self.active_tasks.add(task)

    def remove_task(self, name: str) -> None:
        if not self.locked:
            if name in self.data:
                if self.data[name] in self.active_tasks:
                    self.active_tasks.remove(self.data[name])
                del self.data[name]

    def check_task(self, name: str) -> None:
        if not self.locked:
            task = self.data[name]
            if task in self.active_tasks:
                self.active_tasks.remove(task)
            if task.is_active():
                self.active_tasks.add(task)

    def set_config(self, name:str, interval=None, max_fails=None, pause_on_error: bool=None, catch_up: bool=None, max_fail_streak=None, max_tries=None,target_runs=None) -> None:
        if not self.locked and name in self.data:
            self.data[name].set(interval=interval,max_fails=max_fails,pause_on_error=pause_on_error,catch_up=catch_up,max_fail_streak=max_fail_streak,max_tries=max_tries,target_runs=target_runs)
            self.check_task(name)

    def set_priority(self, name:str, priority:int) -> None:
        if not self.locked and name in self.data:
            self.data[name].change_priority(priority)
            self.check_task(name)


    def revive_task(self, name: str) -> None:
        if not self.locked and name in self.data:
            self.data[name].revive()
            self.check_task(name)

    def pause_task(self, name: str) -> None:
        if not self.locked and name in self.data:
            self.data[name].pause_task()
            self.check_task(name)

    def terminate_task(self, name: str) -> None:
        if not self.locked and name in self.data:
            self.data[name].terminate_task()
            self.check_task(name)

    def complete_task(self, name: str) -> None:
        if not self.locked and name in self.data:
            self.data[name].complete_task()
            self.check_task(name)

    def replace_task(self, name: str,work: Callable,*args,**kwargs) -> None:
        if not self.locked and name in self.data:
            self.data[name].replace(work,*args,**kwargs)

    def resume_task(self, name: str) -> None:
        if not self.locked and name in self.data:
            self.data[name].resume_task()
            self.check_task(name)

    def __check_for_task_state(self, name:str) -> None:
        task = self.data[name]
        active = task.is_active()
        in_list = task in self.active_tasks
        if active and not in_list:
            self.active_tasks.add(task)
        elif not active and in_list:
            self.active_tasks.remove(task)

    def task_success(self, name:str, result) -> None:
        # not a user method
        if name in self.data:
            self.data[name].succeeded(result)
            self.__check_for_task_state(name)

    def task_failure(self, name:str) -> None:
        # not a user method
        if name in self.data:
            self.data[name].failed()
            self.__check_for_task_state(name)

    def get_task(self, name:str) -> Task:
        return self.data.get(name)

    def __iter__(self):
        # return a snapshot list of active tasks
        return iter([t for t in self.active_tasks if t.is_active()])

    def __contains__(self, item: Task) -> bool:
        return item.task_name in self.data

def verify_name_status(action_name="do something"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, name, *args, **kwargs):
            if name not in self.__names:
                raise TaskNotFoundError(f"User tried to {action_name} with INVALID name")

            if self.__running and not self.__mutable:
                raise SchedulerBranchError(
                    f"User tried to {action_name} while branch was running. Branch is Immutable can not {action_name}."
                )
            return func(self, name, *args, **kwargs)
        return wrapper
    return decorator

class Branch:
    __id_counter: int = 0
    def __init__(self, name: str, method: Literal['multiprocessing','threading','asyncio'], mutable=False) -> None:
        self.name:str = name
        self.__mutable = mutable
        self.__id = Branch.__id_counter
        Branch.__id_counter += 1
        if method not in {'multiprocessing','threading','asyncio'}:
            raise ValueError('method must be one of "multiprocessing","threading","asyncio"')
        elif method == 'multiprocessing' and mutable:
            raise SchedulerBranchError("User tried to create a mutable multiprocessing Branch")
        __lock = threading.Lock() if method == "threading" else asyncio.Lock if method == "asyncio" else None
        if mutable:
            self.__register = MutableRegister(name,__lock)
        else:
            self.__register = ImmutableRegister(name)

        self.__running = False
        self.__names = set()

    def assign_task(self, name:str, work: Callable, interval: int | float, config:dict):
        if self.__running and not self.__mutable:
            raise SchedulerBranchError(
                f"User tried to assign task while branch was running. Branch is Immutable can not assign task."
            )

        if not callable(work):
            raise TypeError(
                f"User tried to assign task with INVALID work. Work must be a python callable."
            )

        try:
            t = Task(name,work,interval=interval,**config)
        except TypeError:
            raise InvalidConfigError(f"User tried to assign task with INVALID config")

        self.__register.add(t)
        self.__names.add(t.task_name)

    @verify_name_status('remove task')
    def remove_task(self, name:str) -> None:
        self.__register.remove_task(name)
        self.__names.remove(name)

    @verify_name_status('pause task')
    def pause_task(self, name:str) -> None:
        self.__register.pause_task(name)

    @verify_name_status('terminate task')
    def terminate_task(self, name:str) -> None:
        self.__register.terminate_task(name)

    @verify_name_status('complete task')
    def complete_task(self, name:str) -> None:
        self.__register.complete_task(name)

    @verify_name_status('replace task')
    def replace_task(self, name:str,work:Callable,*args,**kwargs) -> None:
        if not callable(work):
            raise TypeError(
                f"User tried to replace task with INVALID work. Work must be a python callable"
            )

        self.__register.replace_task(name,work,*args,**kwargs)

    @verify_name_status('resume task')
    def resume_task(self, name:str) -> None:
        self.__register.resume_task(name)

    @verify_name_status('revive task')
    def revive_task(self, name:str) -> None:
        self.__register.revive_task(name)

    @verify_name_status('change task configuration')
    def change_config(self, name,config:dict) -> None:
        try:
            self.__register.set_config(name,**config)
        except TypeError:
            raise InvalidConfigError(f"User tried to change task configuration with INVALID config")

    @verify_name_status('change task priority')
    def change_task_priority(self,name,priority:int) -> None:
        if not priority_verify(priority):
            raise InvalidConfigError(f"User tried to change task priority with INVALID priority")

        self.__register.set_priority(name,priority)

    def is_running(self):
        return self.__running

    def get_id(self):
        return self.__id

    def is_mutable(self):
        return self.__mutable





