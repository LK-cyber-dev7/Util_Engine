import time
import enum

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
    def __init__(self, task_name:str, work, interval: int | float, priority:int=float('inf'), paused:bool=False, max_fails:int=100, pause_on_error:bool=False, catch_up:bool=False, max_fail_streak:int=10, max_tries:int = float('inf'), target_runs:int = float('inf'),pass_rate:float = 0.5,*args, **kwargs):
        self.task_name = task_name

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
            self.status = Status.PAUSED
        else:
            self.status = Status.ACTIVE

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
            self.pass_rate = pass_rate
        else:
            raise ValueError('INVALID PASS_RATE')

        if flag_verify(catch_up):
            self.catch_up = catch_up
        else:
            raise ValueError('INVALID CATCHUP')

        if flag_verify(pause_on_error):
            self.pause_on_error = pause_on_error
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

