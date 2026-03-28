import functools
import threading
from concurrent.futures import ThreadPoolExecutor
import time

register = {}
reg_id = 0
high_priority = {}
low_priority = {}
stop_flag = threading.Event()
lock_flag = threading.Lock()

def merge_and_sort(dict1: dict, dict2: dict) -> dict:
    # Step 1: merge
    merged = {**dict1, **dict2}

    # Step 2: sort by *ascending* priority (4 before 7)
    sorted_items = sorted(
        merged.items(),
        key=lambda x: (
            x[1].get("priority", float("inf")),  # low numbers first; no-priority tasks go last
            x[1]["register_id"]
        )
    )

    # Step 3: convert list back to ordered dict
    return dict(sorted_items)

def schedule(task_name,time_period, priority=-1,*args, **kwargs):
    def decorator(func):
        global register, reg_id, high_priority, low_priority
        @functools.wraps(func)
        def wrapper(*f_args, **f_kwargs):
            return func(*f_args, **f_kwargs)

        with lock_flag:
            if priority >= 0:
                high_priority[task_name]=({
                    'task': wrapper,
                    'interval': time_period,
                    'priority': priority,
                    'register_id': reg_id,
                    'arguments': (args,kwargs),
                    'last_run_time': time.monotonic(),
                    'paused': False
                })
            else:
                low_priority[task_name]=({
                    'task': wrapper,
                    'interval': time_period,
                    'register_id': reg_id,
                    'arguments': (args,kwargs),
                    "last_run_time": time.monotonic(),
                    'paused': False
                })
            reg_id += 1
            register = merge_and_sort(high_priority,low_priority)
        return wrapper
    return decorator

def mainloop():
    global register, stop_flag

    with ThreadPoolExecutor() as executor:
        while not stop_flag.is_set():
            current = time.monotonic()
            with lock_flag:
                # Iterate tasks in priority order
                register = merge_and_sort(high_priority, low_priority)
                for key,item in register.items():
                    if current - item["last_run_time"] >= item["interval"] and not item['paused']:
                        item["last_run_time"] += item["interval"]
                        executor.submit(
                            item["task"],
                            *item["arguments"][0],
                            **item["arguments"][1]
                        )

            time.sleep(0.05)

def start_mainloop():
    stop_flag.clear()
    threading.Thread(target=mainloop, daemon=True).start()

def end_mainloop():
    stop_flag.set()

def pause(task_name):
    global high_priority, low_priority
    with lock_flag:
        if task_name in high_priority:
            high_priority[task_name]['paused'] = True
        elif task_name in low_priority:
            low_priority[task_name]['paused'] = True

def play(task_name):
    global low_priority,high_priority
    with lock_flag:
        if task_name in low_priority:
            low_priority[task_name]['paused'] = False
        elif task_name in high_priority:
            high_priority[task_name]['paused'] = False

def remove(task_name):
    global low_priority,high_priority
    with lock_flag:
        if task_name in high_priority:
            del high_priority[task_name]
        elif task_name in low_priority:
            del low_priority[task_name]

def change_priority(task_name, priority):
    global register, reg_id, high_priority, low_priority
    with lock_flag:
        if task_name in high_priority:
            high_priority[task_name]['priority'] = priority
        elif task_name in low_priority:
            high_priority[task_name] = low_priority[task_name]
            del low_priority[task_name]
            high_priority[task_name]['priority'] = priority
