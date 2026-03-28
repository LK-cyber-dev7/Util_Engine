class SchedulerBranchError(Exception):
    def __init__(self, text):
        super.__init__(text)

class TaskNotFoundError(Exception):
    def __init__(self, text):
        super.__init__(text)

class InvalidConfigError(Exception):
    def __init__(self, text):
        super.__init__(text)