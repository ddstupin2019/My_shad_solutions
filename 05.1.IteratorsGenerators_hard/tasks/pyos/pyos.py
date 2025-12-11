from queue import Queue
from abc import ABC, abstractmethod
from typing import Generator, Any


class SystemCall(ABC):
    """SystemCall yielded by Task to handle with Scheduler"""

    @abstractmethod
    def handle(self, scheduler: 'Scheduler', task: 'Task') -> bool:
        """
        :param scheduler: link to scheduler to manipulate with active tasks
        :param task: task which requested the system call
        :return: an indication that the task must be scheduled again
        """


Coroutine = Generator[SystemCall | None, Any, None]


class Task:
    def __init__(self, task_id: int, target: Coroutine) -> None:
        """
        :param task_id: id of the task
        :param target: coroutine to run. Coroutine can produce system calls.
        System calls are being executed by scheduler and the result sends back to coroutine.
        """
        self.task_id = task_id
        self.target = target
        self.last_res = None
        self.is_work = True

    def set_syscall_result(self, result: Any) -> None:
        """
        Saves result of the last system call
        """
        self.last_res = result

    def step(self) -> SystemCall | None:
        """
        Performs one step of coroutine, i.e. sends result of last system call
        to coroutine (generator), gets yielded value and returns it.
        """
        try:
            return self.target.send(self.last_res)
        except StopIteration:
            self.is_work = False
            return None


class Scheduler:
    """Scheduler to manipulate with tasks"""

    def __init__(self) -> None:
        self.task_id = 1
        self.task_queue: Queue[Task] = Queue()
        self.task_map: dict[int, Task] = {}  # task_id -> task
        self.wait_map: dict[int, list[Task]] = {}  # task_id -> list of waiting tasks

    def _schedule_task(self, task: Task) -> None:
        """
        Add task into task queue
        :param task: task to schedule for execution
        """
        self.task_queue.put(task)

    def new(self, target: Coroutine) -> int:
        """
        Create and schedule new task
        :param target: coroutine to wrap in task
        :return: id of newly created task
        """
        task = Task(self.task_id, target)
        self.task_map[task.task_id] = task
        self.task_id += 1
        self._schedule_task(task)
        return task.task_id

    def exit_task(self, task_id: int) -> bool:
        """
        PRIVATE API: can be used only from scheduler itself or system calls
        Hint: do not forget to reschedule waiting tasks
        :param task_id: task to remove from scheduler
        :return: true if task id is valid
        """
        if task_id in self.task_map:
            self.task_map[task_id].target.close()
            del self.task_map[task_id]
            return True
        return False

    def wait_task(self, task_id: int, wait_id: int) -> bool:
        """
        PRIVATE API: can be used only from scheduler itself or system calls
        :param task_id: task to hold on until another task is finished
        :param wait_id: id of the other task to wait for
        :return: true if task and wait ids are valid task ids
        """
        if task_id in self.task_map and wait_id in self.task_map:
            if task_id not in  self.wait_map:
                self.wait_map[task_id] = []
            self.wait_map[task_id].append(self.task_map[wait_id])
            return True
        return False

    def run(self, ticks: int | None = None) -> None:
        """
        Executes tasks consequently, gets yielded system calls,
        handles them and reschedules task if needed
        :param ticks: number of iterations (task steps), infinite if not passed
        """
        if ticks is None:
            ticks = 1000

        for ind in range(ticks):
            if self.task_queue.empty():
                return
            task = self.task_queue.get()

            while task.task_id not in self.task_map:
                if self.task_queue.empty():
                    return
                task = self.task_queue.get()

            if task.task_id in self.wait_map:
                fl = True
                for i in self.wait_map[task.task_id]:
                    if i.task_id in self.task_map:
                        fl = False
                        break
                if not fl:
                    self._schedule_task(task)
                    continue

            tmp = task.step()

            if type(tmp) is GetTid:
                task.set_syscall_result(task.task_id)
            elif type(tmp) is NewTask:
                task.set_syscall_result(self.new(tmp.target))
            elif type(tmp) is KillTask:
                task.set_syscall_result(self.exit_task(tmp.task_id))
            elif type(tmp) is WaitTask:
                task.set_syscall_result(self.wait_task(task.task_id, tmp.task_id))

            if task.is_work:
                self._schedule_task(task)
            else:
                self.exit_task(task.task_id)



    def empty(self) -> bool:
        """Checks if there are some scheduled tasks"""
        return not bool(self.task_map)


class GetTid(SystemCall):
    """System call to get current task id"""

    def handle(self, scheduler: Scheduler, task: Task) -> bool:
        pass


class NewTask(SystemCall):
    """System call to create new task from target coroutine"""

    def __init__(self, target: Coroutine) -> None:
        self.target = target

    def handle(self, scheduler: Scheduler, task: Task) -> bool:
        pass


class KillTask(SystemCall):
    """System call to kill task with particular task id"""

    def __init__(self, task_id: int) -> None:
        self.task_id = task_id

    def handle(self, scheduler: Scheduler, task: Task) -> bool:
        pass


class WaitTask(SystemCall):
    """System call to wait task with particular task id"""

    def __init__(self, task_id: int) -> None:
        self.task_id = task_id

    def handle(self, scheduler: Scheduler, task: Task) -> bool:
        # Note: One shouldn't reschedule task which is waiting for another one.
        # But one must reschedule task if task id to wait for is invalid.
        pass
