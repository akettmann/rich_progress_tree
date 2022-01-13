import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import partial
from time import monotonic_ns
from typing import Any, Callable, MutableMapping, Optional

from rich.live import Live
from rich.table import Column, Table


class TaskStatus(Enum):
    not_run = "Waiting"
    running = "[yellow]Running[/]"
    success = "[green]Success[/]"
    failure = "[blinking red]Failure[/]"


class TaskLogAdapter(logging.LoggerAdapter):
    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        return f"[{self.extra.get('name')}] {msg}", kwargs


class Task:
    __slots__ = [
        "_task_partial",
        "name",
        "_start",
        "_end",
        "_logger",
        "_result",
        "_status",
        "_exc",
    ]

    def __init__(self, task_partial: Callable, name: str):
        self._task_partial = task_partial
        self.name = name
        self._start = None
        self._end = None
        self._result = None
        self._exc = None
        self._status = TaskStatus.not_run

        self._logger = TaskLogAdapter(
            logging.getLogger(self.__class__.__name__), {"name": name}
        )

    def __call__(self) -> Any:
        self._logger.info(f"Starting task")
        self._start = monotonic_ns()
        try:
            self._status = TaskStatus.running
            self._result = self._task_partial()
            self._status = TaskStatus.success
        except Exception as e:
            self._exc = e
            self._status = TaskStatus.failure
        self._end = monotonic_ns()
        self._logger.info(
            f"Task complete, duration: {self.duration}, status: {self._status.name}"
        )

    @property
    def duration(self):
        if self._start and self._end:
            dur = (self._end - self._start) / 1_000_000
        elif self._start and not self._end:
            dur = (monotonic_ns() - self._start) / 1_000_000
        else:
            dur = 0.0
        return f"{dur:,}"

    @property
    def result(self):
        return self._result

    @property
    def status(self):
        return self._status.value


class BaseProgressTable(ABC):
    _TABLE_HEADERS = (
        "[bold]Number",
        "[bold]Name",
        "[bold]Status",
        "[bold]Duration (ms)",
    )

    def __init__(self):
        self._task_list = []

    _task_list: list[Task]

    def add_task(
        self,
        task_callable: Callable,
        *task_args,
        name: Optional[str] = None,
        **task_kwargs,
    ) -> Task:
        task_partial = partial(task_callable, *task_args, **task_kwargs)
        if not name:
            name = task_callable.__name__
        task = Task(task_partial, name)
        self._task_list.append(task)
        return task

    @abstractmethod
    def run_tasks(self):
        raise NotImplementedError

    @property
    def tasks(self):
        return self._task_list

    def __rich__(self):
        t = Table(*self._TABLE_HEADERS)
        for idx, task in enumerate(self._task_list):
            t.add_row(f"{idx + 1}", task.name, task.status, f"{task.duration}")
        return t

    def _make_table_columns_with_cells(self):
        columns = [
            self._make_index_column(),
            self._make_attr_column("name", self._TABLE_HEADERS[1]),
            self._make_attr_column("status", self._TABLE_HEADERS[2]),
            self._make_attr_column("duration", self._TABLE_HEADERS[3]),
        ]
        return columns

    def _make_index_column(self) -> Column:
        cells = [f"{i + 1}" for i in range(len(self._task_list))]
        return Column(header=self._TABLE_HEADERS[0], _cells=cells)

    def _make_attr_column(self, attr_name: str, header_name: str):
        cells = [getattr(t, attr_name) for t in self._task_list]
        return Column(header=header_name, _cells=cells)


class ProgressTable(BaseProgressTable):
    def run_tasks(self):
        with Live(self, refresh_per_second=4):
            for task in self.tasks:
                task()


class ThreadedProgressTable(BaseProgressTable):
    def __init__(self, max_workers=None):
        super().__init__()
        self.max_workers = max_workers

    def run_tasks(self):
        with Live(self, refresh_per_second=4):
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                for task in self.tasks:
                    pool.submit(task)
