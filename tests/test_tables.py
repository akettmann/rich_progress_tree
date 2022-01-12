def test_synchronous_table_executes_tasks():
    from rich_progress_table import ProgressTable, TaskStatus

    run = False

    def task_func(x: int) -> int:
        nonlocal run
        run = True
        return x + 5

    table = ProgressTable()
    task = table.add_task(task_func, 1, name="First")
    assert task.status == TaskStatus.not_run
    assert not run
    table.run_tasks()
    assert task.result == 6
    assert task.status == TaskStatus.success
