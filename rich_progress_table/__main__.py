from rich_progress_table import ProgressTable
import time

if __name__ == "__main__":

    def dumb_task_maker(sleep_time: float):
        def dumb_task():
            time.sleep(sleep_time)

        return dumb_task

    t = ProgressTable()
    t.add_task(dumb_task_maker(0.5), name="Create Database")
    t.add_task(dumb_task_maker(2.5), name="Import Database")
    t.add_task(dumb_task_maker(1.5), name="Create App Dir")
    t.add_task(dumb_task_maker(5.0), name="Download Libraries")
    t.add_task(dumb_task_maker(0.2), name="Reticulating Splines")
    t.run_tasks()
