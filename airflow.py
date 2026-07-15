from airflow.sdk import dag, task
import pendulum
import time


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["BrightDataPipeline_testing"],
)
def BrightDataPipeline_testing():
    @task()
    def task1():
        time.sleep(5)
        print("Slept")

    @task()
    def task2():
        print("Task 2 executed")

    t1 = task1()
    t2 = task2()
    t1 >> t2