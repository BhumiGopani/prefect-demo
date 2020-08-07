from prefect import task, Flow

@task
def hello_world():
    print("Hello World !!!")
    return "Hello Prefect !!!"

@task
def prefect_say(s: str):
    print(s)

with Flow("My First prefect flow") as f:
    r = hello_world()
    s2 = prefect_say(r)

f.run()
