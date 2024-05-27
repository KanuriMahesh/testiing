from prefect import flow, task
 
@task
def say_hello():
    print("Hello, world!")
 
@flow
def hello_world_flow():
    say_hello()
 
if __name__ == "__main__":
    hello_world_flow()