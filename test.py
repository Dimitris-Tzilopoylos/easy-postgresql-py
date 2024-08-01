from  pg_engine.engine import Engine 
from fastapi import FastAPI,Depends,Request
from contextlib import contextmanager
from functools import wraps

app = FastAPI()

EngineConfig = {
    "host":"localhost",
    "port":"5435",
    "user":"postgres",
    "password":"postgres",
    "minconn":2,
    "maxconn":5
}

Engine.init(**EngineConfig)


cache = None

 

@contextmanager
def get_db_connection(schema: str, table: str):
    instance = Engine.model(schema, table)
    instance.connect()
    try:
        yield instance
    finally:
        instance.disconnect()

 
def with_engine_model(fn):
    @wraps(fn)
    async def wrapper_fn(*args,**kwargs):
        with get_db_connection(kwargs.get("schema"),kwargs.get("table")) as db_instance:
            kwargs["db_instance"] = db_instance
            return await fn(*args,**kwargs)
    return wrapper_fn    


@app.get("/{schema}/{table}")
@with_engine_model
async def read_root(*args,schema,table):
    # data = db_instance.find()
    return schema

 