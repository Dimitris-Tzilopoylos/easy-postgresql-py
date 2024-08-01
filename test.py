from  pg_engine.engine import Engine 
from fastapi import FastAPI


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

def on_select(data,instance):
    print(data,instance.is_connected())

Engine.db.on_select("root_engine","engine_users",on_select)
 
@app.get("/{schema}/{table}")
async def read_root(schema:str,table:str):
    instance =  Engine.model(schema,table)
    data = instance.find_one()
    instance.disconnect()
    return data

