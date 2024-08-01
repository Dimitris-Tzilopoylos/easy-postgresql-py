from pg_engine.database import Database 
from pg_engine.model import Model 
from pg_engine.column import Column


Database.init(host="localhost",port="5435",user="postgres",password="postgres")



class User(Model):
    def __init__(self, schema='public', table='users', connection=None, cursor=None, transaction=False, database='postgres'):
        super().__init__(schema, table, connection, cursor, transaction, database)

    id = Column(name="id",type="uuid",primary=True)
    email = Column(name="email",type="text")
    password = Column(name="password",type="text")






model = User()



data = model.find()
data = model.find()
data = model.insert_one({"email":"dim@dim33.com","password":"123123123"})
print(type(data))
data = model.update({"email":"dim@dim2.com"},{"email":{"_eq":"dim@dim.com"}})
print(data)