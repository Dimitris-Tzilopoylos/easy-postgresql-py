# from pg_engine.database import Database
# from pg_engine.column import Column
# from pg_engine.relation import Relation
# from pg_engine.model import Model


# Database.init()
 
# class User(Model):
#     id = Column(type='int8', primary=True, nullable=False)
#     email = Column(type="text", nullable=False, unique=True)
#     password = Column(type="text", nullable=False)
#     role_id = Column(type='int8', nullable=False)
#     name = Column(type="text")

#     role = Relation(from_table="users",to_table="roles",from_column="role_id",to_column="id",alias="role",type="object")

#     def __init__(self, schema='public', table='users', connection=None, cursor=None, transaction=False):
#         super().__init__(schema, table, connection, cursor, transaction)


# class Role(Model):
#     id = Column(type='int8', primary=True, nullable=False)
#     name = Column(type="text", nullable=False, unique=False)
#     created_at = Column(type="timestamp",nullable=False)

#     users = Relation(to_table="users",from_table="roles",to_column="role_id",from_column="id",alias="users",type="array")

#     def __init__(self, schema='public', table='roles', connection=None, cursor=None, transaction=False):
#         super().__init__(schema, table, connection, cursor, transaction)


# class Product(Model):
#      id = Column(type='int8', primary=True, nullable=False)
#      name = Column(type="text", nullable=False, unique=False)
#      def __init__(self, schema='public', table='products', connection=None, cursor=None, transaction=False):
#         super().__init__(schema, table, connection, cursor, transaction)

# Database.register_model(User)
# Database.register_model(Role)
# Database.register_model(Product)

# user = User()

# Database.on_error('roles',lambda data,instance: print(data))
# Database.on_insert('roles',lambda data,instance: print("here",data))
# Database.on_delete('roles',lambda data,instance: print("here",data))

# role = Role()
# res = role.delete(where={"name":{"_ilike":"te"}})
  



from pg_engine.engine import Engine


Engine.init()
# Engine.db.set_logger(True)
instance = Engine.model('users')()
Engine.db.set_logger(True)
 
users = instance.aggregate(count=True)

print(users)