from model import Model
from column import Column
from relation import Relation
from database import Database
class User(Model):
    id = Column(type='int8', primary=True, nullable=False)
    email = Column(type="text", nullable=False, unique=True)
    password = Column(type="text", nullable=False)
    role_id = Column(type='int8', nullable=False)


    role = Relation(from_table="users",to_table="roles",from_column="role_id",to_column="id",alias="role",type="object")

    def __init__(self, schema='public', table='users', connection=None, cursor=None, transaction=False):
        super().__init__(schema, table, connection, cursor, transaction)


class Role(Model):
    id = Column(type='int8', primary=True, nullable=False)
    name = Column(type="text", nullable=False, unique=False)
    created_at = Column(type="timestamp",nullable=False)

    users = Relation(to_table="users",from_table="roles",to_column="role_id",from_column="id",alias="users",type="array")

    def __init__(self, schema='public', table='roles', connection=None, cursor=None, transaction=False):
        super().__init__(schema, table, connection, cursor, transaction)


class Product(Model):
     id = Column(type='int8', primary=True, nullable=False)
     name = Column(type="text", nullable=False, unique=False)
     def __init__(self, schema='public', table='products', connection=None, cursor=None, transaction=False):
        super().__init__(schema, table, connection, cursor, transaction)

Database.register_model(User)
Database.register_model(Role)
Database.register_model(Product)

user = User()

users = user.find(where={
    "_and":[{"email":{"_eq":"admin@admin.com"}},{"email":{"_eq":"sddd@ddd"}}, {"_or":[{"role":{"name":{"_eq":"admin"}}},{"role":{"name":{"_eq":"superadmin"}}},{"_and":[{"email":{"_eq":"admin@admin.com"}},{"email":{"_eq":"sddd@ddd"}}]} ]}]
},limit=2)
print(users )
 
