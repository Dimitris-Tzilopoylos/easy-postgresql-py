from model import Model
from column import Column


class User(Model):
    id = Column(type='int8', primary=True, nullable=False)
    email = Column(type="text", nullable=False, unique=True)
    password = Column(type="text", nullable=False)
    role_id = Column(type='int8', nullable=False)

    def __init__(self, schema='public', table='users', connection=None, cursor=None, transaction=False):
        super().__init__(schema, table, connection, cursor, transaction)


user = User()

print(user.connected)
x = user.insert_one(
    {"email": "dimtzilopoylos2532@gmail.com", "password": "12345678", "role_id": 1})
print(x.get('email'))
print(user.select())
print("here")
