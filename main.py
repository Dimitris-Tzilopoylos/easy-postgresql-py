from pg_engine.migrations import Migrations
from pg_engine.database import Database
from pg_engine.column import Column
from pg_engine.relation import Relation
from pg_engine.model import Model


Database.init(schema='test_migrations')

class User(Model):
    id = Column(type='int8', primary=True, nullable=False)
    email = Column(type="text", nullable=False, unique=True)
    password = Column(type="text", nullable=False)
    role_id = Column(type='int8', nullable=False)


    role = Relation(from_table="users",to_table="roles",from_column="role_id",to_column="id",alias="role",type="object")

    def __init__(self, schema='test_migrations', table='users', connection=None, cursor=None, transaction=False):
        super().__init__(schema, table, connection, cursor, transaction)



Migrations.remove_migrations_folder()
Migrations.drop_schema('test_migrations')
Migrations.create_schema('test_migrations')
Migrations.create_table(User())
Migrations.create_migration("alter table users add column last2_name text not null default 'dim';",'alter table users drop column last2_name;')
Migrations.apply_migrations()