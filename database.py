from psycopg2.pool import SimpleConnectionPool, ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from errors import DatabaseException
from events import DatabaseEvents


class Database:
    __pool = ThreadedConnectionPool(10, 100, user='postgres', port='5432', database='postgres',
                                    host='db.qyisoruuofjxbvgiomvu.supabase.co', password='Noisepageant12#')
    __models = dict()

    __enable_logger = False

    def __init__(self, schema='public', table='', connection=None, cursor=None, transaction=False, columns=dict()):
        self.connection = connection
        self.cursor = cursor
        self.transaction = transaction
        self.connected = connection is not None
        self.schema = schema
        self.table = table
        self.columns = columns
        self.connect()

    def __del__(self):
        if self.transaction:
            self.rollback()
        self.disconnect()

    def is_connected(self):
        return self.connected

    def is_transaction_open(self):
        return self.transaction

    def begin(self):
        if not self.is_connected():
            raise DatabaseException(**DatabaseException.NotConnected)

        self.cursor.execute('BEGIN;')
        self.transaction = True

    def commit(self):
        if not self.is_connected():
            raise DatabaseException(**DatabaseException.NotConnected)

        self.cursor.execute('COMMIT;')
        self.transaction = False

    def rollback(self):
        if not self.is_connected():
            raise DatabaseException(**DatabaseException.NotConnected)

        self.cursor.execute('ROLLBACK;')
        self.transaction = False

    def connect(self):
        if self.connected:
            return
        self.connection = Database.__pool.getconn()
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        self.connected = True
        self.transaction = False
        self.connection.autocommit = True

    def disconnect(self):
        if not self.is_connected():
            return

        self.cursor.close()
        Database.__pool.putconn(self.connection)
        self.transaction = False
        self.connected = False
        self.connection = None
        self.cursor = None

    def get_first(self):
        return self.cursor.fetchone()

    def get_all(self):
        return self.cursor.fetchall()

    def with_transaction(self, callback):
        result = None
        try:
            self.begin()
            result = callback(self)
            self.commit()
        except:
            self.rollback()
            result = None
        finally:
            return result

    def select(self):
        try:
            self.query("select {} from {}".format(
                self.get_columns_to_comma_seperated_str(), self.get_db_and_table_alias()))
            results = self.get_all()
            self.on_select(self.table)
        except:
            results = list()
        finally:
            return results

    def update(self, _set: dict, returning=True):
        try:
            config = {}
            for column in self.columns.values():
                if column.name in _set:
                    config[column.name] = args[column.name]
            if len(config.keys()) == 0:
                raise DatabaseException(**DatabaseException.NoValueOperation)
            set_value = ",".join(
                [f"{col_name}=%s" for col_name in config.keys()])
            values = list(config.values())
            q_str = "update {} set {} {}".format(
                self.get_db_and_table_alias(), set_value, self.get_returning(returning))
            self.query(q_str, values)
            result = self.get_returning_value(returning)
        except:
            result = None
        finally:
            return result

    def delete(self, returning=True):
        try:
            q_str = "delete from {} {}".format(
                self.get_db_and_table_alias(), self.get_returning(returning))
            self.query(q_str)
        except:
            pass
        finally:
            return self.get_returning_value(returning)

    def insert_many(self, args: list(dict), returning=True):
        results = dict()
        results[self.table] = list()
        try:
            for ipt in args:
                result = self.insert_one(ipt, returning)
                if not result:
                    raise DatabaseException(
                        **DatabaseException.InsertionFailed)
                results[self.table].append(result)
        except:
            results[self.table] = list()
        return results

    def insert_one(self, args: dict, returning=True):
        try:
            config = {}
            for column in self.columns.values():
                if column.name in args:
                    config[column.name] = args[column.name]
            if len(config.keys()) == 0:
                raise DatabaseException(**DatabaseException.NoValueOperation)
            columns = ",".join(config.keys())
            placeholders = ",".join(["%s" for _ in config.keys()])
            values = list(config.values())
            query_str = 'insert into {}({}) values({}) {}'.format(
                self.get_db_and_table_alias(), columns, placeholders, self.get_returning(returning))
            self.query(query_str, values)
            result = self.get_returning_value(returning)
            if isinstance(result, bool):
                return result
            return result[0]
        except Exception as e:
            result = None
        finally:
            return result

    def query(self, q_str, args=None):
        if Database.__enable_logger:
            print(q_str, args)
        if not isinstance(args, list):
            args = tuple()
        else:
            args = tuple(args)
        if len(args) > 0:
            self.cursor.execute(q_str, args)
        else:
            self.cursor.execute(q_str)

    def get_db_and_table_alias(self):
        return "{}.{}".format(self.schema, self.table)

    def get_columns_to_comma_seperated_str(self):
        return ",".join([f"{self.table}.{column.name}" for column in self.columns.values()])

    def get_returning(self, returning):
        if not returning:
            return ""
        return "returning *"

    def get_returning_value(self, returning=True):
        if returning:
            return self.get_all()
        return True

    @staticmethod
    def register_model(model):
        instance = model()
        Database.__models[instance.table] = model

    @staticmethod
    def check_table_in_registered_models_or_throw(table):
        model = Database.__models.get(table, None)
        if not model:
            raise Exception('no such table')

    @staticmethod
    def on_insert(table, fn):
        Database.check_table_in_registered_models_or_throw(table)
        DatabaseEvents.register_event(table, DatabaseEvents.INSERT, fn)

    @staticmethod
    def on_select(table, fn):
        Database.check_table_in_registered_models_or_throw(table)
        DatabaseEvents.register_event(table, DatabaseEvents.SELECT, fn)

    @staticmethod
    def on_update(table, fn):
        Database.check_table_in_registered_models_or_throw(table)
        DatabaseEvents.register_event(table, DatabaseEvents.UPDATE, fn)

    @staticmethod
    def on_delete(table, fn):
        Database.check_table_in_registered_models_or_throw(table)
        DatabaseEvents.register_event(table, DatabaseEvents.DELETE, fn)

    @staticmethod
    def on_error(table, fn):
        Database.check_table_in_registered_models_or_throw(table)
        DatabaseEvents.register_event(table, DatabaseEvents.ERROR, fn)

    @staticmethod
    def set_logger(value: bool):
        Database.__enable_logger = value
