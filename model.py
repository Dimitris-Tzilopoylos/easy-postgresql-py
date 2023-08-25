from database import Database
from column import Column
from relation import Relation

class Model(Database):
    def __init__(self, schema='public', table='', connection=None, cursor=None, transaction=False):
        self.columns = dict()
        self.relations = dict()
        self.make_columns(self)
        self.make_relations(self)
        super().__init__(schema, table, connection,
                         cursor, transaction, columns=self.columns)

    @classmethod
    def make_columns(cls, self):
        for attr_name, attr_val in vars(cls).items():
            if isinstance(attr_val, Column):
                attr_val.name = attr_name
                self.columns[attr_name] = attr_val
    @classmethod
    def make_relations(cls, self):
        for attr_name, attr_val in vars(cls).items():
            if isinstance(attr_val, Relation):
                attr_val.name = attr_name
                self.relations[attr_name] = attr_val 

    def __repr__(self):
        return "{}".format(self.__class__.__name__)
