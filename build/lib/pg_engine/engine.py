from .database import Database
from .model import Model 
from .column import Column
from .relation import Relation
import os
import json 

class Engine:
    db = Database
    models = dict()
    model_factory:dict = dict()
    tables = list()
    auth_options:dict | None = None
    __relations_path = "relations.engine.json"

    @staticmethod
    def init(host='localhost',port='5432',user='postgres',password='postgres',pool_type='threaded',schema='public',minconn=10,maxconn=50,auth_options:dict | None= None,relations_filename = 'relations.engine.json'):
        Database.init(host=host,port=port,user=user,password=password,pool_type=pool_type,schema=schema,minconn=minconn,maxconn=maxconn)
        Engine.set_relations_filename(relations_filename)
        Engine.db = Database()
        Engine.get_tables()
        Engine.auth_options = auth_options
        Engine.build_models()
    

    @staticmethod
    def model(table_name:str):
        return Engine.model_factory.get(table_name,None)

    @staticmethod
    def get_tables():
        query = f"""
            select table_name from information_schema.tables where table_schema = '{Engine.db.schema}' and table_type = 'BASE TABLE'; 
        """
        try:
            Engine.db.query(query)
            tables = Engine.db.get_all()
             
            for table in tables:
                columns = Engine.get_table_columns(table.get('table_name'))
                if len(columns):
                    Engine.tables.append({"name":table.get('table_name'),"columns":columns})
        except Exception as e:
            print(e)
            pass 
        finally:
            
            Engine.db.disconnect()
    

    @staticmethod
    def get_table_columns(table:str):
        query = f"""
        SELECT udt_name as mixed_column_type,case when is_nullable = 'NO' then false else true end as nullable,column_name as name, data_type as type FROM information_schema.columns WHERE table_schema = '{Engine.db.schema}' AND table_name = '{table}';
        """
        try:
            Engine.db.query(query)
            columns = Engine.db.get_all()
        except Exception as e:
            print(e)
            columns = []
        finally: 
            return columns 
        

    @staticmethod
    def build_models():
        relations = dict()
        with open(Engine.__resolve_relations_path(),'r',encoding='utf8') as file:
            relations = json.load(file)
        for table in Engine.tables:
            model_for_factory = Engine.build_model_from_table(table,relations.get(table.get('name')))
            if not model_for_factory:
                continue 
            Engine.model_factory[table.get('name')] = model_for_factory
            Database.register_model(model_for_factory)

        
    @staticmethod
    def build_model_from_table(table:dict | None,table_relations:list | None):
        if not table:
            return None 
        if not table.get('columns'):
            return None 
        
        def build_model():
            table_name = table.get('name')
            columns = table.get('columns')
            relations = table_relations
            class ModelFactory(Model):
                def __init__(self, schema=Engine.db.schema, table=table_name, connection=None, cursor=None, transaction=False):
                    super().__init__(schema, table, connection, cursor, transaction)
                    for column in columns:
                        name = column.get('name')
                        nullable = column.get('nullable')
                        data_type = f"{column.get('mixed_column_type')}[]" if column.get('type') == 'ARRAY' else column.get('type')
                        self.__setattr__(column.get('name'), Column(name=name,nullable=nullable,type=data_type))
                        self.columns[column.get('name')] = Column(name=name,nullable=nullable,type=data_type)
                    
                    if isinstance(table_relations,list):
                        for relation in relations:
                            self.relations[relation.get('alias')] = Relation(**relation)
                        
                
                def __repr__(self):
                    return f"{table_name}"
                
            return ModelFactory
        
        return build_model()
    
    @staticmethod
    def set_relations_filename(filename:str = "relations.engine.json"):
        Engine.__relations_path = filename 

    @staticmethod
    def __resolve_relations_path():
        return os.path.join(os.getcwd(),Engine.__relations_path)