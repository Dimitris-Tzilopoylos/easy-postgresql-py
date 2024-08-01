import os 
from .model import Model
from .column import Column 
from .database import Database
from uuid import uuid4
from datetime import datetime
import shutil
import json

class Migrations:
    __folder = 'migrations'

    @staticmethod
    def remove_migrations_folder():
        if Migrations.__dir_exists(Migrations.__resolve_migrations_folder_path()):
            shutil.rmtree(Migrations.__resolve_migrations_folder_path(),)

    @staticmethod
    def __resolve_migrations_folder_path():
        return os.path.join(os.getcwd(),Migrations.__folder)

    @staticmethod
    def __dir_exists(path):
        return os.path.exists(path) and os.path.isdir(path)

    @staticmethod
    def create_migrations_folder(folder:str = 'migrations'):
        Migrations.__folder = folder 
        migrations_folder_path = Migrations.__resolve_migrations_folder_path()
        if not Migrations.__dir_exists(migrations_folder_path):
            os.mkdir(migrations_folder_path)

    @staticmethod 
    def column_to_str(column:Column):
        column_str = f"{column.name} {column.type}"
        if column.length:
            column_str += f"({column.length})"
        
        if not column.nullable:
            column_str += " not null "
        
        if column.unique:
            column_str += " unique "

        if column.primary:
            column_str += " primary key "

        if column.default_value:
            column_str += f" default {column.default_value}"

        if column.check and isinstance(column.check,str):
            column_str += column.check

        return column_str
    
    @staticmethod
    def create_table_columns_to_str(model:Model):
        return ",".join([Migrations.column_to_str(column) for column in model.columns.values()])

    @staticmethod
    def create_schema(schema:str):
        up = f"""create schema if not exists {schema};"""
        down = f"""drop schema if exists {schema} cascade;"""
        Migrations.create_migration(up,down)
        return { "up": up, "down": down, }

    @staticmethod
    def drop_schema(schema:str):
        opposing = Migrations.create_schema(schema)
        Migrations.create_migration(up=opposing.get('down'),down=opposing.get('up'))
        return {"up":opposing.get('down'),"down":opposing.get('up')}


    @staticmethod
    def create_table(model:Model):
        up = f"""create table if not exists {model.get_db_and_table_alias()} ({Migrations.create_table_columns_to_str(model)});"""
        down = f"""drop table if exists {model.get_db_and_table_alias()} cascade;"""
        Migrations.create_migration(up,down)
        return { "up": up, "down": down, }
    
    @staticmethod 
    def drop_table(model:Model):
        opposing = Migrations.create_table(model)
        Migrations.create_migration(opposing.get('down'),opposing.get('up'))
        return {"up":opposing.get('down'),"down":opposing.get('up')}

    @staticmethod
    def set_migrations_folder(folder:str):
        Migrations.__folder = folder

    @staticmethod
    def write_migration(payload):
        Migrations.create_migrations_folder()
        name = f"migration__{payload.get('timestamp')}.json"
        path = os.path.join(Migrations.__resolve_migrations_folder_path(),name)
        with open(path,'w',encoding='utf8') as migration_file:
            json.dump(payload,migration_file, indent=4)
        

    @staticmethod
    def create_migration(up:str,down:str):
        current_datetime = datetime.now()
        timestamp =  current_datetime.timestamp()
        payload = {
            "up":up,
            "down": down,
            "id": uuid4().hex,
            "timestamp":timestamp,
            "applied":False
        }
        Migrations.write_migration(payload)

    @staticmethod
    def apply_migrations(schema:str = Database.schema):
        db = Database(schema)
        queries = list()
        for file in os.listdir(Migrations.__resolve_migrations_folder_path()):
            if file.startswith('migration__') and file.endswith('.json'):
                path = os.path.join(Migrations.__resolve_migrations_folder_path(),file)
                with open(path, "r",encoding='utf8') as json_file:
                    loaded_data = json.load(json_file)
                    if not isinstance(loaded_data,dict):
                        continue
                    if loaded_data.get('applied'):
                        continue 
                    up = loaded_data.get('up')
                    if not up:
                        continue
                    queries.append({"sql": up, "filename":file,"payload":loaded_data})
                    
        
        for query in queries:
            db.query(query.get('sql'))
            path = os.path.join(Migrations.__resolve_migrations_folder_path(),query.get('filename'))
            with open(path,'w',encoding='utf8') as json_file:
                payload = query.get('payload')
                payload['applied'] = True 
                
                json.dump(payload,json_file, indent=4)

