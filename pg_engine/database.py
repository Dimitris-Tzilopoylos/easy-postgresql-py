from psycopg2.pool import SimpleConnectionPool, ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from .errors import DatabaseException
from .events import DatabaseEvents
from .column import Column
from .raw_sql import RawSQL

SELF_UPDATE_OPERATORS = {
        "_inc": " + ",
        "_dec": " - ",
        "_mult": " * ",
        "_div": " / ",
}
class Database:

    
    __pool = None
    __models = dict()
    __registered_models = dict()
    __enable_logger = False
    schema = 'public'
    def __init__(self, schema='public', table='', connection=None, cursor=None, transaction=False, columns=dict()):
        self.connection = connection
        self.cursor = cursor
        self.transaction = transaction
        self.connected = connection is not None
        self.schema = schema
        self.table = table
        self.columns = columns
        # self.connect()

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
        self.transaction = self.transaction
        self.connection.autocommit = not self.transaction

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

    def with_transaction(self, callback,):
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

    def aggregate(self,count:bool=None,min:dict | None=None,max:dict | None=None,sum:dict | None=None,avg:dict | None=None,where:dict | None = None,distinct_on:list | None = None,group_by:list | None = None):
        config = {
            "count":count,
            "min":min,
            "max":max,
            "sum":sum,
            "avg":avg,
            "where":where,
            "distinct_on":distinct_on,
            "group_by":group_by
        }
        agg_sql,args = Aggregation.build_aggregate(self,config,None,None,0)
        if not agg_sql:
            raise DatabaseException(DatabaseException.NoValueOperation,500)
        
        self.query(agg_sql,args)
        if distinct_on:
            return self.get_all()
        return self.get_first()

    
    def find(self,**kwargs):
        try:
            depth = 0
            idx = 1
            args = list()
            alias = Database.make_depth_alias(self.table,depth)
            
            include = kwargs.get('include',dict())
            if not isinstance(include,dict):
                include = dict()
            select_columns_str = Database.relational_and_model_columns_str(self,depth,kwargs.get('include',dict()))
            append_sql = ""
            for relational_key in include.keys():
                agg_relation = Aggregation.is_aggregate(self,relational_key)
                if agg_relation:
                    relation = agg_relation
                else:
                    relation = self.relations.get(relational_key,None)
                if not relation:
                    continue 
                config = include.get(relational_key,dict())
                sql,append_args,next_index = relation.get_select_lateral_join_relational_str(alias,depth + 1,idx,config,agg_relation is not None) 
                append_sql += sql 
                args.extend(append_args)
                idx = next_index
            where_str,where_args = Where.make_where_clause(self,kwargs.get('where',None),alias,depth)
            limit_str,limit_args = Database.make_limit(kwargs.get('limit',None))
            offset_str,offset_args = Database.make_offset(kwargs.get('offset',None))
            args.extend(where_args)
            args.extend(limit_args)
            args.extend(offset_args)
            sql_str = """
                select coalesce(json_agg({}),'[]') as {}
                from (
                    select row_to_json((
                        select {}
                        from ( select {} ) {}
                    ))   {}
                    from ( select {} {} from {}.{} {} {} {} {} {} {} ) {} {} 
                ) {}
            """.format(alias,self.table,alias,select_columns_str,alias,alias,DistinctOn.make_distinct_on(self,kwargs.get('distinct_on',None)),self.get_columns_to_comma_seperated_str(alias),
                       Database.schema,self.table,alias,where_str,GroupBy.make_group_by(self,kwargs.get('group_by',None),alias),OrderBy.make_order_by(self,kwargs.get('order_by',None),alias),limit_str,offset_str, alias,append_sql,alias
             )
            self.query(sql_str,args)
            results = self.get_first()
            results = results[self.table]
            DatabaseEvents.execute_select_events(self.table,results,self)
        except Exception as e:
            print(e)
            DatabaseEvents.execute_error_events(self.table,e,self)
            results = list()
        finally:
            return results

    def find_one(self,**kwargs):
        kwargs['limit'] = 1
        result = self.find(**kwargs)
        return result[0]

    def update(self, _set: dict, where:dict, returning=True):
        try:
            config = {}
            for column in self.columns.values():
                if column.name in _set:
                    config[column.name] = _set[column.name]
                    
            if len(config.keys()) == 0:
                raise DatabaseException(**DatabaseException.NoValueOperation)
            
            values = list()
            cols = list()
            for col_name,value in config.items():
                str,arg = Database.get_update_column_to_str(col_name,value)
                cols.append(str)
                values.append(arg)
            
            where_str,where_args = Where.make_where_clause(self,where,self.table)
            values.extend(where_args)
            q_str = "update {} set {} {} {}".format(
                self.get_db_and_table_alias(), ",".join(cols), where_str,self.get_returning(returning))
            self.query(q_str, values)
            result = self.get_returning_value(returning)
            DatabaseEvents.execute_update_events(self.table,result,self)
        except Exception as e:
            DatabaseEvents.execute_error_events(self.table,e,self)
            result = None
        finally:
            return result

    def delete(self, where:dict,returning=True):
        try:
            where_str,where_args = Where.make_where_clause(self,where,self.table)
            q_str = "delete from {} {} {}".format(
                self.get_db_and_table_alias(), where_str, self.get_returning(returning))
            self.query(q_str,where_args)
            results = self.get_returning_value(returning)
            DatabaseEvents.execute_delete_events(self.table,results,self)
        except Exception as e:
            DatabaseEvents.execute_error_events(self.table,e,self)
            results = None
        finally:
            return results

    def insert_many(self, args: list(), returning=True):
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
            relational_config = {}
            for column in self.columns.values():
                if column.name in args:
                    config[column.name] = args[column.name]
            for relation in self.relations.values():
                if relation.alias in config:
                    relational_config[relation.alias] = config[relation.alias]
                    if not isinstance(relational_config[relation.alias],list):
                        relational_config[relation.alias] = [config[relation.alias]]
            if len(config.keys()) == 0:
                raise DatabaseException(**DatabaseException.NoValueOperation)
            columns = ",".join(config.keys())
            placeholders = ",".join(["%s" for _ in config.keys()])
            values = list(config.values())
            query_str = 'insert into {}({}) values({}) {}'.format(
                self.get_db_and_table_alias(), columns, placeholders, self.get_returning(returning))
            self.query(query_str, values)
            result = self.get_returning_value(returning)
            relational_results = {}
            for alias,rel_config in relational_config.items():
                relation = self.relations.get(alias)
                if not relation:
                    continue 
                model = Database.get_registered_model(relation.to_table)
                if not model:
                    continue
                relational_instance = model(connection=self.connection,cursor=self.cursor,transaction=self.transaction)
                relational_result = relational_instance.insert_many(rel_config,returning)
                relational_results[alias] = relational_result
            if isinstance(result, bool):
                DatabaseEvents.execute_insert_events(self.table,result,self)
                return result
            DatabaseEvents.execute_insert_events(self.table,result[0],self)
            for key,value in relational_results.items():
                result[0][key] = value 

            return result[0]
        except Exception as e:
            DatabaseEvents.execute_error_events(self.table,e,self)
            result = None
        finally:
            return result

    def create_tx(self,args:dict,returning=True):
        def callback(): 
            return self.insert_one(args,returning)
        
        return self.with_transaction(callback)
    
    def create_many_tx(self,args,returning=True):
        def callback(): 
            return self.insert_many(args,returning)
        return self.with_transaction(callback)

    def query(self, q_str, args=None):
        self.connect()
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

    def get_columns_to_comma_seperated_str(self,alias=None):
    
        return ",".join([f"{alias if alias else self.table}.{column.name}" for column in self.columns.values()])

    def get_returning(self, returning):
        if not returning:
            return ""
        return "returning *"

    def get_returning_value(self, returning=True):
        if returning:
            return self.get_all()
        return True
 

    @staticmethod
    def get_update_column_to_str(col_name:str,config:any):
        if isinstance(config,dict):
            for key in SELF_UPDATE_OPERATORS:
                if key in config:
                    return f"{col_name} = {col_name} {SELF_UPDATE_OPERATORS[key]} %s",config[key]
        return f"{col_name} = %s",config

    @staticmethod
    def make_limit(limit):
        try:
            limit = int(limit)
             
            return " limit %s ",[limit]
        except:
            return "",[]
    
    @staticmethod
    def make_offset(offset):
        try:
            offset = int(offset)
            if offset < 0:
                raise Exception()
            return " offset %s ",[offset]
        except:
            return "",[]

    @staticmethod
    def relational_and_model_columns_str(model,depth:int,config:dict):
        
        model_columns_str = model.get_columns_to_comma_seperated_str(Database.make_depth_alias(model.table,depth))
       
        relational_columns = Database.get_relational_columns(config)
        
        if relational_columns:
            for index in range(len(relational_columns)):
                relational_columns[index] = "{}.{}".format(Database.make_depth_alias(relational_columns[index],depth  + 1),relational_columns[index])
            
            cols = [model_columns_str]
            cols.extend(relational_columns)
            model_columns_str = ",".join(cols)
        return model_columns_str
        
    @staticmethod
    def make_depth_alias(alias:str,depth=0):
        return "_{}_{}".format(depth,alias)

    @staticmethod
    def get_relational_columns(config:dict,alias = None):
        if not config:
            return list()
       
        return [key if not alias else "{}.{}".format(alias,key) for key in config.keys()]

    @staticmethod
    def is_optimistic_aggregate_alias(alias:str):
        return alias.endswith('_aggregate')

    @staticmethod
    def register_model(model):
        instance = model()
        Database.__models[instance.table] = model
        Database.__registered_models[instance.table] = instance

    @staticmethod
    def get_registered_model_instance(table:str):
        return Database.__registered_models.get(table,None)
    
    @staticmethod
    def get_registered_model(table:str):
        return Database.__models.get(table,None)

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

    @staticmethod
    def init(host='localhost',port='5432',user='postgres',password='postgres',pool_type='threaded',schema='public',minconn=10,maxconn=50):
        Database.schema = schema 
        if pool_type == 'threaded':
            Database.__pool = ThreadedConnectionPool(minconn,maxconn,host=host,port=port,user=user,password=password)
        else:
            Database.__pool = SimpleConnectionPool(minconn,maxconn,host=host,port=port,user=user,password=password)


allowedOrderDirectionsKeys = {
    "ASC": "asc",
    "DESC": "desc",
    "asc": "asc",
    "desc": "desc",
    "asc_nulls_first": "asc nulls first",
    "asc_nulls_last": "asc nulls last",
    "desc_nulls_first": "desc nulls first",
    "desc_nulls_last": "desc nulls last",
}

IS_ARRAY_SEARCH_OPERATOR = {
    "_in_array": True,
    "_nin_array": True,
}
IS_TEXT_SEARCH_OPERATOR = {
    "_text_search": True,
}

REQUIRE_WILDCARD_TRANSFORMATION = {
"_ilike": True,
"_nilike": True,
}

IS_JSON_ARRAY_KEY_OPERATOR = {
    "_key_exists_any": True,
    "_key_exists_all": True,
}

IS_JSON_KEY_OPERATOR = {
"_key_exists": True,
"_key_exists_any": True,
"_key_exists_all": True,
}

IS_JSON_OPERATOR = {
"_contains": True,
"_contained_in": True,
"_key_exists": True,
"_key_exists_any": True,
"_key_exists_all": True,
}

REQUIRE_CAST_TO_NULL = {
    "_is": True,
    "_is_not": True,
}

REQUIRE_ARRAY_TRANSFORMATION = {
    "_in": True,
    "_nin": True,
    "_any": True,
    "_all": True,
}

QUERY_BINDER_KEYS = {
    "_and": " and ",
    "_or": " or ",
}

WHERE_CLAUSE_OPERATORS = {
   
    "_in": " in ",
    "_nin": " not in ",
    "_lt": " < ",
    "_lte": " <= ",
    "_gt": " > ",
    "_gte": " >= ",
    "_is": " is ",
    "_is_not": " is not ",
    "_like": " like ",
    "_ilike": " ilike ",
    "_eq": " = ",
    "_neq": " <> ",
    "_in": " = any",
    "_any": " = any",
    "_nany": " <> any",
    "_all": " = all",
    "_nin": " <> all",
    "_contains": " @> ",
    "_contained_in": " <@ ",
    "_key_exists": " ? ",
    "_key_exists_any": " ?| ",
    "_key_exists_all": " ?& ",
    "_text_search": "tsquery",
    "_in_array": " = any ",
    "_nin_array": " <> any ",
}


class Aggregation:

    @staticmethod
    def is_aggregate(base_model,alias:str):
        if base_model.relations:
            if alias.endswith('_aggregate'):
                non_aggregate_part = alias.split('_aggregate')[0]
                return  base_model.relations.get(non_aggregate_part,None)
        return None 

    @staticmethod
    def make_count(model,config:dict | None):
        count = config.get('count')
        if not count:
            return [] 
        
        return["'count', count(*)"]

    @staticmethod
    def to_aggregation_multiple_columns(model,alias:str | None,agg_type:str,agg:dict):
        str_parts = []
        if alias:
            alias = f"{alias}."
        else:
            alias = ""
        for column,val in agg.items():
            if not val:
                continue
            if model.columns.get(column):  
                str_parts.append(f"'{column}',{agg_type}({alias}{column})")

        if not str_parts:
            return None
        
        return  ",".join(str_parts) 

    @staticmethod
    def make_min(model,config: dict | None,alias:str | None):
        if not config:
            return []
        sql =  Aggregation.to_aggregation_multiple_columns(model,alias,'min',config)
        if not sql:
            return []
        return [f"'min', json_build_object({sql})"]
    
    @staticmethod
    def make_max(model,config: dict | None,alias:str | None):
        if not config:
            return []
        sql =  Aggregation.to_aggregation_multiple_columns(model,alias,'max',config)
        if not sql:
            return []
        return [f"'max', json_build_object({sql})"]
    
    @staticmethod
    def make_avg(model,config: dict | None,alias:str | None):
        if not config:
            return []
        sql =  Aggregation.to_aggregation_multiple_columns(model,alias,'avg',config)
        if not sql:
            return []
        return [f"'avg', json_build_object({sql})"]
    
    @staticmethod
    def make_sum(model,config: dict | None,alias:str | None):
        if not config:
            return []
        sql =  Aggregation.to_aggregation_multiple_columns(model,alias,'sum',config)
        if not sql:
            return []
        return [f"'sum', json_build_object({sql})"]

    @staticmethod
    def make_aggregation(model,config:dict | None,alias:str):
        str_parts = list()
        str_parts.extend(Aggregation.make_count(model,config))
        str_parts.extend(Aggregation.make_min(model,config.get('min',None),alias))
        str_parts.extend(Aggregation.make_max(model,config.get('max',None),alias))
        str_parts.extend(Aggregation.make_avg(model,config.get('avg',None),alias))
        str_parts.extend(Aggregation.make_sum(model,config.get('sum',None),alias))
        if not str_parts:
            return ""
        

        combined_str = ",".join(str_parts)
        return f"json_build_object({combined_str}) as {alias}" 
    
    @staticmethod
    def build_aggregate(model,config:dict | None,relation,prev_alias:str | None,depth:int = 0):
        alias = Database.make_depth_alias(model.table if not relation else relation.alias,depth) + "_aggregate"
        args = list()
        agg_sql = Aggregation.make_aggregation(model,config,model.table+"_aggregate" if not relation else relation.alias + "_aggregate")
        if not agg_sql:
            return "",[]
        where_str,where_args = Where.make_where_clause(model,config.get('where',None),alias,depth,"and",not relation,not relation)

        if not relation:
            sql = f"""select {DistinctOn.make_distinct_on(model,config.get('distinct_on'),alias)} {agg_sql} 
            from {Database.schema}.{model.table} {alias} {where_str} {GroupBy.make_group_by(model,config.get('group_by'),alias)}
            """
        else :
            sql = f""" left outer join lateral (
            select {DistinctOn.make_distinct_on(model,config.get('distinct_on'),alias)} {agg_sql} 
            from {Database.schema}.{model.table} as {alias} where {prev_alias}.{relation.from_column} = {alias}.{relation.to_column}  {where_str} {GroupBy.make_group_by(model,config.get('group_by'))}
            )    as {alias} on true """
         
        args.extend(where_args)
        return sql,args
            

class OrderBy:
     
    @staticmethod
    def make_order_by(model,order_by:list | None,alias:str = None):
        if not order_by:
            return ""
        order_by_parts = []
        for column in order_by:
            if isinstance(column,RawSQL):
                raw_sql_str,_ = column.to_value(alias)
                order_by_parts.append(raw_sql_str)
                continue
            if not isinstance(column,dict):
                continue 
            col_name = list(column.keys())[0]
            order_by_direction = allowedOrderDirectionsKeys.get(list(column.values())[0],'asc')
            if model.columns.get(col_name):
                if alias:
                    order_by_parts.append(f"{alias}.{col_name} {order_by_direction}")
                else:
                    order_by_parts.append(f"{col_name} {order_by_direction}")
        if not order_by_parts:
            return ""
        order_by_str = ",".join(order_by_parts)

        return f" order by {order_by_str} "
    

class DistinctOn:

    @staticmethod
    def make_distinct_on(model,distinct_on:list | None,alias:str = None):
        if not distinct_on:
            return ""
        distinct_on_parts = []
        for column in distinct_on:
            if isinstance(column,RawSQL):
                raw_sql_str,_ = column.to_value(alias)
                distinct_on_parts.append(raw_sql_str)
                continue
            if not isinstance(column,str):
                continue 
             
            if model.columns.get(column):
                if alias:
                    distinct_on_parts.append(f"{alias}.{column}")
                else:
                    distinct_on_parts.append(f"{column}")
        if not distinct_on_parts:
            return ""
        distinct_on_str = ",".join(distinct_on_parts)

        return f" distinct on ({distinct_on_str}) "
    
class GroupBy:

    @staticmethod
    def make_group_by(model,group_by:list | None,alias:str = None):
        if not group_by:
            return ""
        group_by_parts = []
        for column in group_by:
            if isinstance(column,RawSQL):
                raw_sql_str,_ = column.to_value(alias)
                group_by_parts.append(raw_sql_str)
                continue
            if not isinstance(column,str):
                continue 
             
            if model.columns.get(column):
                if alias:
                    group_by_parts.append(f"{alias}.{column}")
                else:
                    group_by_parts.append(f"{column}")
        if not group_by_parts:
            return ""
        group_by_str = ",".join(group_by_parts)

        return f" group by {group_by_str} "
    

class Where:

    @staticmethod
    def to_binding_operation(str_part,is_first_entry:bool,q_binder:str = "and"):
        return str_part if is_first_entry else " {} {} ".format(q_binder,str_part)
    

    @staticmethod
    def make_where_clause(model,where:dict | None,alias:str,depth=0,q_binder="and",start_with_where=True,is_first_entry=True):
        args = list()
        if not where:
            return "",args 
        sql = " where " if start_with_where else ""

        if isinstance(where,RawSQL):
            raw_str,raw_args = where.to_value(alias)
            sql += Where.to_binding_operation(raw_str,is_first_entry,q_binder)
            args.extend(raw_args)
            return sql 
        for column,config in where.items():
            
            if column in QUERY_BINDER_KEYS:
                binding_config = config if isinstance(config,list) else [config]
                if not len(binding_config):
                    continue 
                sql_initial_str = Where.to_binding_operation(f" ( ",is_first_entry,q_binder)
                should_append = False
                for entry in binding_config:
                    
                    sql_append,append_args = Where.make_where_clause(model,entry,alias,depth,QUERY_BINDER_KEYS[column],False,True)
                    if not sql_append:
                        continue 

                    sql_initial_str += Where.to_binding_operation(sql_append,is_first_entry,QUERY_BINDER_KEYS[column])
                    is_first_entry = False
                    args.extend(append_args)
                    should_append = True
                sql+= sql_initial_str + ")" if should_append else ""
            elif column in model.columns:
                sql += Where.to_binding_operation(f"{alias}.{column}",is_first_entry,q_binder)
                is_first_entry = False 
                if isinstance(config,RawSQL):
                    raw_sql_str,raw_sql_args = config.to_value(alias)
                    if len(raw_sql_str):
                        sql += raw_sql_str
                        is_first_entry = False 
                    if raw_sql_args:
                        args.extend(raw_sql_args)
                for operator,value in config.items():
                    if operator in WHERE_CLAUSE_OPERATORS:
                        
                        operator_sql_str = WHERE_CLAUSE_OPERATORS[operator]
                        if isinstance(value,RawSQL):
                                raw_sql_str,raw_sql_args = value.to_value(alias)
                                sql += f" {operator_sql_str} {raw_sql_str} "
                                args.extend(raw_sql_args)
                                continue
                        if operator in REQUIRE_CAST_TO_NULL:
                            
                            sql += f" {operator_sql_str} null "
                        elif operator in REQUIRE_WILDCARD_TRANSFORMATION:
                            sql += f" {operator_sql_str} %s "
                            args.append(f"%{value}%")
                        elif operator in IS_ARRAY_SEARCH_OPERATOR:
                            sql += f" %s {operator_sql_str}({alias}.{column})"
                            args.append(value)
                        else:
                           
                            sql += f" {operator_sql_str} %s "
                            if isinstance(value,Column):
                                sql += f" {alias}.{value.name} "
                            else: 
                                args.append(value)
            elif column in model.relations:
                relation = model.relations[column]
                relational_model = Database.get_registered_model_instance(relation.to_table)
                if not relational_model:
                    continue
                relational_alias = Database.make_depth_alias(relation.alias,depth)
                sql_append,append_args = Where.make_where_clause(relational_model,config,relational_alias,depth+1,"and",False,False)
                relational_sql = f""" {alias}.{relation.from_column} 
                in ( select {relation.to_column} 
                from {Database.schema}.{relation.to_table} {relational_alias} 
                where {alias}.{relation.from_column} = {relational_alias}.{relation.to_column} {sql_append}) """
                args.extend(append_args)

                sql += Where.to_binding_operation(relational_sql,is_first_entry,q_binder)
                is_first_entry = False 

            else:
                continue
        

        return sql,args