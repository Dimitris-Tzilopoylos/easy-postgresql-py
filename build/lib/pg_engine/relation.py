from .database import Database
from .database import DistinctOn
from .database import OrderBy
from .database import GroupBy
from .database import Where

class Relation:
    def __init__(self,from_table:str,to_table:str,from_column:str,to_column:str,alias:str,type:str) -> None:
        self.from_table = from_table
        self.to_table = to_table
        self.from_column = from_column
        self.to_column = to_column 
        self.alias = alias
        self.type = type 

    def get_select_lateral_join_relational_str(self,prev_alias:str,depth:int,idx:int,config:dict):
        model = Database.get_registered_model_instance(self.to_table)
       
        if not model:
            return "",list(),idx
        coalesce_str = '->0' if self.type == 'object' else ''
        coalesce_fallback = 'null' if self.type == 'object' else '[]'
        depth_alias = self.make_depth_alias(self.alias,depth) if not Database.is_optimistic_aggregate_alias(self.alias) else  self.make_depth_alias(self.alias,depth) + "_aggregate" 
        
        model_columns_str = model.get_columns_to_comma_seperated_str(depth_alias)
        preserved_model_columns_str = model_columns_str
        if not isinstance(config,dict):
            config = dict()
        include = config.get('include',dict())
        if not isinstance(include,dict):
            include = dict()
        relational_columns = Database.get_relational_columns(include)
        preserved_relational_columns = list(relational_columns) 
        
        if relational_columns:
            for index in range(len(relational_columns)):
                relational_columns[index] = "{}.{}".format(self.make_depth_alias(relational_columns[index],depth + index + 1),relational_columns[index])
            cols = [model_columns_str]
            cols.extend(relational_columns)
            model_columns_str = ",".join(cols)

        append_sql = ''
        args = list()
        for relation_key in preserved_relational_columns:
            relation = model.relations.get(relation_key,None)
            if not relation:
                continue
            relational_config = config.get(relation_key,dict())
            if not isinstance(relational_config,dict):
                relational_config = dict()
            sql,append_args,new_index = relation.get_select_lateral_join_relational_str(depth_alias,depth + 1,idx,relational_config.get('include'))
            append_sql += sql 
            args.extend(append_args)
            idx = new_index
        where_str,where_args = Where.make_where_clause(model,config.get('where',None),depth_alias,depth,start_with_where=False,is_first_entry=False)
        limit_str,limit_args = Database.make_limit(config.get('limit',None))
        offset_str,offset_args = Database.make_offset(config.get('offset',None))
        args.extend(where_args)
        args.extend(limit_args)
        args.extend(offset_args)
        query_str = """ 
        left outer join lateral ( select coalesce( json_agg( {} ) {}, '{}' ) as {} 
        from ( 
            select row_to_json( ( 
                select {}
                from ( select {} ) {}
            ))  {}
            from (
                select {} {} from {}.{} {} where {}.{} = {}.{} {} {} {} {} {}
            ) {} {} ) {} )   as {} on true 
        """.format(depth_alias,
                   coalesce_str,
                   coalesce_fallback,
                   self.alias,
                   depth_alias,
                   model_columns_str,
                   depth_alias,
                   depth_alias,
                   DistinctOn.make_distinct_on(model,config.get('distinct_on',None)),
                   preserved_model_columns_str,
                   Database.schema,
                   self.to_table,
                   depth_alias,
                   prev_alias,
                   self.from_column,
                   depth_alias,
                   self.to_column,
                   where_str,
                   GroupBy.make_group_by(model,config.get('group_by',None),depth_alias),
                   OrderBy.make_order_by(model,config.get('order_by',None),depth_alias),
                   limit_str,
                   offset_str,
                   depth_alias,
                   append_sql,
                   depth_alias,
                   depth_alias       
        )
        return query_str,args,idx

    def make_depth_alias(self,alias:str,depth=0):
        return "_{}_{}".format(depth,alias)
    
