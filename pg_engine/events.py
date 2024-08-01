class DatabaseEvents:
    SELECT = 'SELECT'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    ERROR = 'ERROR'

    events = dict()

    @staticmethod
    def register_event(schema,table, event_type, callback):
        if not DatabaseEvents.is_valid_event_type(event_type):
            raise Exception('No such event listener')
        
        if not DatabaseEvents.events.get(event_type):
            DatabaseEvents.events[event_type] = dict()
        if not DatabaseEvents.events[event_type].get(schema):
            DatabaseEvents.events[event_type][schema] = dict()

        current_events =  DatabaseEvents.events[event_type][schema].get(
            table, list())
        
        current_events.append(callback)

        DatabaseEvents.events[event_type][schema][table] = current_events

    @staticmethod
    def get_table_action_events(schema,table, event_type):
        if not DatabaseEvents.is_valid_event_type(event_type):
            raise Exception('No such event listener')
        events_by_table = DatabaseEvents.events.get(event_type,dict()).get(schema,dict())
        return DatabaseEvents.get_table_events(table, events_by_table)

    @staticmethod
    def is_valid_event_type(event_type):
        try:
            return event_type in [DatabaseEvents.SELECT, DatabaseEvents.INSERT, DatabaseEvents.UPDATE, DatabaseEvents.DELETE, DatabaseEvents.ERROR]
        except:
            return False

    @staticmethod
    def get_table_events(table, events_by_table):
        callbacks = events_by_table.get(table, list())
        if not isinstance(callbacks, list):
            return list()

        return callbacks
    
    @staticmethod
    def execute_select_events(schema,table,data,instance):
        DatabaseEvents.execute_events(schema,table,DatabaseEvents.SELECT,data,instance)

    @staticmethod
    def execute_insert_events(schema,table,data,instance):
        DatabaseEvents.execute_events(schema,table,DatabaseEvents.INSERT,data,instance)

    @staticmethod
    def execute_update_events(schema,table,data,instance):
        DatabaseEvents.execute_events(schema,table,DatabaseEvents.UPDATE,data,instance)

    @staticmethod
    def execute_delete_events(schema,table,data,instance):
        DatabaseEvents.execute_events(schema,table,DatabaseEvents.DELETE,data,instance)
    
    @staticmethod
    def execute_error_events(schema,table,data,instance):
        DatabaseEvents.execute_events(schema,table,DatabaseEvents.ERROR,data,instance)

    @staticmethod
    def execute_events(schema:str,table:str,event_type:str,data,instance):
        try:
            events = DatabaseEvents.get_table_action_events(schema,table,event_type)
            for event in events:
                event(data,instance)
        except:
            pass



class AsyncDatabaseEvents:
    SELECT = 'SELECT'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    ERROR = 'ERROR'

    events = dict()

    @staticmethod
    def register_event(table, event_type, callback):
        if not AsyncDatabaseEvents.is_valid_event_type(event_type):
            raise Exception('No such event listener')
        
        if not AsyncDatabaseEvents.events.get(event_type):
            AsyncDatabaseEvents.events[event_type] = dict()
        current_events =  AsyncDatabaseEvents.events[event_type].get(
            table, list())
        
        current_events.append(callback)

        AsyncDatabaseEvents.events[event_type][table] = current_events

    @staticmethod
    def get_table_action_events(table, event_type):
        if not AsyncDatabaseEvents.is_valid_event_type(event_type):
            raise Exception('No such event listener')
        events_by_table = AsyncDatabaseEvents.events.get(event_type)
        return AsyncDatabaseEvents.get_table_events(table, events_by_table)

    @staticmethod
    def is_valid_event_type(event_type):
        try:
            return event_type in [AsyncDatabaseEvents.SELECT, AsyncDatabaseEvents.INSERT, AsyncDatabaseEvents.UPDATE, AsyncDatabaseEvents.DELETE, AsyncDatabaseEvents.ERROR]
        except:
            return False

    @staticmethod
    def get_table_events(table, events_by_table):
        callbacks = events_by_table.get(table, list())
        if not isinstance(callbacks, list):
            return list()

        return callbacks
    
    @staticmethod
    async def execute_select_events(table,data,instance):
        await AsyncDatabaseEvents.execute_events(table,AsyncDatabaseEvents.SELECT,data,instance)

    @staticmethod
    async def execute_insert_events(table,data,instance):
        await AsyncDatabaseEvents.execute_events(table,AsyncDatabaseEvents.INSERT,data,instance)

    @staticmethod
    async def execute_update_events(table,data,instance):
        await AsyncDatabaseEvents.execute_events(table,AsyncDatabaseEvents.UPDATE,data,instance)

    @staticmethod
    async def execute_delete_events(table,data,instance):
        await AsyncDatabaseEvents.execute_events(table,AsyncDatabaseEvents.DELETE,data,instance)
    
    @staticmethod
    async def execute_error_events(table,data,instance):
        await AsyncDatabaseEvents.execute_events(table,AsyncDatabaseEvents.ERROR,data,instance)

    @staticmethod
    async def execute_events(table:str,event_type:str,data,instance):
        try:
            events = AsyncDatabaseEvents.get_table_action_events(table,event_type)
            for event in events:
                await event(data,instance)
        except:
            pass
