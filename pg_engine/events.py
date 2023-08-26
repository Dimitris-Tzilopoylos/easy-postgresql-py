class DatabaseEvents:
    SELECT = 'SELECT'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    ERROR = 'ERROR'

    events = dict()

    @staticmethod
    def register_event(table, event_type, callback):
        if not DatabaseEvents.is_valid_event_type(event_type):
            raise Exception('No such event listener')
        
        if not DatabaseEvents.events.get(event_type):
            DatabaseEvents.events[event_type] = dict()
        current_events =  DatabaseEvents.events[event_type].get(
            table, list())
        
        current_events.append(callback)

        DatabaseEvents.events[event_type][table] = current_events

    @staticmethod
    def get_table_action_events(table, event_type):
        if not DatabaseEvents.is_valid_event_type(event_type):
            raise Exception('No such event listener')
        events_by_table = DatabaseEvents.events.get(event_type)
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
    def execute_select_events(table,data,instance):
        DatabaseEvents.execute_events(table,DatabaseEvents.SELECT,data,instance)

    @staticmethod
    def execute_insert_events(table,data,instance):
        DatabaseEvents.execute_events(table,DatabaseEvents.INSERT,data,instance)

    @staticmethod
    def execute_update_events(table,data,instance):
        DatabaseEvents.execute_events(table,DatabaseEvents.UPDATE,data,instance)

    @staticmethod
    def execute_delete_events(table,data,instance):
        DatabaseEvents.execute_events(table,DatabaseEvents.DELETE,data,instance)
    
    @staticmethod
    def execute_error_events(table,data,instance):
        DatabaseEvents.execute_events(table,DatabaseEvents.ERROR,data,instance)

    @staticmethod
    def execute_events(table:str,event_type:str,data,instance):
        try:
            events = DatabaseEvents.get_table_action_events(table,event_type)
            for event in events:
                event(data,instance)
        except:
            pass
