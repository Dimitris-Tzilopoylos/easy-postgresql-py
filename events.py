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

        DatabaseEvents.events[event_type][table] = DatabaseEvents.events[event_type].get(
            table, list()).append(callback)

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
