class Column:
    def __init__(self, name='', type='text', unique=False, nullable=False, default_value=None, primary=False, auto_increment=False) -> None:
        self.name = name
        self.type = type
        self.unique = unique
        self.auto_increment = auto_increment
        self.nullable = nullable
        self.primary = primary
        self.default_value = default_value

    def get_create_table_sql(self):
        null = "" if self.nullable else "not null"
        default_value = "" if not self.default_value else "default {}".format(
            self.defaultValue)
        primary_key = "primary key" if self.primary else ""
        unique = "unique" if self.unique else ""
        return "{} {} {} {} {} {}".format(self.name, self.type, primary_key, null, unique, default_value,)
