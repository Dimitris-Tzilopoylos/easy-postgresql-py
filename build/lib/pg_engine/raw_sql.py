from typing import Callable

class RawSQL:

    raw_sql: Callable[[str], str] 
    args: list | None
    def __init__(self,sql:Callable[[str], str],args:list | None) -> None:
        self.raw_sql = sql 
        self.args = args 


    def to_value(self,alias = None):
        return self.raw_sql(alias),self.args 
    