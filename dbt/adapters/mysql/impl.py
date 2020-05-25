from dbt.adapters.sql import SQLAdapter
from dbt.adapters.mysql import MySQLConnectionManager


class MySQLAdapter(SQLAdapter):
    ConnectionManager = MySQLConnectionManager

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def convert_number_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        # TODO: handle different precisions?
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "int"

    @classmethod
    def convert_datetime_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "datetime"
