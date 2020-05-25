import agate

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.mysql import MySQLConnectionManager
from dbt.adapters.mysql.relation import MySQLRelation

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.adapters.base.meta import available
from dbt.exceptions import get_relation_returned_multiple_results
from typing import Optional, List
from dbt.adapters.base.relation import BaseRelation


class MySQLAdapter(SQLAdapter):
    ConnectionManager = MySQLConnectionManager
    Relation = MySQLRelation

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

    def quote(self, identifier):
        return '`{}`'.format(identifier)

    @available.parse_none
    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> Optional[BaseRelation]:
        relations_list = self.list_relations(database, schema)

        # TODO: passing schema=None here is a hack to make the matching process work
        # the better solution would be to ensure the schema passed into get_relation()
        # is null. It will require fixing a function in self.list_relations() above,
        # which requires a str value (it runs lower() function)
        #
        # THIS IS THE ONLY MODIFICATION TO THIS FUNCTION IN THE BASE CLASS
        matches = self._make_match(relations_list, database, None,
                                   identifier)

        if len(matches) > 1:
            kwargs = {
                'identifier': identifier,
                'schema': schema,
                'database': database,
            }
            get_relation_returned_multiple_results(
                kwargs, matches
            )

        elif matches:
            return matches[0]

        return None

    def list_relations(self, database: str, schema: str) -> List[BaseRelation]:
        if self._schema_is_cached(database, schema):
            # TODO: schema=None is a hack; see get_relation() for more details
            #
            # THIS IS THE ONLY MODIFICATION TO THIS FUNCTION IN THE BASE CLASS
            return self.cache.get_relations(database, schema=None)

        information_schema = self.Relation.create(
            database=database,
            schema=schema,
            identifier='',
            quote_policy=self.config.quoting
        ).information_schema()

        # we can't build the relations cache because we don't have a
        # manifest so we can't run any operations.
        relations = self.list_relations_without_caching(
            information_schema, schema
        )

        logger.debug('with database={}, schema={}, relations={}'
                     .format(database, schema, relations))
        return relations
