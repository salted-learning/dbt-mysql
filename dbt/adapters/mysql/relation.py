from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy

@dataclass
class MySQLIncludePolicy(Policy):
    database: bool = True
    schema: bool = False
    identifier: bool = True

@dataclass(frozen=True, eq=False, repr=False)    
class MySQLRelation(BaseRelation):
    quote_character: str = '`'
    include_policy: MySQLIncludePolicy = MySQLIncludePolicy()
    
