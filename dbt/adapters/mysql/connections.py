from dataclasses import dataclass
from contextlib import contextmanager
import agate
from typing import Optional

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.helper_types import Port
from dbt.logger import GLOBAL_LOGGER as logger

import mysql.connector

MYSQL_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'host': {
            'type': 'string'
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535
        },
        'database': {
            'type': 'string'
        },
        'username': {
            'type': 'string'
        },
        'password': {
            'type': 'string'
        }
    },
    'required': ['host', 'database', 'username', 'password']
}


@dataclass
class MySQLCredentials(Credentials):
    # TODO: in mysql, schema == database. but they're both required in Credentials.
    # need to figure out how to override that. for now, i'm adding both to my profile
    host: str
    database: str
    username: str
    password: str
    port: Optional[Port] = 3306

    _ALIASES = {
        # 'schema': 'database',
        'dbname': 'database',
        'pass': 'password',
        'user': 'username'
    }

    @property
    def type(self):
        return 'mysql'

    def _connection_keys(self):
        return ('host', 'port', 'database', 'username')


class MySQLConnectionManager(SQLConnectionManager):
    TYPE = 'mysql'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open')
            return connection

        credentials = connection.credentials

        try:
            conn = mysql.connector.connect(
                host=credentials.host,
                port=credentials.port,
                database=credentials.database,
                user=credentials.username,
                password=credentials.password
            )
            connection.state = 'open'
            connection.handle = conn
        except Exception as e:
            # TODO: enumerate the exceptions, add any specific handling
            connection.state = 'fail'
            connection.handle = None
            raise dbt.exceptions.FailedToConnectException(e) from e
            
        return connection

    @classmethod
    def get_status(cls, cursor):
        # TODO: can we get a status?
        # the API docs say cursor.fetchwarnings() is an option...
        # could check that, then default to ok
        return 'OK'

    def cancel(self, connection):
        # TODO: mysql doesn't support query cancellation mid-way through, dbt docs mention care with this method
        connection_id = connection.handle.connection_id

        # "kill query" kills the running statement, but leaves the connection
        sql = f'kill query {connection_id}'
        logger.debug(f'Cancelling query on connection_id {connection_id}')
        _, cursor = self.add_query(sql, 'master')
        res = cursor.fetchone()
        logger.debug(f"Cancelled query on {connection_id}: {res}")

    @contextmanager
    def exception_handler(self, sql, connection_name='master'):
        try:
            yield
        # TODO: enumerate the possible exceptions, add specific handling
        except Exception as e:
            logger.debug(f'Error running SQL: {sql}')
            logger.debug(f'Rolling back transaction')
            self.release()
            raise dbt.exceptions.RuntimeException(e) from e

