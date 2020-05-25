from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager

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
    SCHEMA = MYSQL_CREDENTIALS_CONTRACT
    ALIASES = {
        'schema': 'database'
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
                port=credentials.get('port', 3306),
                database=credentials.database,
                user=credentials.username,
                password=credentials.password
            )
            connection.state = 'open'
            connection.handle = conn
        except:
            # TODO: enumerate the exceptions, add any specific handling
            connection.state = 'fail'
            connection.handle = None
            raise dbt.exceptions.FailedToConnectException()
            
        return connection

    @classmethod
    def get_status(cls, cursor):
        # TODO: can we get a status?
        # the API docs say cursor.fetchwarnings() is an option...
        # could check that, then default to ok
        return 'OK'

    def cancel(self, connection):
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
            self.release(connection_name)
            raise dbt.exceptions.RuntimeException(str(e))

