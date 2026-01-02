from dataclasses import asdict
import logging
import re
from typing import Container, List, Generic, TypeVar, Any, Callable
from .protocol.frontend import Query, StartupMessage, SSLRequest
from .protocol.backend import (
    BackendMessage, ReadyForQuery,
    RowDescription, DataRow, CommandComplete, BindComplete,
    ParseComplete, ParameterDescription, to_sqltype,
    BackendKeyData, ParameterStatus, AuthenticationOk,
    ErrorResponse
    )

T = TypeVar('T')

class GeneratorHandler:
    def __init__(self, factory: Callable[[str],Container[T]], cls: Generic[T] = None):
        if cls is not None:
            logger_name = __name__ + "__" + cls.name
            self.table_name = cls.__name__.lower()
        else:
            logger_name = __name__
            self.table_name = None
        self.logger = logging.getLogger(logger_name)
        if cls is not None:
            # use the passed annotated class
            self.columns = [field.lower() for field in cls.__annotations__.keys()]
        else:
            # infer the types from the first element of the data
            first = factory(None)[0]
            if isinstance(first, dict):
                # first is a dict just use the keys
                self.columns = [field.lower() for field in first.keys()]
            else:
                # assume an annotated class
                self.columns = [field.lower() for field in first.__annotations__.keys()]
        self.factory = factory
        self.fields = []
        for ii, field_name in enumerate(self.columns):
            self.fields.append(to_sqltype(ii, field_name))

    def handle_query(self, msg: Query) -> List[BackendMessage]:
        query = msg.query.strip().upper()
        if query.upper().startswith('SELECT'):
            return self._handle_select(msg.query)
        else:
            raise NotImplementedError(f"Unsupported query: {query}")

    def _data_to_messages(self, sql: str = None):
        # Get rows as pgwire data
        # TODO get sql string from connection state
        rows_data: List[List[Any]] = []
        data = self.factory(sql)
        for row in data:
            row_dict = asdict(row)
            row_values = [row_dict.get(col.lower()) for col in self.columns]
            rows_data.append(row_values)

        messages = [RowDescription(self.fields)]
        for row_values in rows_data:
            messages.append(DataRow(row_values))
        messages.append(CommandComplete(len(rows_data), b'SELECT'))
        messages.append(ReadyForQuery())

        return messages

    def _handle_select(self, query: str) -> List[BackendMessage]:
        # Simple parsing: SELECT columns FROM table
        match = re.match(r'SELECT\s+(.+?)\s+FROM\s+(\w+)', query.upper())
        if not match:
            raise ValueError(f"Invalid SELECT query: {query}")

        columns_str, table_name = match.groups()
        table_name = table_name.lower()

        if self.table_name and self.table_name not in table_name:
            raise ValueError(f"table name does not match {table_name} expected {self.table_name}")

        messages = self._data_to_messages(query)
        return messages

    def handle_bind(self, msg):
        self.logger.debug("handle_bind")
        return [BindComplete()]

    def handle_execute(self, msg):
        self.logger.debug("handle_execute")
        # TODO get the sql from the connection state
        messages = self._data_to_messages()
        return messages

    def handle_parse(self, msg) -> List[BackendMessage]:
        self.logger.info("handle_parse")
        return [
            ParseComplete(),
            ParameterDescription([]),
            RowDescription(self.fields)
            ]

    def handle_ssl_request(self, msg: SSLRequest) -> List[BackendMessage]:
        # TODO add support for TLS upgrade
        return [ErrorResponse('FATAL', '08006', 'SSL not supported')]

    def handle_startup(self, msg: StartupMessage) -> List[BackendMessage]:
        # TODO the implementor may want to pick the date format
        return [
            AuthenticationOk(),
            ParameterStatus("server_version", "9.2"),
            ParameterStatus("server_encoding", "UTF8"),
            ParameterStatus("client_encoding", "UTF8"),
            ParameterStatus("DateStyle", "ISO YMB"),
            BackendKeyData(1, 2),
            ReadyForQuery()
        ]

'''
Generates a Postgres table from a non-empty collection.
When the collection is empty we cannot infer the types, from the contents.
In that case, you need to provide a type annotated class for the schema.
'''
class ContainerHandler(GeneratorHandler):
    def __init__(self, container: Container[T], cls: Generic[T] = None):
        if not container:
            raise ValueError("container cannot be empty")
        # ignore the sql and return a constant dataset
        super().__init__(lambda _sql: container)
 