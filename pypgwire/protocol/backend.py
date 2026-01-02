import struct
from typing import Dict, List, Any

def to_sqltype(index: int, field_name: str, schema: list[int] = None) -> dict[str, Any]:
    '''
    Create a wire protocol type from a python type
    If provided use the schema which is a list of Python OIDs
    for builtin types.
    '''
    oid = 25 # default to text
    format = 0
    if schema is not None:
        oid = schema[index]
        format = 0 # TODO
    return {
        'name': field_name,
        'table_oid': 0,
        'column_attr': 0,
        'type_oid': oid,
        'type_size': -1,
        'type_mod': -1,
        'format': format
        }

class BackendMessage:
    def encode(self) -> bytes:
        raise NotImplementedError

class AuthenticationOk(BackendMessage):
    def encode(self) -> bytes:
        # 'R' length(4) auth_type(4)
        return b'R' + struct.pack('>II', 8, 0)

class BackendKeyData(BackendMessage):
    def __init__(self, pid: int, key: int):
        self.pid = pid
        self.key = key

    def encode(self) -> bytes:
        # 'K' length(4) pid(4) key(4)
        return b'K' + struct.pack('>III', 12, self.pid, self.key)

class ReadyForQuery(BackendMessage):
    def __init__(self, status: bytes = b'I'):
        self.status = status

    def encode(self) -> bytes:
        # 'Z' length(4) status(1)
        # This can be I for idle, T for in transaction T or E in error transaction block
        return b'Z' + struct.pack('>IB', 5, self.status[0])

class RowDescription(BackendMessage):
    def __init__(self, fields: List[Dict[str, Any]]):
        self.fields = fields

    def encode(self) -> bytes:
        # 'T' length(4) num_fields(2) fields...
        # each field: name(cstring) table_oid(4) column_attr(2) type_oid(4) type_size(2) type_mod(4) format(2)
        data = struct.pack('>H', len(self.fields))
        for field in self.fields:
            name = field['name'].encode('utf-8') + b'\0'
            data += name
            data += struct.pack('>Ih',
                                field['table_oid'],
                                field['column_attr'])
            data += struct.pack('>Ih',
                                field['type_oid'],
                                field['type_size'])
            data += struct.pack('>ih',
                                field['type_mod'],
                                field['format'])
        length = 4 + len(data)
        return b'T' + struct.pack('>I', length) + data

class DataRow(BackendMessage):
    def __init__(self, values: List[Any]):
        self.values = values

    def encode(self) -> bytes:
        # 'D' length(4) num_values(2) values...
        # each value: len(4) data or -1 for null
        data = struct.pack('>H', len(self.values))
        for value in self.values:
            if value is None:
                data += struct.pack('>I', -1)
            else:
                val_bytes = str(value).encode('utf-8')
                data += struct.pack('>I', len(val_bytes)) + val_bytes
        length = 4 + len(data)
        return b'D' + struct.pack('>I', length) + data

class ErrorResponse(BackendMessage):
    def __init__(self, severity: str, code: str, message: str):
        self.fields = [
            (b'S', severity.encode('utf-8')),
            (b'C', code.encode('utf-8')),
            (b'M', message.encode('utf-8')),
        ]

    def encode(self) -> bytes:
        # 'E' length(4) fields...
        data = b''
        for tag, value in self.fields:
            data += tag + value + b'\0'
        data += b'\0'
        length = 4 + len(data)
        return b'E' + struct.pack('>I', length) + data

class CommandComplete(BackendMessage):
    def __init__(self, rows: int, command: bytes):
        self.rows = rows
        self.command = command

    def encode(self) -> bytes:
        # 'C' length(4) tag(cstring)
        tag = f"{self.command.decode()} {self.rows}".encode('utf-8') + b'\0'
        length = 4 + len(tag)
        return b'C' + struct.pack('>I', length) + tag

class ParameterStatus(BackendMessage):
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value

    def encode(self) -> bytes:
        # 'S' length(4) name(cstring) value(cstring)
        name_bytes = self.name.encode('utf-8') + b'\0'
        value_bytes = self.value.encode('utf-8') + b'\0'
        data = name_bytes + value_bytes
        length = 4 + len(data)
        return b'S' + struct.pack('>I', length) + data

class ParseComplete(BackendMessage):
    def __init__(self):
        pass

    def encode(self) -> bytes:
        # '1' length(4)
        return b'1' + struct.pack('>I', 4)

class ParameterDescription(BackendMessage):
    def __init__(self, parameter_types):
        pass

    def encode(self) -> bytes:
        # '1' length(4)
        return b't' + struct.pack('>I', 6) + struct.pack('>H', 0)

class BindComplete(BackendMessage):
    def __init__(self):
        pass

    def encode(self) -> bytes:
        # '2' length(4)
        return b'2' + struct.pack('>I', 4)

class EmptyQueryResponse(BackendMessage):
    def __init__(self):
        pass

    def encode(self) -> bytes:
        # 'I' length(4)
        return b'I' + struct.pack('>I', 4)
