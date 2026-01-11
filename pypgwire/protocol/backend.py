from __future__ import annotations

import struct
from decimal import Decimal
from typing import Dict, List, Any, Type

# OID constants for PostgreSQL builtin types
OID_TEXT = 25  # text type
OID_INT8 = 20  # bigint
OID_INT2 = 21  # smallint
OID_INT4 = 23  # integer
OID_FLOAT8 = 701  # double precision (float8)
OID_NUMERIC = 1700  # numeric (a.k.a. DECIMAL)

# --- PostgreSQL NUMERIC binary format (pgwire) ---------------------------------
#
# The pgwire "binary" representation of NUMERIC is NOT IEEE, and it's also
# not a base-2 encoding. Postgres uses a base-10,000 (base 1e4) digit array.
# Each "digit" is a signed int16 (but values are 0..9999), and the header uses
# several int16 fields.
#
# On the wire, a NUMERIC value is encoded as:
#
#   int16 ndigits   : number of base-10000 digits that follow
#   int16 weight    : index of the first digit relative to the decimal point
#   int16 sign      : 0x0000 = POS, 0x4000 = NEG, 0xC000 = NaN
#   int16 dscale    : number of decimal digits after the decimal point (base-10)
#   int16 digits[]  : ndigits values, each 0..9999 (base-10000)
#
# Interpretation (conceptually):
#
#   value = sum(digits[i] * 10000^(weight - i) for i in 0..ndigits-1)
#
# dscale is used by clients to render the correct number of decimal digits and
# is not necessarily a multiple of 4.
#
# The key trick for encoding a Decimal that has a base-10 scale (dscale) which
# is not divisible by 4 is to "pad" the coefficient by multiplying by 10^pad so
# that the scale becomes a multiple of 4, then set weight accordingly. The math
# works out because:
#
#   pad = ceil(dscale/4)*4 - dscale
#   coefficient_adj = coefficient * 10^pad
#   scale_groups = ceil(dscale/4)
#
#   coefficient / 10^dscale == coefficient_adj / 10000^scale_groups
#
# This matches the Postgres base-10000 digit semantics.
#
# References (format): PostgreSQL source `src/backend/utils/adt/numeric.c`
# and pgwire type docs in the PostgreSQL protocol documentation.

NUMERIC_SIGN_POS = 0x0000
NUMERIC_SIGN_NEG = 0x4000
NUMERIC_SIGN_NAN = 0xC000


def _encode_int2(v: int) -> bytes:
    return struct.pack('>I', 2) + struct.pack('>h', v)


def _encode_int4(v: int) -> bytes:
    return struct.pack('>I', 4) + struct.pack('>i', v)


def _encode_int8(v: int) -> bytes:
    return struct.pack('>I', 8) + struct.pack('>q', v)


def _encode_float8(v: float) -> bytes:
    return struct.pack('>I', 8) + struct.pack('>d', v)


def _encode_numeric(v: Decimal) -> bytes:
    """Encode a Python Decimal into Postgres NUMERIC (OID 1700) binary format.

    Returns bytes in the format expected by `DataRow.encode()` for binary types:
    a 4-byte big-endian length prefix, followed by the NUMERIC payload.
    """

    # Note: Decimal can represent NaN, +/-Infinity depending on context.
    # Postgres NUMERIC supports NaN but does not support Infinity.
    if v.is_nan():
        payload = struct.pack('>hhhh', 0, 0, NUMERIC_SIGN_NAN, 0)
        return struct.pack('>I', len(payload)) + payload
    if v.is_infinite():
        raise ValueError('PostgreSQL NUMERIC does not support Infinity')

    # Use absolute value for digit extraction. We handle sign separately.
    sign_code = NUMERIC_SIGN_NEG if v.is_signed() else NUMERIC_SIGN_POS
    abs_v = -v if v.is_signed() else v

    tup = abs_v.as_tuple()
    # DecimalTuple: sign, digits (tuple[int]), exponent (int)
    digits10 = list(tup.digits) or [0]
    exponent = tup.exponent

    # Convert Decimal (digits10, exponent) into an integer coefficient and a
    # base-10 scale (dscale).
    #
    # Example:  Decimal('123.45') -> digits10=12345, exponent=-2
    #           coefficient=12345, dscale=2, value=coefficient/10^dscale
    #
    # Example:  Decimal('1E+3') -> digits10=1, exponent=3
    #           coefficient=1000, dscale=0
    if exponent >= 0:
        coefficient = int(''.join(str(d) for d in digits10)) * (10 ** exponent)
        dscale = 0
    else:
        coefficient = int(''.join(str(d) for d in digits10))
        dscale = -exponent

    # Normalise negative zero: Postgres treats -0 and 0 the same.
    if coefficient == 0:
        sign_code = NUMERIC_SIGN_POS

    # Postgres stores digits in base-10000, but dscale is base-10. To align the
    # decimal point to a base-10000 boundary we right-pad the coefficient by a
    # power of 10 so that dscale becomes a multiple of 4.
    scale_groups = (dscale + 3) // 4  # ceil(dscale / 4)
    pad = scale_groups * 4 - dscale   # 0..3
    coefficient_adj = coefficient * (10 ** pad)

    # Split adjusted coefficient into base-10000 digits.
    # digits10000[0] is the most-significant base-10000 digit.
    digits10000: list[int] = []
    tmp = coefficient_adj
    while tmp:
        tmp, rem = divmod(tmp, 10000)
        digits10000.append(rem)
    digits10000.reverse()

    # Strip leading and trailing zero groups to produce a compact encoding.
    # If we remove a leading digit, we must decrement weight because the first
    # remaining digit moves one base-10000 position to the right.
    weight = 0
    if digits10000:
        weight = len(digits10000) - scale_groups - 1
        while digits10000 and digits10000[0] == 0:
            digits10000.pop(0)
            weight -= 1
        while digits10000 and digits10000[-1] == 0:
            digits10000.pop()

    ndigits = len(digits10000)
    if ndigits == 0:
        # Postgres represents 0 as an empty digit array.
        weight = 0

    payload = struct.pack('>hhhh', ndigits, weight, sign_code, dscale)
    for d in digits10000:
        payload += struct.pack('>h', d)
    return struct.pack('>I', len(payload)) + payload

# Encoder functions for binary formats
ENCODERS = {
    OID_INT2: _encode_int2,
    OID_INT4: _encode_int4,
    OID_INT8: _encode_int8,
    OID_FLOAT8: _encode_float8,
    OID_NUMERIC: _encode_numeric,
}

def to_sqltype(index: int, field_name: str, schema: list[int] = None, typ: Type | None = None) -> dict[str, Any]:
    '''
    Create a wire protocol type from a python type
    If provided use the schema which is a list of Python OIDs
    for builtin types.
    '''
    oid = OID_TEXT  # default to text
    format = 0
    type_size = -1
    if schema is not None and 0 <= index < len(schema):
        oid = schema[index]
        if oid in [OID_INT8, OID_INT2, OID_INT4, OID_FLOAT8, OID_NUMERIC]:  # int8, int2, int4, float8, numeric
            format = 1 # binary pgasync expects binary
    elif typ == int:  # noqa: E721
        oid = OID_INT4  # int4
        format = 1 # binary
        type_size = 4
    elif typ == float:  # noqa: E721
        oid = OID_FLOAT8  # float8
        format = 1  # binary
        type_size = 8
    elif typ == Decimal:  # noqa: E721
        oid = OID_NUMERIC
        format = 1  # binary
    return {
        'name': field_name,
        'table_oid': 0,
        'column_attr': 0,
        'type_oid': oid,
        'type_size': type_size,
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
    def __init__(self, values: List[Any], fields: List[Dict[str, Any]], text_encoding: bool):
        self.values = values
        self.fields = fields
        self.text_encoding = text_encoding

    def encode(self) -> bytes:
        # 'D' length(4) num_values(2) values...
        # each value: len(4) data or -1 for null
        data = struct.pack('>H', len(self.values))
        for i, value in enumerate(self.values):
            if value is None:
                data += struct.pack('>I', -1)
            else:
                field = self.fields[i]
                oid = field['type_oid']
                format_code = field['format']
                if self.text_encoding or format_code == 0:
                    val_bytes = str(value).encode('utf-8')
                    data += struct.pack('>I', len(val_bytes)) + val_bytes
                else:
                    val_bytes = ENCODERS[oid](value)
                    data += val_bytes
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
