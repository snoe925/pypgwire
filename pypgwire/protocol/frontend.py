import struct
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class FrontendMessage:
    pass

class SSLRequest(FrontendMessage):
    pass

class StartupMessage(FrontendMessage):
    def __init__(self, protocol: int, parameters: Dict[str, str]):
        self.protocol = protocol
        self.parameters = parameters

class Query(FrontendMessage):
    def __init__(self, query: str):
        self.query = query

class Parse(FrontendMessage):
    def __init__(self, parse: str):
        self.parse = parse

class Bind(FrontendMessage):
    def __init__(self, name: str):
        self.name = name

class Describe(FrontendMessage):
    def __init__(self, name):
        self.name = name

class Flush(FrontendMessage):
    def __init__(self):
        pass

class Sync(FrontendMessage):
    def __init__(self):
        pass

class Execute(FrontendMessage):
    def __init__(self):
        pass

class Terminate(FrontendMessage):
    def __init__(self):
        pass

class FrontendDecoder:
    FRONTEND_SSL_REQUEST = 80877103
    FRONTEND_STARTUP = 196608
    FRONTEND_CANCEL_REQUEST = 80877102

    def __init__(self):
        self.startup_seen = False
        self.ssl_negotiated = False

    def decode(self, data: bytes) -> Optional[tuple[FrontendMessage, int]]:
        if len(data) < 4:
            return None

        if not self.startup_seen:
            # Startup message: length(4) protocol(4) ...
            length = struct.unpack('>I', data[:4])[0]
            if len(data) < length:
                return None

            protocol = struct.unpack('>I', data[4:8])[0]
            if protocol == FrontendDecoder.FRONTEND_SSL_REQUEST:
                self.ssl_negotiated = True
                consumed = length
                return SSLRequest(), consumed
            elif protocol == FrontendDecoder.FRONTEND_STARTUP:
                parameters = self._read_cstring_map(data[8:])
                self.startup_seen = True
                consumed = length
                return StartupMessage(protocol, parameters), consumed
            elif protocol == FrontendDecoder.FRONTEND_CANCEL_REQUEST:
                raise NotImplementedError("CancelRequest not implemented")
            else:
                raise ValueError(f"Unknown protocol: {protocol}")

        else:
            # Regular message: type(1) length(4) payload
            message_type = data[0:1]
            logger.debug(f"{message_type=}")
            length = struct.unpack('>I', data[1:5])[0]
            if len(data) < 1 + length:
                return None

            payload = data[5:1 + length]
            consumed = 1 + length

            if message_type == b'Q':
                query = self._read_cstring(payload)
                return Query(query), consumed
            elif message_type == b'P':
                parse = self._read_cstring(payload)
                return Parse(parse), consumed
            elif message_type == b'B':
                return Bind(''), consumed
            elif message_type == b'D': # Describe
                name = self._read_cstring(payload[1:])
                return Describe(name), consumed
            elif message_type == b'H':  # Flush
                return Flush(), consumed
            elif message_type == b'S':  # Flush
                return Sync(), consumed
            elif message_type == b'E':
                return Execute(), consumed
            elif message_type == b'X':
                return Terminate(), consumed
            else:
                # For now, ignore other message types
                return None, consumed

    def _read_cstring_map(self, data: bytes) -> Dict[str, str]:
        params = {}
        i = 0
        while i < len(data):
            key = self._read_cstring(data[i:])
            if not key:
                break
            i += len(key) + 1
            value = self._read_cstring(data[i:])
            i += len(value) + 1
            params[key] = value
        return params

    def _read_cstring(self, data: bytes) -> str:
        null_pos = data.find(b'\0')
        if null_pos == -1:
            return data.decode('utf-8')
        return data[:null_pos].decode('utf-8')