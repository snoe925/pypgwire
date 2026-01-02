import asyncio
import logging
import socket
from typing import Optional, Any
from .protocol.frontend import FrontendDecoder
from .protocol.frontend import FrontendMessage, SSLRequest, StartupMessage, Query, Parse, Bind, Flush, Terminate, Execute
from .protocol.backend import ReadyForQuery

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DEFAULT_PORT = 5432
DEFAULT_HOST = "127.0.0.1"

class PostgresProtocol(asyncio.Protocol):
    def __init__(self, handler: Any):
        self.handler = handler
        self.decoder = FrontendDecoder()
        self.buffer = b''
        self.transport: Optional[asyncio.Transport] = None

    def connection_made(self, transport: asyncio.Transport) -> None:
        self.transport = transport
        sock = transport.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        logger.info('Connection established')

    def data_received(self, data: bytes) -> None:
        self.buffer += data
        if data is not None and len(data) > 0:
            cmd = chr(data[0])
            logger.debug(f"Command: {cmd}")

        while True:
            result = self.decoder.decode(self.buffer)
            if result is None:
                break

            message, consumed = result
            self.buffer = self.buffer[consumed:]

            # Handle the message
            self._handle_message(message)

    def _handle_message(self, message: FrontendMessage) -> None:
        try:
            if isinstance(message, SSLRequest):
                # Respond with 'N' for no SSL
                self.transport.write(b'N')
                return
            elif isinstance(message, Flush):
                return
            # handlers with responses
            responses = []
            if isinstance(message, StartupMessage):
                responses = self.handler.handle_startup(message)
            elif isinstance(message, Query):
                responses = self.handler.handle_query(message)
            elif isinstance(message, Parse):
                responses = self.handler.handle_parse(message)
            elif isinstance(message, Bind):
                responses = self.handler.handle_bind(message)
            elif isinstance(message, Execute):
                responses = self.handler.handle_execute(message)
            elif isinstance(message, Terminate):
                responses = [ReadyForQuery()]
            # write the response bytes
            for response in responses:
                self.transport.write(response.encode())
            # Close the connection on termination
            if isinstance(message, Terminate):
                self.transport.close()
        except Exception as e:
            logger.error(f"Error handling message {message}: {e}")
            # For now, just close on error
            self.transport.close()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        logger.info('Connection lost')


async def start_server(handler: Any, host: str = None, port: int = 0, ready_event: Optional[asyncio.Event] = None):
    if not host:
        host = DEFAULT_HOST
    if port <= 0:
        port = DEFAULT_PORT
    if handler is None:
        raise ValueError('handler cannot be None')

    loop = asyncio.get_event_loop()
    try:
        server = await loop.create_server(
            lambda: PostgresProtocol(handler),
            host, port
        )
    except Exception as e:
        logger.fatal(f"Could not start server: {e}")
        raise

    if ready_event is not None:
        ready_event.set()

    logger.info(f'Server listening on {host}:{port}')

    try:
        await server.serve_forever()
    except KeyboardInterrupt:
        logger.info('Shutting down server')
        server.close()
        await server.wait_closed()

