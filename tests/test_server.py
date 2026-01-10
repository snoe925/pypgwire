import pytest
import asyncio
import asyncpg
import psycopg2
import struct
import logging

from pypgwire.server import start_server, DEFAULT_PORT, DEFAULT_HOST
from pypgwire.handler import ContainerHandler
from pypgwire.protocol.frontend import FrontendDecoder
from pypgwire.fake_db import USER_DATA

def server():
    ready_event = asyncio.Event()
    server_task = asyncio.create_task(
        start_server(handler=ContainerHandler(USER_DATA), ready_event=ready_event)
        )
    return (server_task, ready_event)

def sync_server():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(server())
    loop.close()

async def server_stop(server_task):
    if not server_task:
        return
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

@pytest.mark.asyncio
async def test_tcp_connect():
    (server_task, ready_event) = server()
    await ready_event.wait()
    try:
        reader, writer = await asyncio.open_connection(DEFAULT_HOST, DEFAULT_PORT)
        # Send an empty query message with an empty string
        message = struct.pack('>I', 8) + struct.pack('>I', FrontendDecoder.FRONTEND_STARTUP)
        writer.write(message)
        await writer.drain() # Ensure the data is sent
        # Receive data
        data = await reader.read(100) # Read up to 100 bytes
        logging.debug(f"Received: {data.decode()}")

        # Close the connection
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        raise e
    finally:
        await server_stop(server_task)

@pytest.mark.asyncio
async def test_asyncpg_select_query():
    (server_task, ready_event) = server()
    await ready_event.wait()

    try:
        conn = await asyncpg.connect(user='test', password='test',
                                 database='database', host='127.0.0.1', ssl=False)

        rows = await conn.fetch(
            'SELECT id, name, age, balance FROM users'
        )
        logging.debug(f"Query result: {rows}")

        # Verify results
        assert len(rows) == 3
        assert rows[0]['id'] == 1
        assert rows[0]['name'] == 'John'
        assert rows[1]['id'] == 2
        assert rows[1]['name'] == 'Jane'
        assert rows[2]['id'] == 3
        assert rows[2]['name'] == 'Joe'
        assert rows[2]['age'] == 78

        await conn.close()
        conn = None
    finally:
        if conn:
            await conn.close()
        await server_stop(server_task)


def simple_query():
    try:
        conn = psycopg2.connect(
            host='127.0.0.1',
            port=5432,
            user='postgres',
            password='password',  # Replace with actual password
            database='test',  # Replace with actual database name
            sslmode = 'disable'
        )
    except Exception as ex:
        print(ex)
    conn.autocommit = True  # Disable implicit transactions
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM users")
        result = cur.fetchall()
        logging.debug(f"Query result: {result}")
        for row in result:
            logging.debug(f"ID: {row[0]}, Name: {row[1]}")
        cur.close()
    finally:
        conn.close()

@pytest.mark.asyncio
async def test_query():
    (server_task, ready_event) = server()
    await ready_event.wait()
    try:
        await asyncio.to_thread(simple_query)
    finally:
        await server_stop(server_task)

