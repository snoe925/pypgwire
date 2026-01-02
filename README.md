pypgwire - A minimal implementation of the Postgresql wire protocol

# Introduction
A basic implementation of a Postgresql wire protocol server. Implement data services in plain async python for
standard Posgresql clients.

Let your clients be Postgresql clients: psycopg, asyncpg, Java clients, C library clients.  Skip the REST protocol.

# Code
```
# Create an asyncio server on 127.0.0.1 port 5432
USER_DATA = [{"item":"Apple"},{"item":"Pear"}]
ready_event = asyncio.Event()
server_task = asyncio.create_task(
    start_server(handler=ContainerHandler(USER_DATA), ready_event=ready_event)
    )
await ready_event.wait()
```

# TODOs
First commit, so just about everything else.  A basic smoke test works.

# Related repos
Several projects inspired or materially helped to write this code.

https://github.com/GavinRay97/PgProtoJ.git

https://github.com/jwills/buenavista.git

https://github.com/roapi/roapi
