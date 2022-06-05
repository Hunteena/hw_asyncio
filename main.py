import asyncio

from db import get_async_session
from swapi import get_and_write_people

PARTITION = 10


async def main():
    async_db_session = await get_async_session(True, True)
    await get_and_write_people(PARTITION, async_db_session)


if __name__ == '__main__':
    asyncio.run(main())
