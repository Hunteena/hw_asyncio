import asyncio

from sqlalchemy import Integer, String, Column, Text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import config

engine = create_async_engine(config.PG_DSN_ALC)
Base = declarative_base()


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String(16))
    eye_color = Column(String(128))
    films = Column(Text)
    gender = Column(String(128))
    hair_color = Column(String(128))
    height = Column(String(16))
    homeworld = Column(String(128))
    mass = Column(String(16))
    name = Column(String(128))
    skin_color = Column(String(128))
    species = Column(Text)
    starships = Column(Text)
    vehicles = Column(Text)


async def get_async_session(drop: bool = False, create: bool = False):
    async with engine.begin() as conn:
        if drop:
            print('Dropping tables...')
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            print('Creating tables...')
            await conn.run_sync(Base.metadata.create_all)
    async_session_maker = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
    return async_session_maker


async def write_to_db(person_data, db_session):
    async with db_session() as session:
        async with session.begin():
            if person_data:
                person = People(**person_data)
                session.add(person)
    print(f"{person.id:>40} - {person.name} written")


async def main():
    await get_async_session(True, True)


if __name__ == '__main__':
    asyncio.run(main(), debug=True)
