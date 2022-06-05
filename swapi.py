import asyncio
import aiohttp

from db import write_to_db

URL = 'https://swapi.dev/api/people/'
SIMPLE_FIELDS = [
    'birth_year',
    'eye_color',
    'gender',
    'hair_color',
    'height',
    'mass',
    'name',
    'skin_color'
]
RETRIES = 5


class HTTPException(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message


class StarWarsSession:
    def __init__(self, async_http_session, async_db_sessionmaker):
        self.http_session = async_http_session
        self.db_session = async_db_sessionmaker

    async def get_json(self, url):
        attempt, status = 1, 0
        while attempt < RETRIES:
            async with self.http_session.get(url) as response:
                try:
                    return await response.json()
                except Exception:
                    status = response.status
                    print(f"Trying to get json from {url} - attempt {attempt}")
                    await asyncio.sleep(1)
            attempt += 1
        raise HTTPException(status, f"Couldn't get json from {url}")

    async def get_count(self):
        all_people = await self.get_json(f'{URL}')
        person_count = all_people['count']
        return person_count

    async def get_name(self, url):
        json_data = await self.get_json(url)
        return json_data['name']

    async def get_title(self, url):
        json_data = await self.get_json(url)
        title = f"Episode {json_data['episode_id']}: {json_data['title']}"
        return title

    async def get_names_str(self, urls, func):
        coros = [func(url) for url in urls]
        names = await asyncio.gather(*coros)
        return ', '.join(names)

    async def get_person(self, person_id) -> bool:
        json_data = await self.get_json(f'{URL}{person_id}')
        if json_data.get('detail') == 'Not found':
            return False

        person = {field: json_data[field] for field in SIMPLE_FIELDS}
        person['id'] = person_id

        coros = [
            self.get_name(json_data['homeworld']),
            self.get_names_str(json_data['films'], self.get_title)
        ]
        for items in ['species', 'vehicles', 'starships']:
            coros.append(
                self.get_names_str(json_data[items], self.get_name)
            )
        names = await asyncio.gather(*coros)

        person.update(zip(
            ['homeworld', 'films', 'species', 'vehicles', 'starships'],
            names
        ))
        print(f"{person_id:>2} - {person['name']} got")

        task = asyncio.create_task(write_to_db(person, self.db_session))
        await task
        return True

    async def get_and_write(self, ids):
        coros = [self.get_person(person_id) for person_id in ids]
        people = await asyncio.gather(*coros)
        return sum(people)


async def get_and_write_people(partition: int, db_session):
    async with aiohttp.ClientSession() as http_session:
        sw_session = StarWarsSession(http_session, db_session)
        person_count = await sw_session.get_count()
        start, processed = 1, 0
        while processed < person_count:
            stop = start + partition
            new = await sw_session.get_and_write(range(start, stop))
            processed += new
            start = stop
