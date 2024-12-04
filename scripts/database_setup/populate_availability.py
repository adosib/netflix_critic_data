import os
import logging
import psycopg
import asyncio
import aiohttp
import aiofiles
# from pyjsparser import parse
from random import randint
from dataclasses import dataclass
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from pathlib import Path 
from psycopg import sql
from scripts.rate_limiter_cls import ThrottledClientSession


THIS_DIR = Path(__file__).parent
ROOT_DIR, *_ = [parent for parent in THIS_DIR.parents if parent.stem == "netflix_critic"]
SAVETO_DIR = ROOT_DIR / "data" / "raw" / "title"
LOG_DIR = ROOT_DIR / "logs"
SCRAPEOPS_API_KEY = os.getenv("SCRAPEOPS_API_KEY")

log_file = LOG_DIR / f'{datetime.now().strftime('%Y%m%d%H%M%S')}.log'
logging.basicConfig(
    filename=log_file,
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)


@dataclass
class NetflixResponse:
    response: aiohttp.ClientResponse
    response_body: str
    available: bool
        
async def insert_into_database(connection: psycopg.Connection, cursor: psycopg.Cursor, record: dict):
    insert_query = sql.SQL("INSERT INTO availability ({fields}) VALUES ({values})").format(
        fields=sql.SQL(', ').join(map(sql.Identifier, record.keys())),
        values=sql.SQL(', ').join((record.values()))
    )
    logging.info(f"Now executing: {insert_query.as_string(connection)}")
    cursor.execute(insert_query)
    
async def save_response_body(response_body: str, filepath: Path):
    async with aiofiles.open(str(filepath), "w+") as f:
        await f.write(response_body)
    
async def response_indicates_available_title(response: aiohttp.ClientResponse):
    if response.status == 404:
        return False
    elif 'origId' in response.url.query:  # for unavailable titles, /watch redirects to 0?origId=<id>
        return False
    elif BeautifulSoup(await response.text(), 'html.parser').find('div', class_="error-page"):
        return False 
    return response.ok

async def get_fake_session_headers(session):
    
    global session_headers_bag
    session_headers_bag = []
    
    logging.warning("CALLING get_fake_session_headers()")
    
    async with session.get(
        'https://headers.scrapeops.io/v1/browser-headers', 
        params={
            'api_key': SCRAPEOPS_API_KEY,
            'num_results': '100'
        }
    ) as response:
        headers_json = await response.json()
        for headers in headers_json['result']:
            # If you navigate with a mobile device, Netflix really insists you download their app :)
            if 'Mobile;' in headers['user-agent']:
                continue
            # elif headers.get('sec-ch-ua-platform') == '"Android"':
            #     continue # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Sec-CH-UA-Platform
            elif headers.get('sec-ch-ua-mobile', '?0') == '?0':  # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Sec-CH-UA-Mobile
                session_headers_bag.append(headers)
                
async def pick_session_headers():
    random_index = randint(0, len(session_headers_bag) - 1)
    return session_headers_bag[random_index]

async def get_netflix(session: aiohttp.ClientSession, headers: dict, request_url: str):
    try:
        async with session.get(request_url, headers=headers) as response:
            logging.info(f"Starting request for {request_url}")
            status = response.status
            text = await response.text()
            available = await response_indicates_available_title(response)
            
            if status not in (200, 301, 302, 404):
                raise ValueError(f"Unexpected HTTP status for {request_url}: {status}")
            
        return NetflixResponse(
            response=response, 
            response_body=text, 
            available=available
        )
        
    except aiohttp.client_exceptions.NonHttpUrlRedirectClientError as err:
        logging.warning(f"Apparently Netflix does not like the header: {headers}")
        raise
    
async def run(netflix_id, session, dbconn, dbcur):
    
    request_url = f"https://www.netflix.com/title/{netflix_id}"
    while True:
        try:
            # TODO try getting away with the default headers to see if fake headers are necessary
            headers = await pick_session_headers()
            response = await get_netflix(session, headers, request_url)
            break
        except aiohttp.client_exceptions.NonHttpUrlRedirectClientError as err:
            logging.exception(err)
        except aiohttp.client_exceptions.ServerDisconnectedError as err:
            logging.exception(err)
            raise
    
    html = response.response_body
    available = response.available
    checked_at = datetime.now(timezone.utc)
    data = {"netflix_id": netflix_id, "region_code": 'MO', "country": 'US', "available": available, "checked_at": checked_at}
    
    async with asyncio.TaskGroup() as tg:
        if available:
            filepath = SAVETO_DIR / f"{netflix_id}.html"
            tg.create_task(save_response_body(html, filepath))
        
        tg.create_task(insert_into_database(dbconn, dbcur, data))
    
async def main():
    background_tasks = set()
    responses = []
    
    with psycopg.Connection.connect("dbname=postgres user=postgres", autocommit=True) as dbconn:
        with dbconn.cursor() as dbcur:
            dbcur.execute("""
                SELECT DISTINCT titles.netflix_id
                FROM titles
                LEFT JOIN availability 
                    ON availability.netflix_id = titles.netflix_id
                WHERE availability.id IS NULL;
            """)
            concurrency_limit = 5
            connector = aiohttp.TCPConnector(
                limit=concurrency_limit, 
                limit_per_host=concurrency_limit
            )
            # There's some weird timeout stuff that happens here 
            # which necessitated the need for the ClientTimeout instance:
            # https://docs.aiohttp.org/en/stable/client_quickstart.html#aiohttp-client-timeouts
            # https://stackoverflow.com/questions/64534844/python-asyncio-aiohttp-timeout
            # https://github.com/aio-libs/aiohttp/issues/3203
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)
            # <10 requests/second seems to be about what Netflix tolerates before forcefully terminating the session
            async with ThrottledClientSession(rate_limit=concurrency_limit, connector=connector, timeout=timeout) as session:
                
                await get_fake_session_headers(session)
                                
                for netflix_id, *_ in dbcur:
                        
                    task = asyncio.create_task(
                        run(netflix_id, session, dbconn, dbcur), 
                        name=str(netflix_id)
                    )
                    background_tasks.add(task)
                    task.add_done_callback(background_tasks.discard)

                responses.extend(await asyncio.gather(*background_tasks))
                
    return responses


if __name__ == "__main__":
    asyncio.run(main(), debug=True)