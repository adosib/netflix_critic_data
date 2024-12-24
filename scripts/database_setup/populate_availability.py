import os
import asyncio
import logging

# from pyjsparser import parse
from random import randint
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass

import aiohttp
import psycopg
import aiofiles
from bs4 import BeautifulSoup
from psycopg import sql
from aiolimiter import AsyncLimiter

THIS_DIR = Path(__file__).parent
ROOT_DIR, *_ = [
    parent for parent in THIS_DIR.parents if parent.stem == "netflix_critic_data"
]
TITLEPAGE_SAVETO_DIR = ROOT_DIR / "data" / "raw" / "title"
WATCHPAGE_SAVETO_DIR = ROOT_DIR / "data" / "raw" / "watch"
LOG_DIR = ROOT_DIR / "logs"

SCRAPEOPS_API_KEY = os.environ["SCRAPEOPS_API_KEY"]
COOKIE = {"Cookie": os.environ["NETFLIX_COOKIE"]}
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Host": "www.netflix.com",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-model": '""',
    "sec-ch-ua-platform": '"macOS"',
    "sec-ch-ua-platform-version": '"15.1.0"',
}

log_file = LOG_DIR / f'{datetime.now().strftime('%Y%m%d%H%M%S')}.log'
logging.basicConfig(
    filename=log_file,
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


@dataclass
class NetflixResponse:
    url: str
    response: aiohttp.ClientResponse
    response_body: str
    available: bool


async def insert_into_database(cursor: psycopg.Cursor, record: dict):
    insert_query = sql.SQL(
        "INSERT INTO availability ({fields}) VALUES ({values})"
    ).format(
        fields=sql.SQL(", ").join(map(sql.Identifier, record.keys())),
        values=sql.SQL(", ").join((record.values())),
    )
    logging.info(f"Now executing: {insert_query.as_string()}")
    cursor.execute(insert_query)


async def save_response_body(response_body: str, filepath: Path):
    async with aiofiles.open(str(filepath), "w+") as f:
        await f.write(response_body)


async def response_indicates_available_title(response: aiohttp.ClientResponse):
    if response.status == 404:
        return False
    elif (
        "origId" in response.url.query
    ):  # for unavailable titles, /watch redirects to 0?origId=<id>
        return False
    elif BeautifulSoup(await response.text(), "html.parser").find(
        "div", class_="error-page"
    ):
        return False
    return response.ok


async def get_fake_session_headers(session):
    global session_headers_bag
    session_headers_bag = []

    logging.warning("CALLING get_fake_session_headers()")

    async with session.get(
        "https://headers.scrapeops.io/v1/browser-headers",
        params={"api_key": SCRAPEOPS_API_KEY, "num_results": "100"},
    ) as response:
        headers_json = await response.json()
        for headers in headers_json["result"]:
            # If you navigate with a mobile device, Netflix really insists you download their app :)
            if "Mobile;" in headers["user-agent"]:
                continue
            # elif headers.get('sec-ch-ua-platform') == '"Android"':
            #     continue # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Sec-CH-UA-Platform
            elif (
                headers.get("sec-ch-ua-mobile", "?0") == "?0"
            ):  # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Sec-CH-UA-Mobile
                session_headers_bag.append(headers)


async def pick_session_headers():
    random_index = randint(0, len(session_headers_bag) - 1)
    return session_headers_bag[random_index]


async def get_netflix(
    session: aiohttp.ClientSession,
    request_url: str,
    responses: list[NetflixResponse] = None,
) -> list[NetflixResponse]:
    responses = responses or []
    async with session.get(request_url) as response:
        logging.info(f"Starting request for {request_url}")
        status = response.status
        html = await response.text()
        available = await response_indicates_available_title(response)

        if status not in (200, 301, 302, 404):
            raise ValueError(f"Unexpected HTTP status for {request_url}: {status}")

        responses.append(
            NetflixResponse(
                url=request_url,
                response=response,
                response_body=html,
                available=available,
            )
        )
        if available and "title" in request_url:
            # Sometimes we can access /title even if it's not available, so to be doubly sure,
            # try to access /watch
            await get_netflix(
                authenticated_session,
                request_url.replace("title", "watch"),
                responses,
            )

        return responses


async def run(netflix_id, session, limiter, dbcur):
    request_url = f"https://www.netflix.com/title/{netflix_id}"
    async with limiter:
        while True:
            try:
                responses = await get_netflix(session, request_url)
                break
            except aiohttp.client_exceptions.NonHttpUrlRedirectClientError as err:
                logging.exception(err)
            except aiohttp.client_exceptions.ServerDisconnectedError as err:
                logging.exception(err)
                raise

    checked_at = datetime.now(timezone.utc)

    async with asyncio.TaskGroup() as tg:
        for response in responses:
            html = response.response_body
            available = response.available

            if available:
                saveto_dir = (
                    TITLEPAGE_SAVETO_DIR
                    if "title" in response.url
                    else WATCHPAGE_SAVETO_DIR
                )
                filepath = saveto_dir / f"{netflix_id}.html"
                tg.create_task(save_response_body(html, filepath))

        data = {
            "netflix_id": netflix_id,
            "region_code": "MO",
            "country": "US",
            "available": available,
            "checked_at": checked_at,
        }

        tg.create_task(insert_into_database(dbcur, data))


async def main():
    background_tasks = set()
    responses = []

    with psycopg.Connection.connect(
        "dbname=postgres user=postgres", autocommit=True
    ) as dbconn:
        with dbconn.cursor() as dbcur:
            dbcur.execute("""
                SELECT 70050981
            """)
            concurrency_limit = 5
            connector = aiohttp.TCPConnector(
                limit=concurrency_limit, limit_per_host=concurrency_limit
            )
            # There's some weird timeout stuff that happens here
            # which necessitated the need for the ClientTimeout instance:
            # https://docs.aiohttp.org/en/stable/client_quickstart.html#aiohttp-client-timeouts
            # https://stackoverflow.com/questions/64534844/python-asyncio-aiohttp-timeout
            # https://github.com/aio-libs/aiohttp/issues/3203
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)

            # <10 requests/second seems to be about what Netflix tolerates before forcefully terminating the session
            # but I'll be nice and limit to 5 r/s
            limiter = AsyncLimiter(1, 1.0 / concurrency_limit)

            # TODO how can I avoid globals here? I'd like to use some kind of handler but as far as I figure
            # it would still require global variables
            global noauth_session
            global authenticated_session

            noauth_session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            authenticated_session = aiohttp.ClientSession(
                connector=connector, timeout=timeout, headers={**HEADERS, **COOKIE}
            )

            for netflix_id, *_ in dbcur:
                task = asyncio.create_task(
                    run(netflix_id, noauth_session, limiter, dbcur),
                    name=str(netflix_id),
                )
                background_tasks.add(task)
                task.add_done_callback(background_tasks.discard)

            responses.extend(await asyncio.gather(*background_tasks))

            for session in [noauth_session, authenticated_session]:
                await session.close()

    return responses


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
