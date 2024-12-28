import os
import asyncio
import logging
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

COUNTRY_CODE = "US"

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
    netflix_id: int
    orig_url: str
    response: aiohttp.ClientResponse
    response_body: str
    available: bool

    @property
    def redirected_netflix_id(self) -> int:
        url = self.response.url
        if str(self.netflix_id) in str(url):
            return None
        return int(url.query.get("origId", url.name))

    @property
    def saveto_path(self) -> str:
        saveto_dir = (
            WATCHPAGE_SAVETO_DIR if "watch" in self.orig_url else TITLEPAGE_SAVETO_DIR
        )
        return str(saveto_dir / f"{self.netflix_id}.html")


class SessionHandler:
    def __init__(self):
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
        self.limiter = AsyncLimiter(1, 1.0 / concurrency_limit)

        self.noauth_session = aiohttp.ClientSession(
            connector=connector, timeout=timeout
        )
        self.authenticated_session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, headers={**HEADERS, **COOKIE}
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.noauth_session.close()
        await self.authenticated_session.close()

    async def choose_session(self, urlpath) -> aiohttp.ClientSession:
        if "watch" in urlpath:
            return self.authenticated_session
        return self.noauth_session


async def update_database(cursor: psycopg.Cursor, record: dict):
    upsert_availability_query = sql.SQL(
        "INSERT INTO availability (netflix_id, redirected_netflix_id, country, available, titlepage_reachable, checked_at) "
        "VALUES (%(netflix_id)s, %(redirected_netflix_id)s, %(country)s, %(available)s, %(titlepage_reachable)s, %(checked_at)s) "
        "ON CONFLICT (netflix_id, country) DO UPDATE "
        "SET redirected_netflix_id = EXCLUDED.redirected_netflix_id, available = EXCLUDED.available, titlepage_reachable = EXCLUDED.titlepage_reachable, checked_at = EXCLUDED.checked_at"
    )
    logging.info(f"Now executing: {upsert_availability_query.as_string()}")
    cursor.execute(upsert_availability_query, record)


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


async def get_netflix(
    netflix_id: int, request_url: str, session: aiohttp.ClientSession
) -> NetflixResponse:
    async with session.get(request_url) as response:
        logging.info(f"Starting request for {request_url}")
        status = response.status
        html = await response.text()
        available = await response_indicates_available_title(response)

        if status not in (200, 301, 302, 404):
            raise ValueError(f"Unexpected HTTP status for {request_url}: {status}")

        return NetflixResponse(
            netflix_id=netflix_id,
            orig_url=request_url,
            response=response,
            response_body=html,
            available=available,
        )


async def run(netflix_id: int, session_handler: SessionHandler, dbcur: psycopg.Cursor):
    responses: list[NetflixResponse] = []
    async with session_handler.limiter:
        for urlpath in ["title", "watch"]:  # <- order here really matters
            # Sometimes we can access /title even if it's not available, so to be doubly sure,
            # try to access /watch, too
            request_url = f"https://www.netflix.com/{urlpath}/{netflix_id}"
            try:
                session = await session_handler.choose_session(urlpath)
                response = await get_netflix(netflix_id, request_url, session)
                responses.append(response)
                if not response.available:
                    # If /title isn't available, neither will be /watch, so don't bother
                    break
            except aiohttp.client_exceptions.NonHttpUrlRedirectClientError as err:
                logging.exception(err)
                raise
            except aiohttp.client_exceptions.ServerDisconnectedError as err:
                logging.exception(err)
                raise

    checked_at = datetime.now(timezone.utc)

    async with asyncio.TaskGroup() as tg:
        titlepage_reachable = False
        redirected_netflix_id = None

        for response in responses:
            if response.available:
                titlepage_reachable = True
                tg.create_task(
                    save_response_body(response.response_body, response.saveto_path)
                )

            if response.redirected_netflix_id:
                redirected_netflix_id = response.redirected_netflix_id

        data = {
            "netflix_id": netflix_id,
            "redirected_netflix_id": redirected_netflix_id,
            "country": COUNTRY_CODE,
            "available": response.available,  # will always be whether or not /watch is available
            "titlepage_reachable": titlepage_reachable,
            "checked_at": checked_at,
        }

        tg.create_task(update_database(dbcur, data))


async def main():
    background_tasks = set()
    responses = []

    with psycopg.Connection.connect(
        "dbname=postgres user=postgres", autocommit=True
    ) as dbconn:
        with dbconn.cursor() as dbcur:
            dbcur.execute(
                """
                SELECT DISTINCT titles.netflix_id
                FROM titles
                LEFT JOIN availability
                    ON availability.netflix_id = titles.netflix_id
                    AND country = %(country)s
                WHERE availability.netflix_id IS NULL 
                   OR availability.checked_at + INTERVAL '7 days' < current_date;
            """,
                {"country": COUNTRY_CODE},
            )

            async with SessionHandler() as session_handler:
                for netflix_id, *_ in dbcur:
                    task = asyncio.create_task(
                        run(netflix_id, session_handler, dbcur),
                        name=str(netflix_id),
                    )
                    background_tasks.add(task)
                    task.add_done_callback(background_tasks.discard)

                responses.extend(await asyncio.gather(*background_tasks))

    return responses


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
