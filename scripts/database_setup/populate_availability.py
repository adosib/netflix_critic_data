import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass

import aiohttp
from bs4 import BeautifulSoup
from common import NetflixSessionHandler, configure_logger, save_response_body
from psycopg import Cursor, Connection, sql
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

THIS_FILE = Path(__file__)
THIS_DIR = THIS_FILE.parent
ROOT_DIR, *_ = [
    parent for parent in THIS_DIR.parents if parent.stem == "netflix_critic_data"
]
TITLEPAGE_SAVETO_DIR = ROOT_DIR / "data" / "raw" / "title"
WATCHPAGE_SAVETO_DIR = ROOT_DIR / "data" / "raw" / "watch"
LOG_DIR = ROOT_DIR / "logs"

COUNTRY_CODE = "US"


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


async def update_database(cursor: Cursor, record: dict):
    upsert_availability_query = sql.SQL(
        "INSERT INTO availability (netflix_id, redirected_netflix_id, country, available, titlepage_reachable, checked_at) "
        "VALUES (%(netflix_id)s, %(redirected_netflix_id)s, %(country)s, %(available)s, %(titlepage_reachable)s, %(checked_at)s) "
        "ON CONFLICT (netflix_id, country) DO UPDATE "
        "SET redirected_netflix_id = EXCLUDED.redirected_netflix_id, available = EXCLUDED.available, titlepage_reachable = EXCLUDED.titlepage_reachable, checked_at = EXCLUDED.checked_at"
    )
    logger.info(f"Now executing public.availability UPSERT with values: {record}")
    cursor.execute(upsert_availability_query, record)


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


def _retry_log(retry_state):
    logger.warning(
        "Retrying %s(%s): attempt %s",
        retry_state.fn,
        retry_state.args,
        retry_state.attempt_number,
    )


@retry(
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(aiohttp.ClientResponseError),
    wait=wait_exponential(multiplier=60, min=60, max=300),
    before=_retry_log,
)
async def get_netflix(
    netflix_id: int, request_path: str, session: aiohttp.ClientSession
) -> NetflixResponse:
    async with session.get(request_path) as response:
        request_url = session._base_url / request_path
        logger.info(f"Starting request for {request_url}")
        status = response.status

        if status not in (200, 301, 302, 404):
            response.raise_for_status()

        html = await response.text()
        available = await response_indicates_available_title(response)

        return NetflixResponse(
            netflix_id=netflix_id,
            orig_url=request_url,
            response=response,
            response_body=html,
            available=available,
        )


async def run(netflix_id: int, session_handler: NetflixSessionHandler, dbcur: Cursor):
    title_id = netflix_id
    responses: list[NetflixResponse] = []
    async with session_handler.limiter:
        for urlpath in ["title", "watch"]:  # <- order here really matters
            # Sometimes we can access /title even if it's not available, so to be doubly sure,
            # try to access /watch, too
            request_path = f"{urlpath}/{title_id}"
            try:
                session = session_handler.choose_session(urlpath)
                response = await get_netflix(title_id, request_path, session)
                responses.append(response)
                if not response.available:
                    # If /title isn't available, neither will be /watch, so don't bother
                    break
                elif response.redirected_netflix_id:
                    # If /title redirects us, we'll want to use that redirected ID for /watch
                    title_id = response.redirected_netflix_id

            except aiohttp.client_exceptions.NonHttpUrlRedirectClientError as err:
                logger.exception(err)
                raise

            except aiohttp.client_exceptions.ServerDisconnectedError as err:
                logger.exception(err)
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

    with Connection.connect(conn_string, autocommit=True) as dbconn:
        with dbconn.cursor() as dbcur:
            dbcur.execute(
                """
                SELECT titles.netflix_id
                FROM titles
                LEFT JOIN availability
                    ON availability.netflix_id = titles.netflix_id
                    AND country = %(country)s
                WHERE availability.netflix_id IS NULL 
                   OR availability.checked_at + INTERVAL '7 days' < current_date;
                """,
                {"country": COUNTRY_CODE},
            )

            async with NetflixSessionHandler(
                headers={**HEADERS, **COOKIE}
            ) as session_handler:
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

    log_file = (
        LOG_DIR / f"{THIS_FILE.stem}-{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
    )
    logger = logging.getLogger(__name__)

    file_handler = logging.FileHandler(log_file, mode="a+")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    logger.setLevel(logging.DEBUG)
    configure_logger(logger)

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "postgres")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")

    conn_string = (
        f"dbname={dbname} user={user} password={password} host={host} port={port}"
    )

    asyncio.run(main(), debug=True)
