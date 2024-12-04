import os
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone

import aiohttp
import psycopg
from psycopg import sql

from scripts.rate_limiter_cls import ThrottledClientSession

from .populate_availability import (
    get_netflix,
    save_response_body,
)

THIS_DIR = Path(__file__).parent
ROOT_DIR, *_ = [
    parent for parent in THIS_DIR.parents if parent.stem == "netflix_critic"
]
SAVETO_DIR = ROOT_DIR / "data" / "raw" / "watch"
COOKIES = os.getenv("NETFLIX_HEADER")
HEADERS = {"Cookie": COOKIES}


async def update_availability(
    connection: psycopg.Connection,
    cursor: psycopg.Cursor,
    record: dict,
    to_update=("available", "checked_at"),
):
    update_query = sql.SQL(
        "UPDATE availability SET ({fields}) = ({values}) WHERE netflix_id = {netflix_id} "
    ).format(
        fields=sql.SQL(", ").join(map(sql.Identifier, to_update)),
        values=sql.SQL(", ").join((record[x] for x in to_update)),
        netflix_id=record["netflix_id"],
    )
    logging.info(f"Now executing: {update_query.as_string(connection)}")
    cursor.execute(update_query)


# TODO this is an idea for getting DRY... not sure where I'm going with it tho
async def run_subtasks(*async_partials):
    async with asyncio.TaskGroup() as tg:
        for task in async_partials:
            tg.create_task(task())


# TODO figure out how to make run and main reusable so that I'm not doing copy-pasting
async def run(netflix_id, session, dbconn, dbcur, subtasks=None):
    filepath = SAVETO_DIR / f"{netflix_id}.html"
    if filepath.exists():
        return
    request_url = f"https://www.netflix.com/watch/{netflix_id}"
    while True:
        try:
            response = await get_netflix(session, HEADERS, request_url)
            break
        except aiohttp.client_exceptions.NonHttpUrlRedirectClientError as err:
            logging.exception(err)
        except aiohttp.client_exceptions.ServerDisconnectedError as err:
            logging.exception(err)
            raise

    html = response.response_body
    available = response.available
    checked_at = datetime.now(timezone.utc)
    data = {
        "netflix_id": netflix_id,
        "region_code": "MO",
        "country": "US",
        "available": available,
        "checked_at": checked_at,
    }

    async with asyncio.TaskGroup() as tg:
        if available:
            # Only want to save files if we can successfully GET /watch/
            filepath = SAVETO_DIR / f"{netflix_id}.html"
            tg.create_task(save_response_body(html, filepath))
        else:
            # Conversely, only want to update availability if we cannot
            tg.create_task(update_availability(dbconn, dbcur, data))


# main pretty much just requires a SQL string... that's the difference
async def main():
    background_tasks = set()
    responses = []

    with psycopg.Connection.connect(
        "dbname=postgres user=postgres", autocommit=True
    ) as dbconn:
        with dbconn.cursor() as dbcur:
            dbcur.execute("""
                SELECT DISTINCT netflix_id
                FROM availability 
                WHERE available = true;
            """)
            concurrency_limit = 5
            connector = aiohttp.TCPConnector(
                limit=concurrency_limit, limit_per_host=concurrency_limit
            )
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)
            async with ThrottledClientSession(
                rate_limit=concurrency_limit, connector=connector, timeout=timeout
            ) as session:
                for netflix_id, *_ in dbcur:
                    task = asyncio.create_task(
                        run(netflix_id, session, dbconn, dbcur), name=str(netflix_id)
                    )
                    background_tasks.add(task)
                    task.add_done_callback(background_tasks.discard)

                responses.extend(await asyncio.gather(*background_tasks))

    return responses


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
