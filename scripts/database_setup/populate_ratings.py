import os
import sys
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime

from common import (
    Review,
    BrightDataSessionHandler,
    get_serp_html,
    configure_logger,
    save_response_body,
)
from psycopg import Cursor, Connection, sql
from psycopg.rows import namedtuple_row

THIS_FILE = Path(__file__)
THIS_DIR = THIS_FILE.parent
ROOT_DIR, *_ = [
    parent for parent in THIS_DIR.parents if parent.stem == "netflix_critic_data"
]
SAVETO_DIR = ROOT_DIR / "data" / "raw" / "serp"
LOG_DIR = ROOT_DIR / "logs"


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


async def update_db(
    dbcur: Cursor,
    ratings_data: list[Review],
):
    upsert_ratings_query = sql.SQL(
        "INSERT INTO ratings (netflix_id, vendor, url, rating, ratings_count, checked_at) "
        "VALUES (%(netflix_id)s, %(vendor)s, %(url)s, %(rating)s, %(ratings_count)s, %(checked_at)s) "
        "ON CONFLICT (netflix_id, vendor) DO UPDATE "
        "SET url = EXCLUDED.url, rating = EXCLUDED.rating, ratings_count = EXCLUDED.ratings_count, checked_at = EXCLUDED.checked_at"
    )
    ratings_data = [rating.__dict__ for rating in ratings_data]
    logger.info(
        f"Now executing: {upsert_ratings_query.as_string()} with data "
        f"{json.dumps(ratings_data, indent=4, cls=DateTimeEncoder)}"
    )
    dbcur.executemany(upsert_ratings_query, ratings_data)


async def run(brd_handler: BrightDataSessionHandler, dbcur, row):
    async with brd_handler.limiter:
        netflix_id = row.netflix_id
        serp_response = await get_serp_html(
            netflix_id,
            row.title,
            row.content_type,
            row.release_year,
            brd_handler.choose_session(),
        )
        await save_response_body(serp_response.html, SAVETO_DIR / f"{netflix_id}.html")
        await update_db(dbcur, serp_response.ratings)


async def main():
    with Connection.connect(conn_string, autocommit=True) as dbconn:
        with dbconn.cursor() as dbcur:
            dbcur.row_factory = namedtuple_row
            dbcur.execute("""
                select searchable.*
                from (
                    select distinct
                        coalesce(a.redirected_netflix_id, t.netflix_id) as netflix_id,
                        coalesce(t2.metadata, t.metadata) -> 0 -> 'data' ->> 'title' as title,
                        replace(
                            json_extract_element_from_metadata(
                                coalesce(t2.metadata, t.metadata),
                                'moreDetails'
                            )
                            -> 'data'
                            ->> 'type',
                            'show',
                            'tv series'
                        )::public.content_type as content_type,
                        coalesce(t2.release_year, t.release_year) as release_year
                    from availability as a
                    inner join titles as t
                        on a.netflix_id = t.netflix_id
                    left join titles as t2
                        on a.redirected_netflix_id = t2.netflix_id
                    where
                        a.country = 'US'
                        and a.available = true
                        and coalesce(
                            coalesce(t2.metadata, t.metadata)
                            -> 0
                            -> 'data'
                            -> 'details'
                            -> 0
                            -> 'data'
                            -> 'coreGenre'
                            ->> 'genreName', ''
                        ) <> 'Special Interest'
                ) searchable
                left join ratings 
                    on ratings.netflix_id = searchable.netflix_id
                    and ratings.vendor = 'Google users'
                where ratings.id is null;
            """)
            async with BrightDataSessionHandler() as brd_handler:
                async with asyncio.TaskGroup() as tg:
                    for row in dbcur:
                        netflix_id = row.netflix_id
                        tg.create_task(
                            run(brd_handler, dbcur, row),
                            name=str(netflix_id),
                        )


if __name__ == "__main__":
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
