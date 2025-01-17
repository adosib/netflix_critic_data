import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime

from common import get_field, configure_logger, extract_netflix_react_context
from psycopg import Cursor, Connection, sql

THIS_DIR = Path(__file__).parent
ROOT_DIR, *_ = [
    parent for parent in THIS_DIR.parents if parent.stem == "netflix_critic_data"
]
SAVETO_DIR = ROOT_DIR / "data" / "raw" / "serp"
LOG_DIR = ROOT_DIR / "logs"


async def update_db(
    dbcur: Cursor,
    netflix_id: int,
    parsed_data: list[dict],
):
    to_update = ("release_year", "runtime", "metadata")  # TODO content_type? title?
    release_year = get_field(parsed_data, "release_year")
    runtime = get_field(parsed_data, "runtime")

    update_titles_query = sql.SQL(
        "UPDATE titles SET ({fields}) = ({values}) WHERE netflix_id = {netflix_id}"
    ).format(
        fields=sql.SQL(", ").join(map(sql.Identifier, to_update)),
        values=sql.SQL(", ").join((release_year, runtime, json.dumps(parsed_data))),
        netflix_id=netflix_id,
    )
    logger.info(f"Now executing: {update_titles_query.as_string()}")
    dbcur.execute(update_titles_query)


async def run(dbcur, netflix_id):
    html_file_path = ROOT_DIR / "data" / "raw" / "title" / f"{netflix_id}.html"
    try:
        metadata = json.loads(extract_netflix_react_context(html_file_path))
        await update_db(dbcur, netflix_id, metadata)
    except json.decoder.JSONDecodeError:
        logger.error(f"JSONDecodeError for {html_file_path}")


async def main():
    with Connection.connect("dbname=postgres user=postgres", autocommit=True) as dbconn:
        with dbconn.cursor() as dbcur:
            # Gather reachable titles lacking metadata
            dbcur.execute("""
                SELECT DISTINCT titles.netflix_id, metadata
                FROM titles
                JOIN availability 
                    ON availability.netflix_id = titles.netflix_id
                    AND availability.country = 'US'
                WHERE availability.titlepage_reachable
                  AND titles.metadata IS NULL;
            """)
            async with asyncio.TaskGroup() as tg:
                for netflix_id, *_ in dbcur:
                    tg.create_task(
                        run(dbcur, netflix_id),
                        name=str(netflix_id),
                    )


if __name__ == "__main__":
    filename = Path(__file__).stem
    log_file = LOG_DIR / f"{filename}-{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
    logger = logging.getLogger(__name__)
    file_handler = logging.FileHandler(log_file, mode="a+")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    configure_logger(logger)

    asyncio.run(main())
