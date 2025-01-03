import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime

import psycopg
from psycopg import sql

THIS_DIR = Path(__file__).parent
ROOT_DIR, *_ = [
    parent for parent in THIS_DIR.parents if parent.stem == "netflix_critic_data"
]
SAVETO_DIR = ROOT_DIR / "data" / "raw" / "serp"
LOG_DIR = ROOT_DIR / "logs"

JS_EVAL_SCRIPT = THIS_DIR / "utils" / "evaluate.js"

filename = Path(__file__).stem
log_file = LOG_DIR / f'{filename}-{datetime.now().strftime('%Y%m%d%H%M%S')}.log'
logging.basicConfig(
    filename=log_file,
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def get_field(parsed_data, field):
    fields = {}
    hero_data = parsed_data[0]["data"]["details"][0]["data"]
    fields["title"] = hero_data.get("title")
    # NOTE: shows don't have a runtime attribute (their episodes do)
    fields["runtime"] = hero_data.get("runtime")
    release_year = hero_data.get("year")
    fields["release_year"] = _get_release_year(release_year, parsed_data)
    fields["content_type"] = _get_content_type(parsed_data)
    return fields.get(field)


def _get_release_year(release_year: int, all_data: list[dict]):
    for item in all_data:
        if item["type"] == "seasonsAndEpisodes":
            try:
                # TODO order isn't acutally guaranteed so ["seasons"][0] is not always S1
                year_first_ep = item["data"]["seasons"][0]["episodes"][0]["year"]
                return year_first_ep if year_first_ep < release_year else release_year
            except (TypeError, KeyError):
                return release_year
    return release_year


def _get_content_type(parsed_data: list[dict]):
    for item in parsed_data:
        if item["type"] == "moreDetails":
            return item["data"]["type"]


async def extract_netflix_context(html_path):
    logging.info(f"Attempting to extract context from {html_path}")
    process = await asyncio.create_subprocess_exec(
        "node",
        JS_EVAL_SCRIPT,
        html_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await process.communicate()

    if process.returncode == 0:
        return stdout.decode()
    else:
        logging.error(stderr)
        return None


async def update_db(
    dbcur: psycopg.Cursor,
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
    logging.info(f"Now executing: {update_titles_query.as_string()}")
    dbcur.execute(update_titles_query)


async def run(dbcur, netflix_id):
    async with semaphore:
        html_file_path = ROOT_DIR / "data" / "raw" / "title" / f"{netflix_id}.html"
        try:
            metadata = json.loads(await extract_netflix_context(html_file_path))
            await update_db(dbcur, netflix_id, metadata)
        except json.decoder.JSONDecodeError:
            logging.error(f"JSONDecodeError for {html_file_path}")


async def main():
    global semaphore
    semaphore = asyncio.Semaphore(8)
    with psycopg.Connection.connect(
        "dbname=postgres user=postgres", autocommit=True
    ) as dbconn:
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
    asyncio.run(main())
