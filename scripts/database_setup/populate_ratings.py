import os
import re
import json
import random
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlencode

import psycopg
import aiofiles
from bs4 import BeautifulSoup
from psycopg import sql
from apify_client import ApifyClientAsync

THIS_DIR = Path(__file__).parent
ROOT_DIR, *_ = [
    parent for parent in THIS_DIR.parents if parent.stem == "netflix_critic"
]
SAVETO_DIR = ROOT_DIR / "data" / "raw" / "serp"
LOG_DIR = ROOT_DIR / "logs"
SCRIPTS_DIR = (THIS_DIR / "..").resolve()

JS_EVAL_SCRIPT = SCRIPTS_DIR / "evaluate.js"

# See https://docs.apify.com/api/client/python/docs
# Initialize the ApifyClient with your API token
CLIENT = ApifyClientAsync(os.getenv("APIFY_TOKEN"))

RGX_RATING_PATTERNS = {
    "percent": re.compile(r"\d{1,3}(?=%)"),
    "out_of_5": re.compile(
        r"""
        (?:(?<=\s)|(?<=^)) # positive look behinds to make sure the character preceding is either whitespace or start of string
        (?:[0-4](?:\.\d+)?|5(\.0*)?) # match 0-4 optionally followed by a dot and a number 0-9 (e.g. 4.4) OR '5' / '5.0'
        \/5 # match '/5'
        (?=\s|$) # positive lookahead to ensure the succeeding character is either whitespace or end of string
        """,
        re.VERBOSE,
    ),
    "out_of_10": re.compile(
        r"(?:(?<=\s)|(?<=^))(?:\d(?:\.\d+)?|10(\.0*)?)\/10(?=\s|$)"
    ),  # practically identical to out_of_5
}

log_file = LOG_DIR / f'{datetime.now().strftime('%Y%m%d%H%M%S')}.log'
logging.basicConfig(
    filename=log_file,
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

with (
    open(SCRIPTS_DIR / "playwright-pagefn.js", "r") as f1,
    open(SCRIPTS_DIR / "playwright-prenav.js", "r") as f2,
    open(SCRIPTS_DIR / "playwright-postnav.js", "r") as f3,
):
    PLAYWRIGHT_PAGEFN = f1.read()
    PLAYWRIGHT_PRENAV_HOOK = f2.read()
    PLAYWRIGHT_POSTNAV_HOOK = f3.read()


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def find_rating(text):
    for _, rgx in RGX_RATING_PATTERNS.items():
        match = re.search(rgx, text)
        if match:
            return _normalize_rating(match.group(0))


def _normalize_rating(rating):
    rating_split = re.findall(r"[\d\.]+", rating)
    len_ = len(rating_split)
    if len_ > 2 or len_ == 0:
        return None
    try:
        numerator, denominator = map(float, rating_split)
        return int(round(numerator / denominator, 2) * 100)
    except ValueError:
        return rating_split[0]


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
                year_first_ep = item["data"]["seasons"][0]["episodes"][0]["year"]
                return year_first_ep if year_first_ep < release_year else release_year
            except (TypeError, KeyError):
                return release_year
    return release_year


def _get_content_type(parsed_data: list[dict]):
    for item in parsed_data:
        if item["type"] == "moreDetails":
            return item["data"]["type"]


def build_query(parsed_data, permute=False) -> str | list[str]:
    title = get_field(parsed_data, "title")
    release_year = get_field(parsed_data, "release_year")
    content_type = get_field(parsed_data, "content_type")
    query = f'"{title}" ({release_year}) reviews'
    if permute:
        # Some alternative searches to consider in case the Google user reviews snippet isn't present for the initial query
        alt1 = f"{title} ({release_year}) reviews"
        alt2 = f"{title} ({release_year})"
        alt3 = f"{title} ({content_type})"
        return [query, alt1, alt2, alt3]
    return query


def build_google_urls(queries) -> list[str]:
    base_url = "https://www.google.com/search"
    if isinstance(queries, str):
        queries = [queries]
    return [
        f"{base_url}?{
            urlencode(
                {"q": query, 
                "hl": "en", 
                "geo": "us"}
            )}"
        for query in queries
    ]


async def save_response_body(response_body: str, filepath: Path):
    async with aiofiles.open(str(filepath), "w+") as f:
        await f.write(response_body)


async def extract_reviews_from_serp(html):
    soup = BeautifulSoup(html, "html.parser")
    reviews = soup.select("[data-attrid$=reviews], [data-attrid$=thumbs_up]")
    reviews_list = []
    for review in reviews:
        reviews_list.extend(await _extract_linked_reviews(review))
        reviews_list.extend(await _extract_non_link_reviews(review))

    return reviews_list


async def _extract_linked_reviews(review):
    """Extracts reviews from links with vendor info."""
    reviews_list = []
    a_tags = review.find_all("a", href=True)

    for a_tag in a_tags:
        # NOTE: assumption here is the `stripped_strings` property on linked reviews is always ordered.
        # Example output for the call `list(a_tag.stripped_strings)`:
        # ['4/5', 'Common Sense Media', 'Easy A got a 4 stars review on Common Sense Media.']
        inner_text_arr = [
            text for text in a_tag.stripped_strings if len(text) > 1
        ]  # len > 1 filter is to remove separator characters e.g. '·' in ['100%', '·', 'Rotten Tomatoes', 'Inuyasha scored 100 percent on Rotten Tomatoes.']
        vendor = inner_text_arr[1]
        rating = find_rating(inner_text_arr[0])
        if rating:
            reviews_list.append(
                {
                    "url": a_tag["href"],
                    "vendor": vendor,
                    "rating": rating,
                    "ratings_count": None,
                }
            )

    return reviews_list


async def _extract_non_link_reviews(review):
    """Extracts Google and Audience reviews where there are no links."""
    stripped_strings = list(review.stripped_strings)
    reviews_list = []

    if "Google users" in stripped_strings:
        reviews_list.append(
            {
                "url": None,
                "vendor": "Google users",
                "rating": find_rating(stripped_strings[0]),
                "ratings_count": None,
            }
        )
    elif "Audience rating summary" in stripped_strings:
        rating = None
        ct_ratings = None
        try:
            rating = float(stripped_strings[6]) * 20  # Rating is out of 100
            ct_ratings = int(
                re.search(r"\d+(?=\s+ratings)", stripped_strings[7]).group(0)
            )
        except (IndexError, ValueError, AttributeError) as e:
            logging.error(f"Error processing audience summary: {e}")
        finally:
            reviews_list.append(
                {
                    "url": None,
                    "vendor": "Audience rating summary",
                    "rating": rating,
                    "ratings_count": ct_ratings,
                }
            )

    return reviews_list


async def update_db(
    dbcur: psycopg.Cursor,
    netflix_id: int,
    parsed_data: list[dict],
    ratings_data: list[dict],
):
    to_update = ("release_year", "runtime", "metadata")
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

    upsert_ratings_query = sql.SQL(
        "INSERT INTO ratings (netflix_id, vendor, url, rating, ratings_count, checked_at) "
        "VALUES (%(netflix_id)s, %(vendor)s, %(url)s, %(rating)s, %(ratings_count)s, %(checked_at)s) "
        "ON CONFLICT (netflix_id, vendor) DO UPDATE "
        "SET url = EXCLUDED.url, rating = EXCLUDED.rating, ratings_count = EXCLUDED.ratings_count, checked_at = EXCLUDED.checked_at"
    )
    logging.info(
        f"Now executing: {upsert_ratings_query.as_string()} with data {json.dumps(ratings_data, indent=4, cls=DateTimeEncoder)}"
    )
    dbcur.executemany(upsert_ratings_query, ratings_data)


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


async def get_serp_html(netflix_id, parsed_data):
    queries = build_query(parsed_data, permute=True)
    start_url, *alt_search_paths = build_google_urls(queries)
    run: dict = await CLIENT.actor("MpRbnNmVAoj5RC1Ma").call(
        # https://docs.apify.com/api/client/python/reference/class/ActorClientAsync#call
        run_input={  # https://apify.com/apify/playwright-scraper/input-schema
            "browserLog": False,
            "closeCookieModals": False,
            "customData": {
                "alt_search_paths": alt_search_paths,
                "netflix_id": netflix_id,
            },
            "debugLog": True,
            "downloadCss": True,
            "downloadMedia": True,
            "headless": True,
            "ignoreCorsAndCsp": False,
            "ignoreSslErrors": False,
            "keepUrlFragments": False,
            "launcher": "chromium",
            "maxCrawlingDepth": 1,
            "pageFunction": PLAYWRIGHT_PAGEFN,
            "postNavigationHooks": PLAYWRIGHT_POSTNAV_HOOK,
            "preNavigationHooks": PLAYWRIGHT_PRENAV_HOOK,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": [],
                "apifyProxyCountry": "US",
            },
            "startUrls": [{"url": start_url, "method": "GET"}],
            "useChrome": False,
            "waitUntil": "load",
        },
        memory_mbytes=1024,
        timeout_secs=120,
        wait_secs=120,
    )
    retry_delay = 0.1
    for _ in range(5):
        try:
            dataset = await _get_dataset(run)
            html = dataset["html"]
            return html
        except (IndexError, KeyError, TypeError):
            await asyncio.sleep(retry_delay)
            retry_delay *= 2  # Double the delay for the next attempt
            retry_delay += random.uniform(0, 1)  # Add jitter
    return ""  # TODO


async def _get_dataset(run):
    await CLIENT.run(run["id"]).wait_for_finish()  # TODO is this necessary?

    dataset_items = await CLIENT.dataset(run["defaultDatasetId"]).list_items()

    logging.info(f"Found {len(dataset_items.items)} dataset items for run {run["id"]}")

    # https://docs.apify.com/api/client/python/reference/class/DatasetClientAsync#list_items
    for i, dataset in enumerate(dataset_items.items):
        if dataset.get("googleUserRating"):
            logging.info(f"Found the Google user rating in dataset item {i}")
            return dataset
    # If we don't have Google user reviews, we can default to returning the dataset item
    # with the most captured review elements
    return max(
        dataset_items.items, key=lambda x: len(x.get("allRatings", [])), default=None
    )


async def get_ratings(netflix_id, html) -> list[dict]:
    reviews = await extract_reviews_from_serp(html)
    checked_at = datetime.now(timezone.utc)
    for review in reviews:
        review["netflix_id"] = netflix_id
        review["checked_at"] = checked_at
    return reviews


async def run(semaphore, dbcur, netflix_id: str):
    # Need to use a Semaphore to limit concurrent Actor runs, otherwise risk the APIfy error:
    # apify_client._errors.ApifyApiError: By launching this job you will exceed the memory limit of 8192MB for all your Actor runs and builds [...]
    async with semaphore:
        html_file_path = ROOT_DIR / "data" / "raw" / "title" / f"{netflix_id}.html"
        data = await extract_netflix_context(html_file_path)
        try:
            parsed_data: list[dict] = json.loads(data)
            html = await get_serp_html(netflix_id, parsed_data)
            await save_response_body(html, SAVETO_DIR / f"{netflix_id}.html")
            ratings_data = await get_ratings(netflix_id, html)
            await update_db(dbcur, netflix_id, parsed_data, ratings_data)
        except json.JSONDecodeError as e:
            logging.exception(e)


async def main():
    semaphore = asyncio.Semaphore(32)
    with psycopg.Connection.connect(
        "dbname=postgres user=postgres", autocommit=True
    ) as dbconn:
        with dbconn.cursor() as dbcur:
            dbcur.execute("""
                SELECT DISTINCT titles.netflix_id
                FROM titles
                JOIN availability 
                    ON availability.netflix_id = titles.netflix_id
                LEFT JOIN ratings
                    on ratings.netflix_id = titles.netflix_id
                WHERE availability.available = true
                  AND ratings.id IS NULL
                LIMIT 32;
            """)
            async with asyncio.TaskGroup() as tg:
                for netflix_id, *_ in dbcur:
                    tg.create_task(
                        run(semaphore, dbcur, netflix_id), name=str(netflix_id)
                    )


if __name__ == "__main__":
    asyncio.run(main())
