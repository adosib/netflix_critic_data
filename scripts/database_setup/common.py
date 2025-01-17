import os
import re
import random
import asyncio
import logging
from typing import Any, NewType, Callable, Optional
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
from urllib.parse import urlencode

import aiohttp
import aiofiles
import minify_html
import pythonmonkey as pm
from bs4 import BeautifulSoup
from aiolimiter import AsyncLimiter
from apify_client import ApifyClientAsync

THIS_DIR = Path(__file__).parent
SCRIPTS_DIR = THIS_DIR / "utils"

# See https://docs.apify.com/api/client/python/docs
# Initialize the ApifyClient with your API token
APIFY_CLIENT = ApifyClientAsync(os.getenv("APIFY_TOKEN"))

LOGGER = logging.getLogger(__name__)

with (
    open(SCRIPTS_DIR / "playwright-pagefn.js", "r") as f1,
    open(SCRIPTS_DIR / "playwright-prenav.js", "r") as f2,
    open(SCRIPTS_DIR / "playwright-postnav.js", "r") as f3,
):
    PLAYWRIGHT_PAGEFN = f1.read()
    PLAYWRIGHT_PRENAV_HOOK = f2.read()
    PLAYWRIGHT_POSTNAV_HOOK = f3.read()

HTMLContent = NewType("HTML", str)


class ContextExtractionError(Exception):
    pass


class JobStore:
    """Data structure to avoid re-processing title IDs that have been processed already."""

    def __init__(self):
        self._data = defaultdict(list)
        self._global_values = set()  # Set to enforce global uniqueness of values

    def add(self, key: Any, values: list):
        """Add a value to a key. Ensure global uniqueness."""
        if key not in self._data:
            self._data[key]

        for value in values:
            if value in self._global_values:
                continue

            self._data[key].append(value)
            self._global_values.add(value)

    def get(self, key):
        """Get all values associated with a key."""
        return self._data.get(key, [])

    def __setitem__(self, key, value):
        return self.add(key, value)

    def __getitem__(self, key):
        return self.get(key)

    def __repr__(self):
        return repr(self._data)


class HttpSessionHandler:
    def __init__(self, **kwargs):
        concurrency_limit = kwargs.pop("concurrency_limit", 5)
        connector = aiohttp.TCPConnector(
            limit=concurrency_limit, limit_per_host=concurrency_limit
        )
        headers = kwargs.pop("headers", {})
        # There's some weird timeout stuff that happens here
        # which necessitated the need for the ClientTimeout instance:
        # https://docs.aiohttp.org/en/stable/client_quickstart.html#aiohttp-client-timeouts
        # https://stackoverflow.com/questions/64534844/python-asyncio-aiohttp-timeout
        # https://github.com/aio-libs/aiohttp/issues/3203
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)

        # <10 requests/second seems to be about what Netflix tolerates before forcefully terminating the session
        # but I'll be nice and limit to 5 r/s
        self.limiter = AsyncLimiter(1, 1.0 / concurrency_limit)

        self.active_sessions = []

        self.noauth_session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, **kwargs
        )
        self.active_sessions.append(self.noauth_session)

        if headers.get("Cookie", None):
            self.authenticated_session = aiohttp.ClientSession(
                connector=connector, timeout=timeout, headers=headers, **kwargs
            )
            self.active_sessions.append(self.authenticated_session)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        for session in self.active_sessions:
            await session.close()

    async def close(self):
        for session in self.active_sessions:
            await session.close()

    async def choose_session(self, urlpath) -> aiohttp.ClientSession:
        if "title" in urlpath:
            return self.noauth_session
        return self.authenticated_session


class RatingPattern:
    def __init__(
        self, name: str, pattern: str, normalization: Callable[[str], int] = None
    ):
        self.name = name
        self.regex = re.compile(pattern, re.VERBOSE)
        self.normalization = normalization
        if normalization is None:
            self.normalization = self.normalize_fractional

    def match(self, text: str) -> Optional[int]:
        match = self.regex.search(text)
        if match:
            return self.normalization(match.group(0))
        return None

    def normalize_fractional(self, rating: str) -> int:
        numerator, denominator = map(float, rating.split("/"))
        return round((numerator / denominator) * 100)


RATING_PATTERNS = [
    RatingPattern("percent", r"\d{1,3}(?=%)", lambda x: int(x)),
    RatingPattern(
        "out_of_5",
        r"""
            (?:(?<=\s)|(?<=^)) # positive look behind to make sure the character preceding is either whitespace or start of string
            (?:[0-4](?:\.\d+)?|5(\.0*)?) # match 0-4 optionally followed by a dot and a number 0-9 (e.g. 4.4) OR '5' / '5.0'
            \/5 # match '/5'
            (?=\s|$) # positive lookahead to ensure the succeeding character is either whitespace or end of string
        """,
    ),
    # practically identical to out_of_5
    RatingPattern(
        "out_of_10",
        r"(?:(?<=\s)|(?<=^))(?:\d(?:\.\d+)?|10(\.0*)?)\/10(?=\s|$)",
    ),
    RatingPattern(
        "audience_rating",
        r"^[0-4]\.\d+|5\.0$",
        lambda x: int(float(x) * 20),
    ),
]


def get_field(react_context: list[dict], field: str):
    fields = {}
    hero_data = _parse_hero_data(react_context)
    fields["title"] = hero_data.get("title")
    fields["runtime"] = hero_data.get(
        "runtime"
    )  # NOTE: shows don't have a runtime attribute (their episodes do)
    release_year = hero_data.get("year")
    fields["release_year"] = _get_release_year(react_context, release_year)
    fields["content_type"] = _get_content_type(react_context)
    return fields.get(field)


def _parse_hero_data(react_context: list[dict]) -> dict:
    try:
        return react_context[0]["data"]["details"][0]["data"]
    except (TypeError, IndexError):
        return {}


def _get_release_year(react_context: list[dict], release_year: int):
    for item in react_context:
        if item["type"] == "seasonsAndEpisodes":
            try:
                for season in item["data"]["seasons"]:
                    for episode in season["episodes"]:
                        if episode["year"] > 1900 and episode["year"] < release_year:
                            release_year = episode["year"]
            except (TypeError, KeyError):
                return release_year
    return release_year


def _get_content_type(react_context: list[dict]):
    for item in react_context:
        if item["type"] == "moreDetails":
            return item["data"]["type"].replace("show", "tv series")


def extract_netflix_react_context(html: HTMLContent) -> list[dict]:
    scripts = _find_all_script_elements(html)
    for script in scripts:
        content = script["content"]
        if content is None:
            continue
        context_def = content.find("reactContext =")
        if context_def != -1:
            try:
                react_context = script["content"][context_def:]
                return _sanitize_pythonmonkey_obj(
                    pm.eval(react_context).models.nmTitleUI.data.sectionData
                )
            except (KeyError, AttributeError, pm.SpiderMonkeyError) as e:
                raise ContextExtractionError("Error extracting reactContext: ", e)


def _find_all_script_elements(html: HTMLContent):
    """
    Finds and returns all <script> elements in the provided HTML string.

    :param html: A string containing the HTML content.
    :return: A list of dictionaries with script details (content and attributes).
    """
    soup = BeautifulSoup(html, "html.parser")
    script_elements = soup.find_all("script")

    scripts_info = []
    for script in script_elements:
        script_info = {
            "content": script.string,  # Inline script content (None if external)
            "attributes": script.attrs,  # Script element attributes (e.g., src, type)
        }
        scripts_info.append(script_info)

    return scripts_info


def _sanitize_pythonmonkey_obj(obj):
    if isinstance(obj, dict):
        return {k: _sanitize_pythonmonkey_obj(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_sanitize_pythonmonkey_obj(v) for v in obj]
    elif obj == pm.null:
        return None
    else:
        if isinstance(obj, float):
            if obj.is_integer():
                return int(obj)
        return obj


async def save_response_body(response_body: HTMLContent, saveto_path: Path):
    async with aiofiles.open(str(saveto_path), "w+") as f:
        minified = minify_html.minify(response_body, minify_css=True, minify_js=True)
        await f.write(minified)


async def get_serp_html(netflix_id, title, content_type, release_year):
    queries = _build_query(title, content_type, release_year, permute=True)
    start_url, *alt_search_paths = _build_google_urls(queries)
    run: dict = await APIFY_CLIENT.actor("MpRbnNmVAoj5RC1Ma").call(
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
    await APIFY_CLIENT.run(run["id"]).wait_for_finish()  # TODO is this necessary?

    dataset_items = await APIFY_CLIENT.dataset(run["defaultDatasetId"]).list_items()

    LOGGER.info(f"Found {len(dataset_items.items)} dataset items for run {run['id']}")

    # https://docs.apify.com/api/client/python/reference/class/DatasetClientAsync#list_items
    for i, dataset in enumerate(dataset_items.items):
        if dataset.get("googleUserRating"):
            LOGGER.info(f"Found the Google user rating in dataset item {i}")
            return dataset
    # If we don't have Google user reviews, we can default to returning the dataset item
    # with the most captured review elements
    return max(
        dataset_items.items, key=lambda x: len(x.get("allRatings", [])), default=None
    )


def _build_query(title, content_type, release_year, permute=False) -> str | list[str]:
    query = f'"{title}" {content_type} ({release_year})'
    if permute:
        # Some alternative searches to consider
        # in case the Google user reviews snippet isn't present for the initial query
        alt1 = f"{title} ({content_type}) reviews"
        alt2 = f"{title} ({content_type})"
        alt3 = f"{title} ({release_year})"
        return [query, alt1, alt2, alt3]
    return query


def _build_google_urls(queries) -> list[str]:
    base_url = "https://www.google.com/search"
    if isinstance(queries, str):
        queries = [queries]
    return [
        f"{base_url}?{urlencode({'q': query, 'hl': 'en', 'geo': 'us'})}"
        for query in queries
    ]


async def get_ratings(netflix_id, html) -> list[dict]:
    reviews = await extract_reviews_from_serp(html)
    checked_at = datetime.now(timezone.utc)
    for review in reviews:
        review["netflix_id"] = netflix_id
        review["checked_at"] = checked_at
    return reviews


async def extract_reviews_from_serp(html):
    soup = BeautifulSoup(html, "html.parser")
    # TODO this isn't super robust e.g. for the Netflix title with ID 80107103,
    # the first Google search term that yielded results was:
    # "ONE PIECE" tv series (1999.0)
    # but the IMDb rating wasn't captured though present on the page.
    # Granted, querying with the release year as a float was a bug that's been resolved,
    # but the code should be more robust.
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
        rating = _find_rating(inner_text_arr[0])
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
                "rating": _find_rating(stripped_strings[0]),
                "ratings_count": None,
            }
        )
    elif "Audience rating summary" in stripped_strings:
        rating = None
        ct_ratings = None
        try:
            for string in stripped_strings:
                if rating is None:
                    rating = _find_rating(string)
                if ct_ratings is None:
                    match = re.search(r"\d+(?=\s+ratings)", string)
                    if match:
                        ct_ratings = int(match.group(0))
        except (IndexError, ValueError, AttributeError) as e:
            LOGGER.error(f"Error processing audience summary: {e}")
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


def _find_rating(text: str) -> Optional[int]:
    for pattern in RATING_PATTERNS:
        rating = pattern.match(text)
        if rating is not None:
            return rating
    return None


def configure_logger(external_logger):
    global LOGGER
    LOGGER = external_logger
