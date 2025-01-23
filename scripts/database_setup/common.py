import os
import re
import json
import asyncio
import logging
import warnings
from typing import Any, NewType, Callable, Optional
from pathlib import Path
from datetime import datetime, timezone
from itertools import cycle
from collections import defaultdict
from dataclasses import dataclass
from urllib.parse import urlencode

import aiohttp
import aiofiles
import minify_html
import pythonmonkey as pm
from bs4 import Tag, BeautifulSoup
from aiolimiter import AsyncLimiter

BRD_API_URL = "https://api.brightdata.com/request"
BRD_ZONE = os.getenv("BRD_ZONE", "serp_api1")
BRD_AUTH_TOKEN = os.getenv("BRD_AUTH_TOKEN")
if BRD_AUTH_TOKEN is None:
    warnings.warn(
        "The environment variable 'BRD_AUTH_TOKEN' is not set. "
        "Titles that are not found in the database will not be looked up "
        "(see environment variables section in docs).",
        UserWarning,
    )

LOGGER = logging.getLogger(__name__)

HTMLContent = NewType("HTML", str)


class ContextExtractionError(Exception):
    pass


class SessionLimitError(Exception):
    pass


@dataclass
class Review:
    netflix_id: int
    url: str
    vendor: str
    rating: int
    ratings_count: int
    checked_at: datetime = datetime.now(timezone.utc)


@dataclass
class SERPResponse:
    html: HTMLContent
    ratings: list[Review]


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
    def __init__(self, base_url=None, session_limit=5, max_rps=5):
        self.base_url = base_url
        self.session_limit = session_limit
        self.active_sessions = []
        self.limiter = AsyncLimiter(1, 1.0 / max_rps)

    def start_session(self, cookie_auth=False, **kwargs) -> aiohttp.ClientSession:
        if len(self.active_sessions) >= self.session_limit:
            raise SessionLimitError(
                "The maximum allowable number of active sessions has been reached."
            )

        concurrency_limit = kwargs.pop("concurrency_limit", 5)
        connector = kwargs.pop(
            "connector",
            aiohttp.TCPConnector(
                limit=concurrency_limit, limit_per_host=concurrency_limit
            ),
        )
        # There's some weird timeout stuff that happens here
        # which necessitated the need for the ClientTimeout instance:
        # https://docs.aiohttp.org/en/stable/client_quickstart.html#aiohttp-client-timeouts
        # https://stackoverflow.com/questions/64534844/python-asyncio-aiohttp-timeout
        # https://github.com/aio-libs/aiohttp/issues/3203
        timeout = kwargs.pop(
            "timeout", aiohttp.ClientTimeout(total=None, sock_connect=15)
        )

        headers = kwargs.pop("headers", {})
        if not cookie_auth:
            # If we're not authenticating with cookies, remove any present cookies
            headers.pop("Cookie", None)

        session = aiohttp.ClientSession(
            self.base_url,
            connector=connector,
            headers=headers,
            timeout=timeout,
            **kwargs,
        )

        self.active_sessions.append(session)
        return session

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        while self.active_sessions:
            session = self.active_sessions.pop()
            await session.close()


class NetflixSessionHandler(HttpSessionHandler):
    def __init__(
        self, base_url="https://www.netflix.com/", session_limit=2, max_rps=5, **kwargs
    ):
        # <10 requests/second seems to be about what Netflix tolerates before forcefully terminating the session
        # but I'll be nice and limit to 5 r/s
        super().__init__(
            base_url=base_url, session_limit=session_limit, max_rps=max_rps
        )
        self.noauth_session = self.start_session(**kwargs)
        self.authenticated_session = self.start_session(cookie_auth=True, **kwargs)

    def choose_session(self, urlpath) -> aiohttp.ClientSession:
        if "title" in urlpath:
            return self.noauth_session
        return self.authenticated_session


class BrightDataSessionHandler(HttpSessionHandler):
    def __init__(self, session_limit=10, max_rps=10, **kwargs):
        super().__init__(session_limit=session_limit, max_rps=max_rps)
        self._session_iterator = None
        for _ in range(session_limit):
            self.start_session(**kwargs)

    def _update_session_iterator(self):
        """Reinitialize the cycle iterator based on the updated active_sessions."""
        self._session_iterator = cycle(self.active_sessions)

    def start_session(self, cookie_auth=False, **kwargs):
        session = super().start_session(cookie_auth, **kwargs)
        self._update_session_iterator()
        return session

    def choose_session(self):
        if not self.active_sessions:
            raise ValueError("No active sessions available.")
        if self._session_iterator is None:
            self._update_session_iterator()
        return next(self._session_iterator)

    async def close(self):
        await super().close()
        self._update_session_iterator()


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


def get_field(react_context: list[dict], field: str) -> str | int | None:
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


def _get_release_year(react_context: list[dict], release_year: int) -> int:
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


def _get_content_type(react_context: list[dict]) -> str | None:
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
    return []


def _find_all_script_elements(html: HTMLContent) -> list[dict]:
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


def _sanitize_pythonmonkey_obj(obj: Any) -> Any:
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


async def save_response_body(response_body: HTMLContent, saveto_path: Path) -> None:
    if not response_body:
        return
    async with aiofiles.open(str(saveto_path), "w+") as f:
        minified = minify_html.minify(response_body, minify_css=True, minify_js=True)
        await f.write(minified)


async def get_serp_html(
    netflix_id: int,
    title: str,
    content_type: str,
    release_year: int,
    session: aiohttp.ClientSession = None,
) -> SERPResponse:
    if BRD_AUTH_TOKEN is None:
        return SERPResponse("", [])

    queries = _build_query(title, content_type, release_year, permute=True)

    html_content = ""
    ratings_list = []
    ct_ratings = 0

    for url in _build_google_urls(queries):
        try:
            # Kind of have to be sync about this... don't want to make more queries than necessary
            async with session.post(
                BRD_API_URL,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {BRD_AUTH_TOKEN}",
                },
                json={
                    "zone": BRD_ZONE,
                    "url": url,
                    "format": "raw",
                },
            ) as response:
                html = _get_html_from(response)

            ratings = await extract_reviews_from_serp(netflix_id, html)

            if (len_ := len(ratings)) > ct_ratings:
                # If we don't have Google user reviews, we can default to returning the page
                # with the most captured review elements
                ct_ratings = len_
                html_content = html
                ratings_list = ratings

            for rating in ratings:
                if rating.vendor == "Google users":
                    return SERPResponse(html, ratings)

        except aiohttp.ConnectionTimeoutError:
            break

        except json.JSONDecodeError as e:
            LOGGER.exception(e)
            continue

    return SERPResponse(html_content, ratings_list)


async def _get_html_from(response: aiohttp.ClientResponse):
    try:
        json_body = await response.json()
        html = json_body["html"]
    except aiohttp.ContentTypeError:
        html = await response.text()
    return html


def _build_query(
    title: str, content_type: str, release_year: int, permute: bool = False
) -> str | list[str]:
    query = f'"{title}" {content_type} ({release_year})'
    if permute:
        # Some alternative searches to consider
        # in case the Google user reviews snippet isn't present for the initial query
        alt1 = f"{title} ({content_type})"
        alt2 = f"{title} ({content_type}) reviews"
        alt3 = f"{title} ({release_year})"
        return [query, alt1, alt2, alt3]
    return query


def _build_google_urls(queries: list[str] | str) -> list[str]:
    base_url = "https://www.google.com/search"
    if isinstance(queries, str):
        queries = [queries]
    return [
        # https://docs.brightdata.com/scraping-automation/serp-api/query-parameters/google
        f"{base_url}?{urlencode({'q': query, 'brd_json': 'html', 'gl': 'us', 'hl': 'en', 'num': 100})}"
        for query in queries
    ]


async def extract_reviews_from_serp(netflix_id: int, html: HTMLContent) -> list[Review]:
    soup = BeautifulSoup(html, "html.parser")
    # TODO this isn't super robust e.g. for the Netflix title with ID 80107103,
    # the first Google search term that yielded results was:
    # "ONE PIECE" tv series (1999.0)
    # but the IMDb rating wasn't captured (though present on the page).
    # Granted, querying with the release year as a float was a bug that's been resolved,
    # but the code should be more robust.
    reviews = soup.select("[data-attrid$=reviews], [data-attrid$=thumbs_up]")
    reviews_list = []
    for review in reviews:
        reviews_list.extend(await _extract_linked_reviews(netflix_id, review))
        reviews_list.extend(await _extract_non_link_reviews(netflix_id, review))

    return reviews_list


async def _extract_linked_reviews(netflix_id: int, review: Tag) -> list[Review]:
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
                Review(
                    netflix_id=netflix_id,
                    url=a_tag["href"],
                    vendor=vendor,
                    rating=rating,
                    ratings_count=None,
                )
            )

    return reviews_list


async def _extract_non_link_reviews(netflix_id: int, review: Tag) -> list[Review]:
    """Extracts Google and Audience reviews where there are no links."""
    stripped_strings = list(review.stripped_strings)
    reviews_list = []

    if "Google users" in stripped_strings:
        reviews_list.append(
            Review(
                netflix_id=netflix_id,
                url=None,
                vendor="Google users",
                rating=_find_rating(stripped_strings[0]),
                ratings_count=None,
            )
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
                Review(
                    netflix_id=netflix_id,
                    url=None,
                    vendor="Audience rating summary",
                    rating=rating,
                    ratings_count=ct_ratings,
                )
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


async def main():
    netflix_id = 81578318
    async with aiohttp.ClientSession() as httpsession:
        with open(
            f"/Users/asibalo/Documents/Dev/PetProjects/netflix_critic_data/data/raw/title/{netflix_id}.html"
        ) as f:
            html = f.read()
            context = extract_netflix_react_context(html)
            serp_response = await get_serp_html(
                netflix_id,
                get_field(context, "title"),
                get_field(context, "content_type"),
                get_field(context, "release_year"),
                session=httpsession,
            )
            print(serp_response.ratings)


if __name__ == "__main__":
    asyncio.run(main())
