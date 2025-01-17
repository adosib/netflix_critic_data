from typing import Any, NewType
from pathlib import Path
from collections import defaultdict

import aiohttp
import aiofiles
import minify_html
import pythonmonkey as pm
from bs4 import BeautifulSoup
from aiolimiter import AsyncLimiter

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


def get_field(parsed_data: list[dict], field: str):
    fields = {}
    hero_data = _parse_hero_data(parsed_data)
    fields["title"] = hero_data.get("title")
    fields["runtime"] = hero_data.get(
        "runtime"
    )  # NOTE: shows don't have a runtime attribute (their episodes do)
    release_year = hero_data.get("year")
    fields["release_year"] = _get_release_year(parsed_data, release_year)
    fields["content_type"] = _get_content_type(parsed_data)
    return fields.get(field)


def _parse_hero_data(parsed_data) -> dict:
    try:
        return parsed_data[0]["data"]["details"][0]["data"]
    except (TypeError, IndexError):
        return {}


def _get_release_year(parsed_data: list[dict], release_year: int):
    for item in parsed_data:
        if item["type"] == "seasonsAndEpisodes":
            try:
                for season in item["data"]["seasons"]:
                    for episode in season["episodes"]:
                        if episode["year"] > 1900 and episode["year"] < release_year:
                            release_year = episode["year"]
            except (TypeError, KeyError):
                return release_year
    return release_year


def _get_content_type(parsed_data: list[dict]):
    for item in parsed_data:
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
