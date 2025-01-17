from typing import Any, NewType
from collections import defaultdict

import pythonmonkey as pm
from bs4 import BeautifulSoup

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
