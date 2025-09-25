import re

import pandas as pd
from rapidfuzz import process
from unidecode import unidecode

"""
Required packages to run in notebook on local machine:
%pip install unidecode
%pip install rapidfuzz
"""


swedish_cities = [
    "alingsås",
    "björlanda",
    "boden",
    "boo",
    "borlänge",
    "borås",
    "bromma",
    "enköping",
    "eskilstuna",
    "eslöv",
    "falkenberg",
    "falköping",
    "falun",
    "gamla uppsala",
    "gustavsberg",
    "gävle",
    "göteborg",
    "halmstad",
    "haninge",
    "helsingborg",
    "huddinge",
    "hudiksvall",
    "huskvarna",
    "härnösand",
    "hässleholm",
    "jakobsberg",
    "jönköping",
    "kalmar",
    "karlshamn",
    "karlskoga",
    "karlskrona",
    "karlstad",
    "katrineholm",
    "kinna",
    "kiruna",
    "kristianstad",
    "kristinehamn",
    "kumla",
    "kungsbacka",
    "kungsholmen",
    "kungälv",
    "köping",
    "landskrona",
    "lerum",
    "lidingö",
    "lidköping",
    "linköping",
    "ljungby",
    "ludvika",
    "luleå",
    "lund",
    "majorna",
    "malmö",
    "mariestad",
    "motala",
    "märsta",
    "mölndal",
    "mölnlycke",
    "nacka",
    "norrköping",
    "norrtälje",
    "nyköping",
    "nässjö",
    "oskarshamn",
    "partille",
    "piteå",
    "råsunda",
    "sandviken",
    "skellefteå",
    "skövde",
    "sollentuna",
    "solna",
    "staffanstorp",
    "stockholm",
    "sundbyberg",
    "sundsvall",
    "södermalm",
    "södertälje",
    "tensta",
    "trelleborg",
    "trollhättan",
    "tullinge",
    "tumba",
    "täby",
    "uddevalla",
    "umeå",
    "upplands väsby",
    "uppsala",
    "vallentuna",
    "varberg",
    "vasastan",
    "visby",
    "vänersborg",
    "värnamo",
    "västervik",
    "västerås",
    "västra frölunda",
    "växjö",
    "ystad",
    "ängelholm",
    "åkersberga",
    "årsta",
    "örebro",
    "örnsköldsvik",
    "östermalm",
    "östersund",
]
swedish_cities_ascii = [unidecode(city.lower()) for city in swedish_cities]
manual_cities_ascii = {
    "copenhagen": "köpenhamn",
    "kopenhamn": "köpenhamn",
    "gothenburg": "göteborg",
    "skane lan": "skåne",
}


def clean_work_location_column(values):
    clean_values = values.apply(split_multiple_cities)
    clean_values = clean_values.apply(
        lambda x: ", ".join(match_decoded_cities(city.strip()) for city in x.split(","))
    )
    clean_values = clean_values.apply(
        lambda x: ", ".join(city.strip().capitalize() for city in x.split(","))
    )
    clean_values = clean_values.apply(summarize_cities)
    return clean_values


def split_multiple_cities(city):
    """Split multiple cityations but keep 'lan' and 'län' together."""
    if pd.isna(city):
        return ""

    city = city.lower()
    # Protect 'lan' and 'län' by temporarily replacing spaces before them
    city_protected = re.sub(
        r"(\b\w+)\s(lan|län)\b", r"\1__LAN__\2", city, flags=re.IGNORECASE
    )

    city_parts = re.split(
        r",|/|;|\s", city_protected
    )  # Split on comma, slash, semicolon, or remaining spaces
    city_parts = [
        p.replace("__LAN__", " ") for p in city_parts if p.strip()
    ]  # Restore 'lan' and 'län'

    drop_parts = ["area", "dk", "metropolitan", "se", "sverige"]
    city_parts = sorted([p for p in city_parts if p not in drop_parts])

    return ", ".join(city_parts)


def match_decoded_cities(city, match_score=80):
    """Match unicoded citys as malmo -> malmö"""
    if pd.isna(city) or city == "":
        return ""

    city_ascii = unidecode(city.lower())

    if city_ascii in manual_cities_ascii:
        return manual_cities_ascii[city_ascii]

    match = process.extractOne(
        city_ascii, swedish_cities_ascii
    )  # find closest match in ascii version
    if match and match[1] > match_score:
        return swedish_cities[
            swedish_cities_ascii.index(match[0])
        ]  # return original city from swedish_cities, not ascii version
    return city


def summarize_cities(row, max_cities=2):
    """
    Summarize multiple cities in a row for dashboards.
    - If more than max_cities, show first city + others.
    - Else, show all cities comma-separated.
    """
    if pd.isna(row) or row.strip() == "":
        return ""

    cities = [c.strip() for c in row.split(",")]

    if len(cities) > max_cities:
        return f"{cities[0]}..."
    else:
        return ", ".join(cities)
