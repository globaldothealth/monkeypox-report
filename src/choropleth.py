from pathlib import Path
from typing import Optional

import pycountry
import pandas as pd

import logging

alpha_3 = set(country.alpha_3 for country in pycountry.countries)

COUNTRY_QUIRKS = {
    "england": "GBR",
    "scotland": "GBR",
    "wales": "GBR",
    "northern ireland": "GBR",
    "uae": "UAE",
}

# replace general locations with approximate countries
TRAVEL_HISTORY_QUIRKS = {
    "Africa": "Mali",  # middle-ish of West Africa
    "Gran Canaria": "Spain",
    "Canary Islands": "Spain",
    "Europe": "Luxembourg",  # middle-ish of Europe
    "Western Europe": "Luxembourg",
    "West Africa": "Mali",
}


def get_iso_alpha_3(country: str) -> Optional[str]:
    country = country.lower()
    try:
        return (
            COUNTRY_QUIRKS.get(country) or pycountry.countries.lookup(country).alpha_3
        )
    except LookupError as e:
        logging.error(e)
        return None


def counts(data: pd.DataFrame) -> pd.DataFrame:
    data = data[data.Status == "confirmed"].groupby("Country_ISO3").size()
    for country in alpha_3 - set(data.index):
        data[country] = 0
    return data.reset_index().rename({0: "Count"}, axis=1)


def travel_history(data: pd.DataFrame) -> pd.DataFrame:
    data = data[(data.Status == "confirmed") & ~pd.isna(data.Travel_history_country)]
    data = data.assign(
        Travel_history_country=data.Travel_history_country.replace(
            TRAVEL_HISTORY_QUIRKS
        ).map(lambda s: [k.strip() for k in s.replace(",", ";").split(";")])
    )
    data = data.assign(
        Travel_history_country_ISO3=data.Travel_history_country.map(
            lambda countries: tuple(map(get_iso_alpha_3, countries))
        )
    )
    data = data[data.Country_ISO3 != data.Travel_history_country_ISO3]
    th = data.groupby("Travel_history_country_ISO3").Travel_history_entry.agg(
        [list, len]
    )
