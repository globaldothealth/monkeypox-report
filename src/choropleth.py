import random
from pathlib import Path
from typing import Optional

import pycountry
import pandas as pd
import geopandas as gpd

import logging

random.seed(0)

alpha_3 = {
    country.alpha_3: getattr(country, "common_name", None) or country.name
    for country in pycountry.countries
}

COUNTRY_QUIRKS = {
    "england": "GBR",
    "scotland": "GBR",
    "wales": "GBR",
    "northern ireland": "GBR",
    "uae": "ARE",
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


def _centroids():
    import geopandas as gpd

    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

    centroids = world.centroid.to_crs(epsg=4326)
    centroid_list = (
        pd.concat([world.iso_a3, centroids], axis=1)
        .set_index("iso_a3")
        .rename({0: "centroid"}, axis=1)
    )
    return centroid_list.assign(
        longitude=centroid_list.centroid.map(lambda point: point.x),
        latitude=centroid_list.centroid.map(lambda point: point.y),
    )[["latitude", "longitude"]]


centroids = _centroids()
centroids.loc["SGP"] = 1.28992, 103.85097
centroids.loc["UAE"] = 23.991, 53.987
centroids_dict = centroids.to_dict()


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
    for country in set(alpha_3) - set(data.index):
        data[country] = 0
    return (
        data.reset_index()
        .rename({0: "Count"}, axis=1)
        .assign(Country=data.index.map(alpha_3.get))
    )


def cumulative_countries(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[df.Status == "confirmed"]
        .sort_values("Date_confirmation")
        .drop_duplicates("Country_ISO3")
        .groupby("Date_confirmation")
        .size()
        .cumsum()
        .reset_index()
        .rename({0: "Cumulative_countries"}, axis=1)
    )


def cumulative_counts(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[df.Status == "confirmed"]
        .groupby("Date_confirmation")
        .size()
        .cumsum()
        .reset_index()
        .rename({0: "Cumulative_cases"}, axis=1)
    )


def singleton_list(x):
    return [x]


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
    # remove cases where travel country is same as country reported
    data = data[
        data.apply(
            lambda row: row.Country_ISO3 not in row.Travel_history_country_ISO3, axis=1
        )
    ]
    # add country where case is reported to list
    th = (
        data.assign(
            Travel_route_ISO3=(
                data.Country_ISO3.map(singleton_list)
                + data.Travel_history_country_ISO3.map(list)
            ).map(tuple)
        )
        .groupby("Travel_route_ISO3")
        .Travel_history_entry.agg([list, len])
    )
    th = th.assign(list=th.list.map(lambda xs: [x for x in xs if isinstance(x, str)]))
    th = th.assign(Travel_route=th.index.map(lambda xs: [alpha_3.get(x) for x in xs]))
    return th


def figure(df: pd.DataFrame):
    fig = go.Figure(
        data=go.Choropleth(
            locations=df.Country_ISO3,
            z=df.Count,
            text=df.Country,
            colorscale="Blues",
            autocolorscale=False,
            marker_line_color="darkgray",
            marker_line_width=0.5,
        )
    )
    fig.update_layout(
        title_text="Confirmed monkeypox cases",
        geo=dict(
            showframe=False, showcoastlines=False, projection_type="equirectangular"
        ),
    )
    for row in th.itertuples():
        fig.add_trace(
            go.Scattergeo(
                lon=choropleth.travel_history_coords(row.Index, "longitude"),
                lat=choropleth.travel_history_coords(row.Index, "latitude"),
                name=" ➔ ".join(reversed(row.Travel_route))
                + "<br>"
                + "<br>".join(row.list),
                mode="lines",
                line=dict(width=0.7, color="#505050"),
            )
        )
    fig.update_traces(
        hovertemplate="%{text}<br>%{z}<extra></extra>", selector=dict(type="choropleth")
    )
    fig.update_traces(
        showlegend=False, hovertemplate="\b\b", selector=dict(type="scattergeo")
    )
    fig.update_traces(
        colorbar_orientation="h",
        colorbar_y=-0.1,
        colorbar_thickness=15,
        selector=dict(type="choropleth"),
    )

    fig.show()


def travel_history_coords(countries: list[str], key: str) -> list[float]:
    MAX = {"latitude": 90, "longitude": 180}
    return [
        min(centroids_dict[key][c] + 2 * random.random(), MAX[key]) for c in countries
    ]
