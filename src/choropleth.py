import random
from pathlib import Path
from typing import Optional

import pycountry
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import logging

random.seed(0)

alpha_3 = {
    country.alpha_3: getattr(country, "common_name", None) or country.name
    for country in pycountry.countries
}

TRAVEL_HISTORY_LINEWIDTH = 0.9

BINS = [-1, 0, 9, 100, 500, 2000, 5000]
COLORS = [
    "rgb(216, 232, 236)",  # NoData
    "rgb(136, 208, 235)",  # <10
    "rgb(100, 198, 240)",  # 10-100
    "rgb(41, 177, 234)",  # 101-500
    "rgb(0, 147, 228)",  # 501-2000
    "rgb(0, 116, 171)",  # 2001-5000
    "rgb(34, 88, 147)",  # >5000
]

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


def interval_str(interval: pd.Interval) -> str:
    left = int(interval.left)
    right = int(interval.right)
    if left + 1 == right and interval.closed == "right":
        return str(right)
    if interval.closed == "right":
        left += 1
    elif interval.closed == "left":
        right -= 1
    elif interval.closed == "neither":
        left += 1
        right -= 1
    else:
        pass
    return f"{left} - {right}"


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
        .assign(Date_confirmation=pd.to_datetime(df.Date_confirmation))
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
        .assign(Date_confirmation=pd.to_datetime(df.Date_confirmation))
        .sort_values("Date_confirmation")
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
                data.Travel_history_country_ISO3.map(list) +
                data.Country_ISO3.map(singleton_list)
            ).map(tuple)
        )
        .groupby("Travel_route_ISO3")
        .Travel_history_entry.agg([list, len])
    )
    th = th.assign(list=th.list.map(lambda xs: [x for x in xs if isinstance(x, str)]))
    th = th.assign(Travel_route=th.index.map(lambda xs: [alpha_3.get(x) for x in xs]))
    return th


def figure(data: pd.DataFrame):
    df = counts(data)
    th = travel_history(data)
    binned_counts = (
        pd.cut(df.Count, bins=BINS).map(interval_str).replace({"0": "0 or no data"})
    )
    fig = px.choropleth(
        df,
        locations="Country_ISO3",
        color=binned_counts,
        hover_name="Country",
        hover_data=dict(Country_ISO3=False, Count=True),
        category_orders=dict(color=binned_counts.cat.categories),
        color_discrete_sequence=COLORS,
        labels=dict(color="Cases"),
    )

    fig.update_layout(
        title_text="<b>A</b>. Confirmed monkeypox cases",
        legend_orientation="h",
        margin={"r": 0, "t": 30, "l": 0, "b": 0},
        geo=dict(
            showframe=False, showcoastlines=False, projection_type="equirectangular"
        ),
    )
    for row in th.itertuples():
        fig.add_trace(
            go.Scattergeo(
                lon=travel_history_coords(row.Index, "longitude"),
                lat=travel_history_coords(row.Index, "latitude"),
                name=" ➔ ".join(row.Travel_route)
                + "<br>"
                + "<br>".join(row.list),
                mode="lines",
                line=dict(width=TRAVEL_HISTORY_LINEWIDTH, color="#505050"),
            )
        )
    fig.update_traces(
        showlegend=False, hovertemplate="\b\b", selector=dict(type="scattergeo")
    )
    return fig


def figure_counts(data: pd.DataFrame):
    cca = cumulative_counts(data)
    cco = cumulative_countries(data)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=cca.Date_confirmation, y=cca.Cumulative_cases, name="Cases"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=cco.Date_confirmation,
            y=cco.Cumulative_countries,
            name="Countries",
            mode="lines+markers",
        ),
        secondary_y=True,
    )

    fig.update_xaxes(title_text="Confirmation date")

    fig.update_yaxes(title_text="Cumulative cases", secondary_y=False)
    fig.update_yaxes(title_text="Countries", secondary_y=True, range=(0, 250))
    fig.update_layout(plot_bgcolor="white", title_text="<b>B</b>")
    return fig


def travel_history_coords(countries: list[str], key: str) -> list[float]:
    MAX = {"latitude": 90, "longitude": 180}
    return [
        min(centroids_dict[key][c] + 2 * random.random(), MAX[key]) for c in countries
    ]
