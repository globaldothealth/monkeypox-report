import sys
import json
import logging
import argparse
import datetime
import subprocess
from typing import Final, Any, Tuple
from pathlib import Path

import yaml
import chevron
import pandas as pd
import requests
import inflect  # plurals, counts etc.
import boto3
import plotly.io

import choropleth
import figures.genomics as genomics

readable: Final = inflect.engine()
logger: Final = logging.getLogger()
logger.setLevel("INFO")

today: Final = datetime.datetime.today().date()
oneday: Final = datetime.timedelta(days=1)
week: Final = datetime.timedelta(days=7)

DATA_REPO: Final = "globaldothealth/monkeypox"
NEXTSTRAIN_FILE: Final = "nextstrain_monkeypox_hmpxv1_metadata.tsv"
DIFFERENCE_LAST_WEEK_COLUMN: Final = "% difference in cases compared to last week"

FIGURES: Final = [
    "delay-to-confirmation",
    "genomics",
    "age-gender",
]

if not (DATA_PATH := Path(__file__).parent / "data").exists():
    DATA_PATH.mkdir()
BUILD_PATH = Path(__file__).parent.parent / "build"


def fetch_nextstrain(bucket: str, date: datetime.date):
    s3 = boto3.client("s3")
    s3.download_file(
        bucket, f"{date}/{NEXTSTRAIN_FILE}", str(DATA_PATH / NEXTSTRAIN_FILE)
    )


def read_nextstrain():
    df = pd.read_csv(DATA_PATH / NEXTSTRAIN_FILE, sep="\t")
    df = df[df.host == "Homo sapiens"]
    if "outbreak_associated" in df.columns:
        df = df[df.outbreak_associated == "yes"]
    else:
        df = df[df.date >= "2022-05"]

    return {
        "n_genomes": len(df),
        "country_with_most_genomes": df.groupby("country")
        .size()
        .sort_values(ascending=False)
        .head(n=1)
        .axes[0][0],
    }


def fetch_urls(urls: list[str], corresponding_filenames: list[str]):
    for url, filename in zip(urls, corresponding_filenames):
        with (DATA_PATH / filename).open("wb") as fp:
            if (res := requests.get(url)).status_code != 200:
                logging.warning(f"Failed to download {url}")
            fp.write(res.content)


def get_archives_list(suffix: str = "") -> list[str]:
    contents_url = f"https://api.github.com/repos/{DATA_REPO}/contents/archives"
    if (res := requests.get(contents_url)).status_code != 200:
        raise ConnectionError("Failed to get archives list, aborting")
    return [
        item["download_url"]
        for item in res.json()
        if item["download_url"].endswith(suffix)
    ]


def last_file_on_date(links: list[str], date: datetime.date) -> str:
    try:
        return max(
            link for link in links if link.split("/")[-1].startswith(date.isoformat())
        )
    except ValueError:
        logging.error(f"No link found on {date}")
        raise


def get_compare_days(
    today: datetime.date,
) -> Tuple[datetime.date, datetime.date, datetime.date]:
    """Get tuple of (yesterday, day before yesterday, last week) dates
    to compare data.

    build is run early morning UTC and looks at the last file the day before
    and compares it to the last file from day before yesterday. This way,
    only full days are compared.

    No reporting on weekends, so shift today to Saturday so that we
    compare Thu -> Fri change for Monday report
    """
    weekday = today.isoweekday()
    if weekday in [6, 7]:  # Sat, Sun
        raise ValueError("Generating reports on weekend is not supported")
    if weekday in [3, 4, 5]:  # Wed, Thu, Fri
        return today - oneday, today - 2 * oneday, today - week
    if weekday == 2:  # Tue, compares Fri -> Mon
        return today - oneday, today - 4 * oneday, today - week
    if weekday == 1:  # Mon, compares Thu -> Fri
        return today - 3 * oneday, today - 4 * oneday, today - week


def input_files(links: list[str], today: datetime.date) -> dict[str, str]:
    """Get input files to compare for today"""

    yesterday, day_before_yesterday, last_week = get_compare_days(today)
    return {
        "file": last_file_on_date(links, yesterday),
        "previous_day_file": last_file_on_date(links, day_before_yesterday),
        "last_week_file": last_file_on_date(links, last_week),
    }


def initial_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Initial filtering applied to all data"""
    if df.ID.map(str).map(str.isdigit).all():  # older non-endemic list
        return df[df.Status != "omit_error"]
    else:
        return df[(df.Status != "omit_error") & df.ID.str.startswith("N")]


def table_confirmed_cases(df, prev_week_df: pd.DataFrame) -> dict[str, str]:
    """Returns variables to populate Table 1: Confirmed cases by country"""
    df = initial_filter(df)
    prev_week_df = initial_filter(prev_week_df)

    yesterday_counts = (
        df[df.Status == "confirmed"].groupby("Country").count()[["Status"]]
    )
    last_week_counts = (
        prev_week_df[prev_week_df.Status == "confirmed"]
        .groupby("Country")
        .count()[["Status"]]
    )
    table = yesterday_counts.merge(last_week_counts, on="Country").rename(
        columns={"Status_x": "Confirmed", "Status_y": "Confirmed_last_week"}
    )
    table[DIFFERENCE_LAST_WEEK_COLUMN] = (
        100 * (table.Confirmed - table.Confirmed_last_week) / table.Confirmed_last_week
    ).astype(int)
    return {
        "embed_table_confirmed_cases": table[["Confirmed", DIFFERENCE_LAST_WEEK_COLUMN]]
        .reset_index()
        .sort_values("Confirmed", ascending=False)
        .to_html(index=False)
    }


def n_cases(df: pd.DataFrame, status: str | list[str]) -> int:
    """Returns number of cases for a given status"""

    df = initial_filter(df)
    statuses = [status] if isinstance(status, str) else status
    return len(df[df.Status.isin(statuses)])


def countries(
    df: pd.DataFrame, status: str | list[str], only: bool = False
) -> set[str]:
    """Returns set of countries for a given status

    only: Whether to return number of countries only having that status(es)
    """
    df = initial_filter(df)
    statuses = [status] if isinstance(status, str) else status
    if not only:
        return set(df[df.Status.isin(statuses)].Country)
    else:
        return set(df[df.Status.isin(statuses)].Country) - set(
            df[~df.Status.isin(statuses)].Country
        )


def n_countries(df: pd.DataFrame, status: str | list[str], only: bool = False) -> int:
    """Returns number of countries for a given status

    only: Whether to return number of countries only having that status(es)
    """

    return len(countries(df, status, only))


def travel_history_counts(df: pd.DataFrame) -> dict[str, int]:
    df = initial_filter(df)
    df = df[df.Status == "confirmed"]
    return {
        "n_travel_history": len(df[df["Travel_history (Y/N/NA)"] == "Y"]),
        "n_unknown_travel_history": len(
            df[
                (df["Travel_history (Y/N/NA)"] == "Y")
                & pd.isnull(df.Travel_history_location)
            ]
        ),
    }


def text_diff_countries(countries: set) -> str:
    """Returns text about new countries"""
    if countries:
        return (
            f", and {len(countries)} new {readable.plural_noun('country', len(countries))} "
            f"{readable.plural_verb('has', len(countries))} been added to the list ({', '.join(sorted(countries))})"
        )
    else:
        return ""


def counts(df: pd.DataFrame, prev_df: pd.DataFrame) -> dict[str, int]:
    """Return count variables from data

    df: Today's data file as a dataframe
    prev_df: Previous day's data file as a dataframe
    """
    new_countries = sorted(
        countries(df, ["confirmed", "suspected"])
        - countries(prev_df, ["confirmed", "suspected"])
    )
    return {
        "n_countries_confirmed_or_suspected": n_countries(
            df, ["confirmed", "suspected"]
        ),
        "n_countries_confirmed": n_countries(df, "confirmed"),
        "n_countries_suspected_only": n_countries(df, "suspected", only=True),
        "n_countries_discarded": n_countries(df, "discarded"),
        "n_countries_discarded_only": n_countries(df, "discarded", only=True),
        "n_confirmed": n_cases(df, "confirmed"),
        "n_suspected": n_cases(df, "suspected"),
        "n_confirmed_or_suspected": n_cases(df, ["confirmed", "suspected"]),
        "n_diff_confirmed": n_cases(df, "confirmed") - n_cases(prev_df, "confirmed"),
        "diff_countries": new_countries,
        "n_diff_countries": len(new_countries),
        "text_diff_countries": text_diff_countries(new_countries),
        **travel_history_counts(df),
    }


def travel_history(df: pd.DataFrame) -> dict[str, str]:
    df = initial_filter(df)
    df_travel = df[df["Travel_history (Y/N/NA)"] == "Y"]
    travel_counts_by_country = [
        (country, len(group)) for country, group in df_travel.groupby("Country")
    ]
    return {
        "text_travel_history": ", ".join(
            f"{n} were from {country}" for country, n in travel_counts_by_country
        )
    }


def mid_bucket_age(age_interval: str) -> float:
    try:  # if age_interval is a number, return that
        return float(age_interval)
    except ValueError:
        pass
    try:
        start, end = list(map(float, age_interval.split("-")))
        return (start + end) / 2
    except Exception as e:
        logging.exception(e)
        return None


def age_bucket(age: int) -> int:
    """Returns age bucket from

    0 - 10, 11 - 20, 21 - 30, 31 - 40, 41 - 50,
    51 - 60, 61 - 70, 71 - 80, 81 -
    """
    if age < 0 or age > 120:
        raise ValueError(f"Invalid age {age}")
    if age == 0:
        return 0
    # ages above 80 are in the same bucket
    return min((age - 1) // 10, 8)


def not_same_age_bucket(age: str) -> bool:
    try:
        start_age, end_age = list(map(lambda x: int(x.strip()), age.split("-")))
    except ValueError:
        return False
    return age_bucket(start_age) != age_bucket(end_age)


def percentage_occurrence(df: pd.DataFrame, filter_series: pd.Series) -> int:
    """Returns percentage occurrence of filter_series within a dataframe"""
    return int(round(100 * sum(filter_series) / len(df)))


def demographics(df: pd.DataFrame) -> dict[str, int]:
    df = initial_filter(df)
    df = df[df.Status == "confirmed"]
    df["Age_mid"] = df.Age.map(mid_bucket_age)
    valid_age_df = df[~pd.isnull(df.Age_mid)]
    valid_age_gender = df[
        (df.Age != "<40") & (~df.Age.isna()) & (df.Gender.isin(["male", "female"]))
    ].reset_index(drop=True)
    return {
        "mean_age_confirmed_cases": int(valid_age_df.Age_mid.mean()),
        "percentage_male": percentage_occurrence(
            df[~pd.isnull(df.Gender)], df.Gender == "male"
        ),
        "pc_valid_age_gender_in_confirmed": percentage_occurrence(
            df, (~df.Age.isna()) & (~df.Gender.isna()) & (df.Age != "<40")
        ),
        "pc_age_range_multiple_buckets": percentage_occurrence(
            valid_age_gender, valid_age_gender.Age.map(not_same_age_bucket)
        ),
    }


def delay_suspected_to_confirmed(df: pd.DataFrame) -> dict[str, Any]:
    """Returns mean and median delay from a case going from suspected to confirmed"""

    df = initial_filter(df)
    df = df[df.Status == "confirmed"]
    delay_df = df[df.Date_entry < df.Date_confirmation].assign(
        Date_entry=pd.to_datetime(df.Date_entry),
        Date_confirmation=pd.to_datetime(df.Date_confirmation),
    )
    delay_df["Delay"] = delay_df.Date_confirmation - delay_df.Date_entry
    return {
        "mean_delay_suspected_confirmed": round(
            delay_df.Delay.mean().total_seconds() / 86400, 2
        ),
        "median_delay_suspected_confirmed": delay_df.Delay.median().days,
        "n_suspected_confirmed": len(delay_df),
    }


def render(template: Path, variables: dict[str, Any], output: Path):
    with template.open() as f:
        output.write_text(chevron.render(f, variables))


def render_figure(fig, key: str) -> str:
    return {key: plotly.io.to_html(fig, include_plotlyjs=False, full_html=False)}


def build(
    fetch_bucket: str,
    date: datetime.date,
    skip_fetch: bool = False,
    skip_figures: bool = False,
    overrides_file: str = "overrides.yml",
):
    """Build Monkeypox epidemiological report for a particular date"""
    var = {}
    with open(overrides_file) as fp:
        overrides = yaml.safe_load(fp)
    date = date or today
    if date in overrides:
        overrides = overrides[date]
        logging.info(f"Found overrides for {date} in {overrides_file}")
        logging.info(yaml.dump(overrides))
    else:
        overrides = {}
    if not skip_fetch:
        logging.info("Fetch nextstrain data from S3")
        fetch_nextstrain(fetch_bucket, date)
    var.update(read_nextstrain())
    yesterday, day_before_yesterday, _ = get_compare_days(date)
    var.update(
        {
            "date": date.isoformat(),
            "yesterday": yesterday.isoformat(),
            "day_before_yesterday": day_before_yesterday.isoformat(),
        }
    )

    try:
        var.update(input_files(get_archives_list("csv"), date))
    except ValueError as e:
        logging.error(e)
        sys.exit(1)
    except ConnectionError as e:
        logging.error(e)
        sys.exit(1)

    if not skip_fetch:
        logging.info("Fetch yesterday, day before yesterday, and last week's files")
        var.update(overrides)
        fetch_urls(
            [var["file"], var["previous_day_file"], var["last_week_file"]],
            ["yesterday.csv", "day_before_yesterday.csv", "last_week.csv"],
        )
    genomics.aggregate(DATA_PATH / "yesterday.csv", DATA_PATH / NEXTSTRAIN_FILE).to_csv(
        DATA_PATH / "genomics.csv",
        header=True,
        index=False,
    )
    df = pd.read_csv(DATA_PATH / "yesterday.csv")
    prev_df = pd.read_csv(DATA_PATH / "day_before_yesterday.csv")
    last_week_df = pd.read_csv(DATA_PATH / "last_week.csv")

    var.update(counts(df, prev_df))
    var.update(table_confirmed_cases(df, last_week_df))
    var.update(travel_history(df))
    var.update(demographics(df))
    var.update(delay_suspected_to_confirmed(df))
    var.update(overrides)
    var.update(render_figure(choropleth.figure(df), "embed_choropleth"))
    var.update(render_figure(choropleth.figure_counts(df), "embed_counts"))

    # remove these for now
    del var["text_travel_history"]
    logging.info("Rendering index.html")

    render(Path(__file__).parent / "index.html", var, BUILD_PATH / "index.html")

    logging.info("Writing variables to index.json")
    with (BUILD_PATH / "index.json").open("w") as fp:
        json.dump(
            {k: v for k, v in var.items() if not k.startswith("embed_")},
            fp,
            indent=2,
            sort_keys=True,
        )

    if not skip_figures:
        for figure in FIGURES:
            logging.info(f"Generating figure {figure}")
            subprocess.run(["Rscript", f"src/figures/{figure}.r"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Monkeypox epidemiology report")
    parser.add_argument("bucket", help="S3 bucket to fetch genomics data from")
    parser.add_argument("--date", help="Build report for date instead of today")
    parser.add_argument(
        "--skip-fetch", help="Skip data fetch and use cached files", action="store_true"
    )
    parser.add_argument(
        "--skip-figures", help="Skip figure generation", action="store_true"
    )
    parser.add_argument(
        "--overrides", help="Specify overrides file", default="overrides.yml"
    )
    args = parser.parse_args()
    build(
        args.bucket,
        date=datetime.datetime.fromisoformat(args.date).date()
        if args.date
        else datetime.datetime.today().date(),
        skip_fetch=args.skip_fetch,
        skip_figures=args.skip_figures,
        overrides_file=args.overrides,
    )
