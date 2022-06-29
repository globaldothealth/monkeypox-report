import io
import random
import datetime
import urllib.parse

import pandas as pd
import pytest

import build

HEX = list(map(str, range(10))) + ["a", "b", "c", "d", "e", "f"]
SHA = "9c9dce36ed84fd2c3fde112249fe17450f885ab4"
DATA_REPO = "globaldothealth/monkeypox"

TODAY = pd.read_csv(
    io.StringIO(
        """Status,Country,Travel_history (Y/N/NA),Travel_history_location,Age,Gender
confirmed,USA,Y,,,male
suspected,USA,N,,20-40,male
confirmed,USA,N,,25-45,male
confirmed,USA,Y,London,31-40,male
confirmed,England,Y,,21-30,female
confirmed,England,N,,41-50,male
suspected,England,Y,New York,51-60,male
suspected,Belgium,N,,,male
discarded,England,NA,,41-50,male
"""
    )
)

YESTERDAY = pd.read_csv(
    io.StringIO(
        """Status,Country,Travel_history (Y/N/NA),Travel_history_location,Age,Gender
confirmed,USA,Y,,,male
suspected,USA,N,,20-40,male
confirmed,USA,Y,London,30-40,male
"""
    )
)


def date(year, day, month):
    return datetime.datetime(year, day, month).date()


def random_sha():
    return "".join(random.choices(HEX, k=41))


def random_size():
    return random.choice(range(100000, 500000))


def times(day: datetime.date, hours=[3, 6, 9, 12, 15, 18]):
    return [
        datetime.datetime(day.year, day.month, day.day, hour, 0, 0) for hour in hours
    ]


def github_archive_file(data_repo: str, filename: str, sha: str, size: int):
    space_escaped_filename = filename.replace(" ", "%20")
    quoted_filename = urllib.parse.quote(filename)

    url = f"https://api.github.com/repos/{data_repo}/contents/archives/{space_escaped_filename}?ref=main"
    html_url = (
        f"https://github.com/{data_repo}/blob/main/archives/{space_escaped_filename}"
    )
    git_url = f"https://api.github.com/repos/{data_repo}/git/blobs/{sha}"
    return {
        "name": filename,
        "path": f"archives/{filename}",
        "sha": sha,
        "size": size,
        "url": url,
        "html_url": html_url,
        "git_url": git_url,
        "download_url": f"https://raw.githubusercontent.com/{data_repo}/main/archives/{quoted_filename}",
        "type": "file",
        "_links": {"self": url, "git": git_url, "html": html_url},
    }


def github_archive_api(data_repo: str):
    datetimes = sum(list(map(times, [date(2022, 6, day) for day in range(13, 21)])), [])
    return sum(
        [
            [
                github_archive_file(
                    data_repo, f"{datetime}.csv", random_sha(), random_size()
                ),
                github_archive_file(
                    data_repo, f"{datetime}.json", random_sha(), random_size()
                ),
            ]
            for datetime in datetimes
        ],
        [],
    )


GITHUB_ARCHIVE_API = github_archive_api(DATA_REPO)


def test_get_archives_list():
    assert all(url.endswith("csv") for url in build.get_archives_list("csv"))


def test_last_file_on_date():
    links = [
        data["download_url"]
        for data in GITHUB_ARCHIVE_API
        if data["download_url"].endswith("csv")
    ]
    assert (
        build.last_file_on_date(links, date(2022, 6, 20))
        == "https://raw.githubusercontent.com/globaldothealth/monkeypox/main/archives/2022-06-20%2018%3A00%3A00.csv"
    )


@pytest.mark.parametrize(
    "source,expected",
    [
        (date(2022, 6, 20), (date(2022, 6, 17), date(2022, 6, 16), date(2022, 6, 13))),
        (date(2022, 6, 21), (date(2022, 6, 20), date(2022, 6, 17), date(2022, 6, 14))),
        (date(2022, 6, 22), (date(2022, 6, 21), date(2022, 6, 20), date(2022, 6, 15))),
    ],
)
def test_get_compare_days(source, expected):
    assert build.get_compare_days(source) == expected


def test_counts():
    assert build.counts(TODAY, YESTERDAY) == {
        "diff_countries": ["Belgium", "England"],
        "n_confirmed": 5,
        "n_confirmed_or_suspected": 8,
        "n_countries_confirmed": 2,
        "n_countries_confirmed_or_suspected": 3,
        "n_countries_discarded": 1,
        "n_countries_discarded_only": 0,
        "n_countries_suspected_only": 1,
        "n_diff_confirmed": 3,
        "n_diff_countries": 2,
        "n_suspected": 3,
        "n_travel_history": 3,
        "n_unknown_travel_history": 2,
        "text_diff_countries": ", and 2 new countries have been added to the list "
        "(Belgium, England)",
    }


@pytest.mark.parametrize(
    "source,expected",
    [(40, 40), ("40", None), ("20-30", 25), ("20-40", 30), ("0-5", 2.5)],
)
def test_mid_bucket_age(source, expected):
    assert build.mid_bucket_age(source) == expected


@pytest.mark.parametrize(
    "source,expected", [(5, 0), (80, 7), (90, 8), (100, 8), (0, 0), (39, 3), (41, 4)]
)
def test_age_bucket(source, expected):
    assert build.age_bucket(source) == expected


@pytest.mark.parametrize(
    "source,expected",
    [
        ("11 - 20", False),
        ("9 - 20", True),
        ("41 - 45", False),
        ("20 - 40", True),
        ("81 - 100", False),
    ],
)
def test_not_same_age_bucket(source, expected):
    assert build.not_same_age_bucket(source) == expected


def test_demographics():
    assert build.demographics(TODAY) == {
        "mean_age_confirmed_cases": 35,
        "pc_age_range_multiple_buckets": 25,
        "pc_valid_age_gender_in_confirmed": 80,
        "percentage_male": 80,
    }
