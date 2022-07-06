import io
import json
import random
import datetime
import urllib.parse

import pandas as pd
import pytest
import requests

import build

HEX = list(map(str, range(10))) + ["a", "b", "c", "d", "e", "f"]
SHA = "9c9dce36ed84fd2c3fde112249fe17450f885ab4"
DATA_REPO = "globaldothealth/monkeypox"


def dataframe(csv_data: str) -> pd.DataFrame:
    "Returns dataframe from CSV data"
    return pd.read_csv(io.StringIO(csv_data))


TODAY = dataframe(
    """ID,Status,Country,Travel_history (Y/N/NA),Travel_history_location,Age,Gender
N1,confirmed,USA,Y,,,male
N2,suspected,USA,N,,20-40,male
N3,confirmed,USA,N,,25-45,male
N4,confirmed,USA,Y,London,31-40,male
N5,confirmed,England,Y,,21-30,female
N6,confirmed,England,N,,41-50,male
N7,suspected,England,Y,New York,51-60,male
N8,suspected,Belgium,N,,,male
N9,discarded,England,NA,,41-50,male
N10,omit_error,Australia,Y,,30-40,male
"""
)

YESTERDAY = dataframe(
    """ID,Status,Country,Travel_history (Y/N/NA),Travel_history_location,Age,Gender
N1,confirmed,USA,Y,,,male
N2,suspected,USA,N,,20-40,male
N3,confirmed,USA,Y,London,30-40,male
"""
)

STATUS_ID_INTEGER = dataframe(
    """Status,ID
confirmed,1
confirmed,2
suspected,3
omit_error,4
"""
)

STATUS_ID_ALPHANUMERIC = dataframe(
    """Status,ID
confirmed,N1
confirmed,N2
suspected,N3
omit_error,N4
confirmed,E1
suspected,E2
"""
)

STATUS_ID_INTEGER_filtered = dataframe(
    """Status,ID
confirmed,1
confirmed,2
suspected,3
"""
)

STATUS_ID_ALPHANUMERIC_filtered = dataframe(
    """Status,ID
confirmed,N1
confirmed,N2
suspected,N3
"""
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


@pytest.mark.parametrize(
    "data,filtered_data",
    [
        (STATUS_ID_INTEGER, STATUS_ID_INTEGER_filtered),
        (STATUS_ID_ALPHANUMERIC, STATUS_ID_ALPHANUMERIC_filtered),
    ],
)
def test_initial_filter(data, filtered_data):
    assert build.initial_filter(data).equals(filtered_data)


@pytest.fixture
def successful_request(monkeypatch):
    response = requests.Response()
    monkeypatch.setattr(response, "json", lambda: GITHUB_ARCHIVE_API)
    monkeypatch.setattr(response, "status_code", 200)
    monkeypatch.setattr(requests, "get", lambda _: response)


@pytest.fixture
def failed_request(monkeypatch):
    response = requests.Response()
    monkeypatch.setattr(response, "status_code", 404)
    monkeypatch.setattr(requests, "get", lambda _: response)


def test_get_archives_list(successful_request):
    assert all(url.endswith("csv") for url in build.get_archives_list("csv"))


def test_get_archives_list_raises_exception(failed_request):
    with pytest.raises(ConnectionError, match="Failed to get archives list, aborting"):
        build.get_archives_list("csv")


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


def test_last_file_on_date_failure():
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)
    links = [f"http://foo.bar/{yesterday}.csv"]
    with pytest.raises(ValueError):
        build.last_file_on_date(links, today)


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


def test_percentage_occurrence():
    assert build.percentage_occurrence(TODAY, TODAY.Status == "confirmed") == 50  # 5/10


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"status": "confirmed"}, {"USA", "England"}),
        ({"status": "suspected"}, {"USA", "England", "Belgium"}),
        ({"status": "suspected", "only": True}, {"Belgium"}),
    ],
)
def test_countries(kwargs, expected):
    assert build.countries(TODAY, **kwargs) == expected


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"status": "confirmed"}, 2),
        ({"status": "suspected"}, 3),
        ({"status": "suspected", "only": True}, 1),
    ],
)
def test_n_countries(kwargs, expected):
    assert build.n_countries(TODAY, **kwargs) == expected


@pytest.mark.parametrize(
    "status,expected",
    [
        ("confirmed", 5),
        ("suspected", 3),
        ("discarded", 1),
        (["confirmed", "suspected"], 8),
    ],
)
def test_n_cases(status, expected):
    assert build.n_cases(TODAY, status) == expected


def test_travel_history_counts():
    assert build.travel_history_counts(TODAY) == {
        "n_travel_history": 3,
        "n_unknown_travel_history": 2,
    }


@pytest.mark.parametrize(
    "countries,expected",
    [
        (set(), ""),
        ({"Belgium"}, ", and 1 new country has been added to the list (Belgium)"),
        (
            {"Belgium", "Australia"},
            ", and 2 new countries have been added to the list (Australia, Belgium)",
        ),
    ],
)
def test_text_diff_countries(countries, expected):
    assert build.text_diff_countries(countries) == expected


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
    [(40, 40), ("40", 40), ("20-30", 25), ("20-40", 30), ("0-5", 2.5)],
)
def test_mid_bucket_age(source, expected):
    assert build.mid_bucket_age(source) == expected


@pytest.mark.parametrize(
    "source,expected", [(5, 0), (80, 7), (90, 8), (100, 8), (0, 0), (39, 3), (41, 4)]
)
def test_age_bucket(source, expected):
    assert build.age_bucket(source) == expected


def test_age_bucket_failure():
    with pytest.raises(ValueError):
        build.age_bucket(-1)
    with pytest.raises(ValueError):
        build.age_bucket(121)


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


def test_not_same_age_bucket_failure():
    with pytest.raises(ValueError, match=r"Invalid age.*"):
        build.not_same_age_bucket("85-121")


def test_demographics():
    assert build.demographics(TODAY) == {
        "mean_age_confirmed_cases": 35,
        "pc_age_range_multiple_buckets": 25,
        "pc_valid_age_gender_in_confirmed": 80,
        "percentage_male": 80,
    }
