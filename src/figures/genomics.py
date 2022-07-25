#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 08:29:14 2022

@author: tannervarrelman
"""


import numpy as np
from pathlib import Path

import pandas as pd


def aggregate(gh_data: pd.DataFrame, genome_data: pd.DataFrame) -> pd.DataFrame:

    genome_data = genome_data.assign(
        country=genome_data.country.replace("USA", "United States")
    ).rename(columns={"country": "Country"})

    genome_agg = (
        genome_data.reset_index(drop=True)
        .groupby("Country")
        .size()
        .reset_index(name="nextstrain_genome_count")
    )

    # confirmed Gh cases only, non-endemic (N)
    con_cases = gh_data[
        (gh_data.Status == "confirmed") & (gh_data.ID.str.startswith("N"))
    ].reset_index(drop=True)
    con_cases["Country"] = con_cases.Country.replace(
        ["England", "Scotland", "Wales", "Northern Ireland"], "United Kingdom"
    )
    agg_con_cases = (
        con_cases.groupby("Country")
        .size()
        .reset_index(name="Gh_confirmed_cases")
        .sort_values(by="Gh_confirmed_cases", ascending=False)
    )

    return agg_con_cases.merge(genome_agg, how="outer").replace(np.nan, 0)
