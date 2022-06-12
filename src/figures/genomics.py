#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 08:29:14 2022

@author: tannervarrelman
"""


import numpy as np
from pathlib import Path

import pandas as pd


def aggregate(gh_data_path: Path, nextstrain_path: Path) -> pd.DataFrame:

    gh_data = pd.read_csv(gh_data_path)
    genome_data = pd.read_csv(nextstrain_path, sep="\t")
    # Change some names so we can merge dataframes after aggregation

    genome_data = genome_data.assign(
        country=genome_data.country.replace("USA", "United States")
    ).rename(columns={"country": "Country"})

    # Out break associated genomes only
    genome_agg = (
        genome_data[
            (genome_data.outbreak_associated == "yes")
            & (genome_data.host == "Homo sapiens")
        ]
        .reset_index(drop=True)
        .groupby("Country")
        .size()
        .reset_index(name="nextstrain_genome_count")
    )

    # confirmed Gh cases only
    con_cases = gh_data[gh_data.Status == "confirmed"].reset_index(drop=True)
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
