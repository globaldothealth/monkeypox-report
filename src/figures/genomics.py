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
    genome_data.country = genome_data.country.replace(["USA"], ["United States"])
    genome_data.columns = genome_data.columns.str.replace("country", "Country")

    # Out break associated genomes only
    out_yes = genome_data[
        (genome_data.outbreak_associated == "yes") & (genome_data.host == "human")
    ].reset_index(drop=True)
    genome_agg = (
        out_yes.groupby(["Country"])["Country"]
        .size()
        .reset_index(name="nextstrain_genome_count")
    )

    # confirmed Gh cases only
    con_cases = gh_data[gh_data.Status == "confirmed"].reset_index(drop=True)
    con_cases.Country = con_cases.Country.replace(
        ["England", "Scotland", "Wales", "Northern Ireland"], "United Kingdom"
    )
    agg_con_cases = (
        con_cases.groupby(["Country"])["Country"]
        .size()
        .reset_index(name="Gh_confirmed_cases")
        .sort_values(by="Gh_confirmed_cases", ascending=False)
    )

    # merge aggregate counts together, and replace nan with 0
    gh_nextstrain_merge = agg_con_cases.merge(
        genome_agg, left_on="Country", right_on="Country", how="outer"
    )
    gh_nextstrain_merge = gh_nextstrain_merge.replace(np.nan, 0)

    return gh_nextstrain_merge
