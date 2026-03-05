# -*- coding: utf-8 -*-
"""
Created on Wed Sep 24 17:23:41 2025

@author: dowes
"""

import polars as pl

itr = '03'                            

def both_sexes_1933_2023():
    df_mort = pl.read_csv(r"data\USA_both_1933-2023.psv",separator='|')
    df_mort = df_mort.rename({'Year': 'year', 'Age': 'age'})
    enum_sex  = pl.Enum(["M", "F"])
    df_mort = df_mort.with_columns(pl.col('sex').cast(enum_sex))
    return df_mort



