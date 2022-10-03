"""Sprawlbase registry and execution logic

Helper functions to register datasets and run SQL against them
"""
from typing import Dict, List

import dask.dataframe as dd
from dask_sql.context import Context


def query_csv_dataset(sql: str, datasources: Dict[str, str]) -> List[Dict]:
    dask_sql_context = Context()

    if datasources:
        for datasource, datasource_location in datasources.items():
            df = dd.read_csv(datasource_location)
            dask_sql_context.create_table(datasource, df)

    result = dask_sql_context.sql(sql)
    res = result.compute()
    return res
