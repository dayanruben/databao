from dataclasses import dataclass

import pandas as pd
from duckdb import DuckDBPyConnection
from sqlalchemy import Connection, Engine


@dataclass
class DataSource:
    name: str
    context: str


@dataclass
class DFDataSource(DataSource):
    df: pd.DataFrame


@dataclass
class DBDataSource(DataSource):
    db_connection: DuckDBPyConnection | Engine | Connection


@dataclass
class Sources:
    dfs: dict[str, DFDataSource]
    dbs: dict[str, DBDataSource]
    additional_context: list[str]
