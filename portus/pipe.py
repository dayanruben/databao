import abc
from abc import ABC
from typing import Optional, Any

from pandas import DataFrame
from langchain_core.language_models.chat_models import BaseChatModel

from portus.opa import Opa
from portus.vizualizer import Visualizer, VisualisationResult
from portus.data_executor import DataExecutor, DataResult


class Pipe(ABC):
    @abc.abstractmethod
    def df(self, *, rows_limit: Optional[int] = None) -> DataFrame:
        pass

    @abc.abstractmethod
    def plot(self, request: str = "visualize data", *, rows_limit: Optional[int] = None):
        pass

    @abc.abstractmethod
    def meta(self) -> dict[str, Any]:
        pass

    @abc.abstractmethod
    def text(self) -> str:
        pass

    @abc.abstractmethod
    def ask(self, query: str) -> "Pipe":
        pass


class LazyPipe(Pipe):
    def __init__(
            self,
            llm: BaseChatModel,
            data_executor: DataExecutor,
            visualizer: Visualizer,
            dbs: dict[str, Any],
            dfs: dict[str, DataFrame],
            *,
            default_rows_limit: int = 1000
    ):
        self.__llm = llm
        self.__dbs = dict(dbs)
        self.__dfs = dict(dfs)
        self.__data_executor = data_executor
        self.__visualizer = visualizer
        self.__default_rows_limit = default_rows_limit

        self.__data_materialized = False
        self.__data_materialized_rows: Optional[int] = None
        self.__data_result: Optional[DataResult] = None
        self.__visualization_materialized = False
        self.__visualization_result: Optional[VisualisationResult] = None
        self.__opas: list[Opa] = []

    def __materialize_data(self, rows_limit: Optional[int]) -> DataResult:
        rows_limit = rows_limit if rows_limit else self.__default_rows_limit
        if not self.__data_materialized or rows_limit != self.__data_materialized_rows:
            self.__data_result = self.__data_executor.execute(self.__opas, self.__llm, self.__dbs, self.__dfs,
                                                              rows_limit=rows_limit)
            self.__data_materialized = True
            self.__data_materialized_rows = rows_limit
        return self.__data_result

    def __materialize_visualization(self, request: str, rows_limit: Optional[int]) -> VisualisationResult:
        self.__materialize_data(rows_limit)
        if not self.__visualization_materialized:
            self.__visualization_result = self.__visualizer.visualize(request, self.__llm, self.__data_result)
            self.__visualization_materialized = True
        return self.__visualization_result

    def df(self, *, rows_limit: Optional[int] = None) -> DataFrame:
        return self.__materialize_data(rows_limit if rows_limit else self.__data_materialized_rows).df

    def plot(self, request: str = "visualize data", *, rows_limit: Optional[int] = None):
        return self.__materialize_visualization(request,
                                                rows_limit if rows_limit else self.__data_materialized_rows).plot

    def meta(self) -> dict[str, Any]:
        if self.__data_materialized:
            return self.__materialized_meta
        raise ValueError("Result is not materialized")

    def text(self) -> str:
        return self.__materialize_data(self.__data_materialized_rows).text

    def __str__(self):
        return self.text()

    def ask(self, query: str) -> Pipe:
        self.__opas.append(Opa(query=query))
        return self
