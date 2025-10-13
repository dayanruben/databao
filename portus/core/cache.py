from io import BytesIO
from abc import ABC, abstractmethod


class Cache(ABC):
    @abstractmethod
    def put(self, key: str, source: BytesIO) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str, dest: BytesIO) -> None:
        raise NotImplementedError

    @abstractmethod
    def scoped(self, scope: str) -> "Cache":
        raise NotImplementedError
