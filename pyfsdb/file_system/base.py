from abc import ABCMeta, abstractmethod
from typing import Callable, Iterator, List, Optional, Tuple, TypeVar

from pytyped.macros.boxed import Boxed

A = TypeVar("A")
Key = List[str]  # Each string should only use base64 chars, i.e., `A`-`Z`, `a`-`z`, `0`-`9`, `+`, or `/`
Value = bytes


class FileSystem(metaclass=ABCMeta):
    @abstractmethod
    def read_data(self, key: Key) -> Optional[Value]:
        pass

    @abstractmethod
    def write_data(self, key: Key, contents: Value) -> None:
        pass

    @abstractmethod
    def prefix_scan(self, key_prefix: Key) -> Iterator[Tuple[Key, Value]]:
        pass

    @abstractmethod
    def lock_and_run(self, key: Key, f: Callable[[], A]) -> Optional[Boxed[A]]:
        pass


