from dataclasses import dataclass
from random import random
from time import sleep
from typing import Callable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar

from pyfsdb.file_system.base import FileSystem


Key = List[str]
Value = bytes


@dataclass
class DatabaseException(Exception):
    pass


@dataclass
class InvalidKeyException(DatabaseException):
    key: Key


@dataclass
class LockNotObtainedException(DatabaseException):
    key: Key


A = TypeVar("A")


@dataclass
class RetryStrategy:
    max_retries: int = 0
    initial_backoff_seconds: float = 0.1

    additive_increase: float = 0.0
    multiplicative_increase: float = 0.0

    min_jitter_additive: float = 0.0
    min_jitter_multiplicative: float = 0.0

    max_jitter_additive: float = 0.0
    max_jitter_multiplicative: float = 0.0


@dataclass
class UntypedStore:
    fs: FileSystem
    base_key: Key
    retry_strategy: RetryStrategy = RetryStrategy()

    def get(self, key: Key) -> Optional[Value]:
        return self.fs.read_data(self.base_key + key)

    # raises an exception if `key` is locked.
    def put(self, key: Key, value: Value, retry_strategy: Optional[RetryStrategy] = None) -> None:
        def write_to_file() -> None:
            self.fs.write_data(self.base_key + key, value)

        if retry_strategy is None:
            retry_strategy = self.retry_strategy
        self._lock(key, write_to_file, retry_strategy)

    # The scan method does not lock anything and it is only guaranteed to work as expected if no other process changes
    # the state of the database while a scan is going on.
    # If the state of the database changes in between, the scanner method may return key-value pairs that no longer
    # exist or it may skips key-value pairs that are generated and/or updated in the meantime.
    # The returned keys are partial meaning that the base_key of the current instance of UntypedStore is removed.
    def prefix_scan(self, key_prefix: Key) -> Iterator[Tuple[Key, Value]]:
        def acceptable_base(key: Key) -> bool:
            for index, part in enumerate(self.base_key):
                if len(key) < index + 1 or key[index] != part:
                    return False
            return True

        for key, value in self.fs.prefix_scan(self.base_key + key_prefix):
            if acceptable_base(key):
                yield key[len(self.base_key):], value

    # raises an exception if `key` is locked.
    # returns false if the value did not match the expected value
    # returns true if everything went well.
    def compare_and_put(
        self,
        key: Key,
        expected: Optional[Value],
        new: Value,
        retry_strategy: Optional[RetryStrategy] = None
    ) -> bool:
        def compare_contents() -> bool:
            existing_data = self.fs.read_data(self.base_key + key)
            if existing_data == expected:
                UntypedStore.fs.write_data(self.base_key + key, new)
                return True
            return False

        if retry_strategy is None:
            retry_strategy = self.retry_strategy
        return self._lock(key, compare_contents, retry_strategy)

    # raises an exception if `key` is locked.
    # f is the function that is called with only parameter: the file that is guaranteed to be locked.
    # the file itself is not guaranteed to exist but the directory possibly containing that file is guaranteed to exist.
    def _lock(self, key: Key, f: Callable[[], A], retry_strategy: RetryStrategy) -> A:
        retry_count: int = retry_strategy.max_retries
        backoff: float = retry_strategy.initial_backoff_seconds
        while retry_count >= 0:
            result = self.fs.lock_and_run(self.base_key + key, f)
            if result is None:
                if retry_count > 0:
                    min_jitter = retry_strategy.min_jitter_multiplicative * backoff + retry_strategy.min_jitter_additive
                    max_jitter = retry_strategy.max_jitter_multiplicative * backoff + retry_strategy.max_jitter_additive
                    jitter = min_jitter + random() * (max_jitter - min_jitter)
                    sleep(backoff + jitter)
                    backoff += retry_strategy.multiplicative_increase * backoff + retry_strategy.additive_increase

                retry_count -= 1
            else:
                return result.t

        raise LockNotObtainedException(key)

    # raises an exception if `key` is locked.
    def lock_and_run(
        self,
        key: Key,
        f: Callable[[Optional[Value]], A],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        def run_f() -> A:
            data = self.fs.read_data(self.base_key + key)
            return f(data)

        if retry_strategy is None:
            retry_strategy = self.retry_strategy
        return self._lock(key, run_f, retry_strategy)

    # f is expected to return a potentially new value (to update key) plus the result of function
    def lock_and_transform(
        self,
        key: Key,
        f: Callable[[Optional[Value]], Tuple[Optional[Value], A]],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        def run_f() -> A:
            data = self.fs.read_data(self.base_key + key)
            (maybe_new_value, result) = f(data)
            if maybe_new_value is not None:
                self.fs.write_data(self.base_key + key, maybe_new_value)
            return result

        if retry_strategy is None:
            retry_strategy = self.retry_strategy
        return self._lock(key, run_f, retry_strategy)

    def get_descendant_store(self, descendant_key: Key) -> "UntypedStore":
        if len(descendant_key) <= 0:
            return self
        return UntypedStore(self.fs, self.base_key + descendant_key, self.retry_strategy)

    def get_ancestor_store(self, level: int = 1) -> "UntypedStore":
        if level <= 0:
            return self
        if level >= len(self.base_key):
            level = len(self.base_key)
        return UntypedStore(self.fs, self.base_key[:-level], self.retry_strategy)
