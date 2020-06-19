from abc import ABCMeta
from abc import abstractmethod
from dataclasses import dataclass
from typing import Callable
from typing import Generic
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar

from pytyped.macros.boxed import Boxed

from pyfsdb.typed import GetResult
from pyfsdb.typed import TypedStore
from pyfsdb.untyped import Key
from pyfsdb.untyped import RetryStrategy
from pyfsdb.untyped import Value

A = TypeVar("A")
G = TypeVar("G")
G1 = TypeVar("G1")
G2 = TypeVar("G2")
K = TypeVar("K")
K1 = TypeVar("K1")
K2 = TypeVar("K2")
T = TypeVar("T")
T1 = TypeVar("T1")
T2 = TypeVar("T2")


@dataclass
class CombinatorResult(Generic[G, T]):
    exact: G
    value: T


class CombinatorStore(Generic[K, T, G], metaclass=ABCMeta):
    @abstractmethod
    def get(self, key: K) -> Optional[CombinatorResult[G,T]]:
        pass

    # raises an exception if `key` is locked.
    @abstractmethod
    def put(self, key: K, value: T, retry_strategy: Optional[RetryStrategy] = None) -> None:
        pass

    # raises an exception if `key` is locked.
    # returns false if the value did not match the expected value
    # returns true if everything went well.
    @abstractmethod
    def compare_and_put(
        self,
        key: K,
        expected: Optional[G],
        new: T,
        retry_strategy: Optional[RetryStrategy] = None
    ) -> bool:
        pass

    # raises an exception if `key` is locked.
    @abstractmethod
    def lock_and_run(
        self,
        key: K,
        f: Callable[[Optional[CombinatorResult[G, T]]], A],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        pass

    @abstractmethod
    def lock_and_transform(
        self,
        key: K,
        f: Callable[[Optional[CombinatorResult[G, T]]], Tuple[Optional[Boxed[T]], A]],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        pass


@dataclass
class UnitCombinator(Generic[T], CombinatorStore[Key, T, Value]):
    underlying: TypedStore[T]

    def get(self, key: Key) -> Optional[CombinatorResult[Value, T]]:
        r = self.underlying.get(key)
        if r is None:
            return None
        return CombinatorResult(r.exact, r.value)

    def put(self, key: Key, value: T, retry_strategy: Optional[RetryStrategy] = None) -> None:
        self.underlying.put(key, value, retry_strategy)

    def compare_and_put(self, key: Key, expected: Optional[Value], new: T, retry_strategy: Optional[RetryStrategy] = None) -> bool:
        return self.underlying.compare_and_put(key, expected, new, retry_strategy)

    def lock_and_run(self, key: Key, f: Callable[[Optional[CombinatorResult[Value, T]]], A], retry_strategy: Optional[RetryStrategy] = None) -> A:
        def run_f(maybe_value: Optional[GetResult[T]]) -> A:
            if maybe_value is None:
                return f(None)
            return f(CombinatorResult(maybe_value.exact, maybe_value.value))

        return self.underlying.lock_and_run(key, run_f, retry_strategy)

    def lock_and_transform(
        self,
        key: Key,
        f: Callable[[Optional[CombinatorResult[Value, T]]], Tuple[Optional[Boxed[T]], A]],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        def run_f(maybe_value: Optional[GetResult[T]]) -> Tuple[Optional[Boxed[T]], A]:
            if maybe_value is None:
                return f(None)
            return f(CombinatorResult(maybe_value.exact, maybe_value.value))

        return self.underlying.lock_and_transform(key, run_f, retry_strategy)


@dataclass
class PairCombinator(CombinatorStore[Tuple[K1, K2], Tuple[T1, T2], Tuple[G1, G2]], Generic[K1, K2, T1, T2, G1, G2]):
    fst: CombinatorStore[K1, T1, G1]
    snd: CombinatorStore[K2, T2, G2]

    def get(self, key: Tuple[K1, K2]) -> Optional[CombinatorResult[Tuple[G1, G2], Tuple[T1, T2]]]:
        (key1, key2) = key
        r1 = self.fst.get(key1)
        r2 = self.snd.get(key2)
        if r1 is None or r2 is None:
            return None
        return CombinatorResult(
            exact=(r1.exact, r2.exact),
            value=(r1.value, r2.value)
        )

    def put(self, key: Tuple[K1, K2], value: Tuple[T1, T2], retry_strategy: Optional[RetryStrategy] = None) -> None:
        self.lock_and_transform(key, lambda x: (Boxed(value), None), retry_strategy)

    def compare_and_put(self, key: Tuple[K1, K2], expected: Optional[Tuple[G1, G2]], new: Tuple[T1, T2], retry_strategy: Optional[RetryStrategy] = None) -> bool:
        def cap(
            x: Optional[CombinatorResult[Tuple[G1, G2], Tuple[T1, T2]]]
        ) -> Tuple[Optional[Boxed[Tuple[T1, T2]]], bool]:
            if x is None and expected is None:
                return Boxed(new), True
            if x is None or expected is None:
                return None, False
            if x.exact == expected:
                return Boxed(new), True
            return None, False

        return self.lock_and_transform(key, cap, retry_strategy)

    def lock_and_run(
        self,
        key: Tuple[K1, K2],
        f: Callable[[Optional[CombinatorResult[Tuple[G1, G2], Tuple[T1, T2]]]], A],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        return self.lock_and_transform(key, lambda x: (None, f(x)), retry_strategy)

    def lock_and_transform(
        self,
        key: Tuple[K1, K2],
        f: Callable[
            [Optional[CombinatorResult[Tuple[G1, G2], Tuple[T1, T2]]]],
            Tuple[Optional[Boxed[Tuple[T1, T2]]], A]
        ],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        def run_f(
            r1: Optional[CombinatorResult[G1, T1]],
            r2: Optional[CombinatorResult[G2, T2]]
        ) -> Tuple[Optional[Boxed[T2]], Tuple[Optional[Boxed[T1]], A]]:
            updated: Optional[Boxed[Tuple[T1, T2]]]
            result: A
            if r1 is None:
                updated, result = f(None)
            elif r2 is None:
                updated, result = f(None)
            else:
                updated, result = f(CombinatorResult((r1.exact, r2.exact), (r1.value, r2.value)))
            if updated is None:
                return None, (None, result)
            new_t1, new_t2 = updated.t
            return Boxed(new_t2), (Boxed(new_t1), result)

        key1, key2 = key
        return self.fst.lock_and_transform(
            key1,
            lambda r1: self.snd.lock_and_transform(
                key2,
                lambda r2: run_f(r1, r2),
                retry_strategy
            ),
            retry_strategy
        )


@dataclass
class ListCombinatorLengthError(ValueError):
    message: str


@dataclass
class ListCombinator(CombinatorStore[List[K], List[T], List[G]], Generic[K, T, G]):
    element_store: CombinatorStore[K, T, G]

    def get(self, key: List[K]) -> Optional[CombinatorResult[List[G], List[T]]]:
        maybe_results = [self.element_store.get(k) for k in key]
        result: CombinatorResult[List[G], List[T]] = CombinatorResult([], [])
        for r in maybe_results:
            if r is None:
                return None
            result.exact.append(r.exact)
            result.value.append(r.value)
        return result

    def put(self, key: List[K], value: List[T], retry_strategy: Optional[RetryStrategy] = None) -> None:
        if len(key) != len(value):
            raise ListCombinatorLengthError("Different number of keys (%d) from values (%d)." % (len(key), len(value)))
        self.lock_and_transform(key, lambda x: (Boxed(value), None), retry_strategy)

    def compare_and_put(self, key: List[K], expected: Optional[List[G]], new: List[T], retry_strategy: Optional[RetryStrategy] = None) -> bool:
        def cap(x: Optional[CombinatorResult[List[G], List[T]]]) -> Tuple[Optional[Boxed[List[T]]], bool]:
            if x is None and expected is None:
                return Boxed(new), True
            if x is None or expected is None:
                return None, False
            if x.exact == expected:
                return Boxed(new), True
            return None, False

        if len(key) != len(new):
            raise ListCombinatorLengthError(
                "Different number of keys (%d) from new values (%d)." % (len(key), len(new))
            )
        if expected is not None and len(key) != len(expected):
            raise ListCombinatorLengthError(
                "Different number of keys (%d) from expected values (%d)." % (len(key), len(expected))
            )
        return self.lock_and_transform(key, cap, retry_strategy)

    def lock_and_run(self, key: List[K], f: Callable[[Optional[CombinatorResult[List[G], List[T]]]], A], retry_strategy: Optional[RetryStrategy] = None) -> A:
        return self.lock_and_transform(key, lambda x: (None, f(x)), retry_strategy)

    def lock_and_transform(
        self,
        key: List[K],
        f: Callable[
            [Optional[CombinatorResult[List[G], List[T]]]],
            Tuple[Optional[Boxed[List[T]]], A]
        ],
        retry_strategy: Optional[RetryStrategy] = None
    ) -> A:
        def run_f(
            index: int,
            prev_values: List[Optional[CombinatorResult[G, T]]],
            new_value: Optional[CombinatorResult[G, T]]
        ) -> Tuple[Optional[Boxed[T]], Tuple[List[Optional[Boxed[T]]], A]]:
            prev_values.append(new_value)
            if index < len(key):
                updates, result = self.element_store.lock_and_transform(
                    key=key[index],
                    f=lambda r: run_f(index + 1, prev_values, r),
                    retry_strategy=retry_strategy
                )
                return updates[index - 1], (updates, result)
            else:
                empty: bool = False
                values: CombinatorResult[List[G], List[T]] = CombinatorResult([], [])
                for maybe_value in prev_values:
                    if maybe_value is None:
                        empty = True
                        break
                    values.exact.append(maybe_value.exact)
                    values.value.append(maybe_value.value)

                maybe_updates: Optional[Boxed[List[T]]]
                res: A
                if empty:
                    maybe_updates, res = f(None)
                else:
                    maybe_updates, res = f(values)
                    if maybe_updates is not None and len(maybe_updates.t) != len(key):
                        raise ListCombinatorLengthError(
                            "Different number of updated values (%d) than number of keys (%d)." % (
                                len(maybe_updates.t), len(key)
                            )
                        )
                updated_results: List[Optional[Boxed[T]]]
                if maybe_updates is None:
                    updated_results = [None for i in range(len(key))]
                else:
                    updated_results = [Boxed(t) for t in maybe_updates.t]
                return updated_results[len(key) - 1], (updated_results, res)

        result: A
        if len(key) <= 0:
            _, result = f(CombinatorResult([], []))
        else:
            _, result = self.element_store.lock_and_transform(key[0], lambda r: run_f(1, [], r), retry_strategy)
        return result
