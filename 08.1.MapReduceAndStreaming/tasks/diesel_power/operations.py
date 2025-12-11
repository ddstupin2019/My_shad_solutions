import copy
import string
from abc import abstractmethod, ABC
import typing as tp
import re
import heapq
from itertools import groupby

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self._filename = filename
        self._parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self._filename) as f:
            for line in f:
                yield self._parser(line)


class ReadIterFactory(Operation):
    def __init__(self, name: str) -> None:
        self._name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self._name]():
            yield row


# Operations


class Mapper(ABC):
    """Base class for mappers"""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self._mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self._mapper(row)


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self._reducer = reducer
        self._keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for _, rows in groupby(rows, key=lambda row: tuple(row[key] for key in self._keys)):
            yield from self._reducer(tuple(self._keys), rows)


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def _merge_row(self, keys: tp.Sequence[str], row_a: TRow, row_b: TRow) -> TRow:
        result_row = {}
        key_set = set(keys)
        for key, val in row_a.items():
            if key in row_b.keys() and key not in key_set:
                result_row[key + self._a_suffix] = val
            else:
                result_row[key] = val
        for key, val in row_b.items():
            if key in row_a.keys() and key not in key_set:
                result_row[key + self._b_suffix] = val
            else:
                result_row[key] = val
        return result_row


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self._keys = keys
        self._joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        rows_a = groupby(rows, key=lambda row: tuple(row[key] for key in self._keys))
        rows_b = groupby(args[0], key=lambda row: tuple(row[key] for key in self._keys))
        key_row_a = self._next_key_row(rows_a)
        key_row_b = self._next_key_row(rows_b)
        while True:
            if key_row_a[0] == key_row_b[0]:
                yield from self._joiner(self._keys, key_row_a[1], key_row_b[1])
                key_row_a = self._next_key_row(rows_a)
                key_row_b = self._next_key_row(rows_b)
            elif key_row_a[0] < key_row_b[0]:
                yield from self._joiner(self._keys, key_row_a[1], {})
                key_row_a = self._next_key_row(rows_a)
            else:
                yield from self._joiner(self._keys, {}, key_row_b[1])
                key_row_b = self._next_key_row(rows_b)
            if key_row_a is None:
                while key_row_b is not None:
                    yield from self._joiner(self._keys, {}, key_row_b[1])
                    key_row_b = self._next_key_row(rows_b)
                break
            if key_row_b is None:
                while key_row_a is not None:
                    yield from self._joiner(self._keys, key_row_a[1], {})
                    key_row_a = self._next_key_row(rows_a)
                break

    def _next_key_row(self, data):
        try:
            return next(data)
        except StopIteration:
            return None


# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield copy.deepcopy(row)


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield copy.deepcopy(row)
            break


# Mappers


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self._column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        result_row = copy.deepcopy(row)
        result_row[self._column] = row[self._column].translate(str.maketrans('', '', string.punctuation))
        yield result_row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self._column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        result_row = copy.deepcopy(row)
        result_row[self._column] = row[self._column].lower()
        yield result_row


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self._column = column
        self._separator = r'\s+' if separator is None else re.escape(separator)

    def __call__(self, row: TRow) -> TRowsGenerator:
        l_str = 0
        for r_str in re.finditer(self._separator, row[self._column]):
            yield self._create_row(row, l_str, r_str.start())
            l_str = r_str.end()
        yield self._create_row(row, l_str, len(row[self._column]))

    def _create_row(self, row: TRow, l_str: int, r_str: int) -> TRow:
        result_row = copy.deepcopy(row)
        result_row[self._column] = row[self._column][l_str:r_str]
        return result_row


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self._columns = columns
        self._result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        result_row = copy.deepcopy(row)
        result_row[self._result_column] = 1
        for col in self._columns:
            result_row[self._result_column] *= row[col]
        yield result_row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self._condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self._condition(row):
            yield copy.deepcopy(row)


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self._columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {col_name: copy.deepcopy(row[col_name]) for col_name in self._columns}


# Reducers


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self._column_max = column
        self._n = n

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for result_row in heapq.nlargest(self._n, rows, key=lambda row: row[self._column_max]):
            yield result_row


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self._words_column = words_column
        self._result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        row_dict: dict[tp.Any, TRow] = {}
        len_row_dict: dict[tp.Any, int] = {}
        len_rows = 0
        for row in rows:
            len_rows += 1
            if row[self._words_column] in len_row_dict:
                len_row_dict[row[self._words_column]] += 1
            else:
                len_row_dict[row[self._words_column]] = 1
                row_dict[row[self._words_column]] = {key: row[key] for key in group_key}

        for key, row in row_dict.items():
            result_row = row
            result_row[self._result_column] = len_row_dict[key] / len_rows
            result_row[self._words_column] = key
            yield result_row


class Count(Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self._column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        row_dict: dict[tuple[tp.Any], TRow] = {}
        col_row_dict: dict[tuple[tp.Any], int] = {}
        for row in rows:
            key = tuple(row[key] for key in group_key)
            if key in col_row_dict:
                col_row_dict[key] += 1
            else:
                col_row_dict[key] = 1
                row_dict[key] = {key: row[key] for key in group_key}

        for key in col_row_dict.keys():
            result_row = row_dict[key]
            result_row[self._column] = col_row_dict[key]
            yield result_row


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self._column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        row_dict: dict[tuple[tp.Any], TRow] = {}
        col_row_dict: dict[tuple[tp.Any], int] = {}
        for row in rows:
            key = tuple(row[key] for key in group_key)
            if key in col_row_dict:
                col_row_dict[key] += row[self._column]
            else:
                col_row_dict[key] = row[self._column]
                row_dict[key] = {key: row[key] for key in group_key}

        for key in col_row_dict.keys():
            result_row = row_dict[key]
            result_row[self._column] = col_row_dict[key]
            yield result_row


# Joiners


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b_tuple = tuple(rows_b)
        for row_a in rows_a:
            for row_b in rows_b_tuple:
                yield self._merge_row(keys, row_a, row_b)


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b_tuple = tuple(rows_b)
        if rows_b_tuple:
            a_is_empty = True
            for row_a in rows_a:
                a_is_empty = False
                for row_b in rows_b_tuple:
                    yield self._merge_row(keys, row_a, row_b)
            if a_is_empty:
                for row in rows_b_tuple:
                    yield row
        else:
            for row in rows_a:
                yield row


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b_tuple = tuple(rows_b)
        if rows_b_tuple:
            for row_a in rows_a:
                for row_b in rows_b_tuple:
                    yield self._merge_row(keys, row_a, row_b)
        else:
            for row in rows_a:
                yield row


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a_tuple = tuple(rows_a)
        if rows_a_tuple:
            for row_b in rows_b:
                for row_a in rows_a_tuple:
                    yield self._merge_row(keys, row_a, row_b)
        else:
            for row in rows_b:
                yield row
