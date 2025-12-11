import copy
import string
from abc import abstractmethod, ABC
import typing as tp
import re
import heapq
from itertools import groupby
import math
import datetime

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    """Abstract Operation"""

    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    """Read table for file"""

    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:

        self._filename = filename
        self._parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self._filename) as f:
            for line in f:
                yield self._parser(line)


class ReadIterFactory(Operation):
    """Read table for Iter"""

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
    """Map class"""

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
    """Base Reduce class"""

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
    """Base Join class"""

    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self._keys = keys
        self._joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        rows_a: groupby[tuple[tp.Any, ...], TRow] = groupby(rows, key=lambda row: tuple(
            row[key] for key in self._keys))
        rows_b: groupby[tuple[tp.Any, ...], TRow] = groupby(args[0], key=lambda row: tuple(
            row[key] for key in self._keys))
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
            if key_row_a == ((),{}):
                while key_row_b != ((),{}):
                    yield from self._joiner(self._keys, {}, key_row_b[1])
                    key_row_b = self._next_key_row(rows_b)
                break
            if key_row_b == ((),{}):
                while key_row_a != ((),{}):
                    yield from self._joiner(self._keys, key_row_a[1], {})
                    key_row_a = self._next_key_row(rows_a)
                break

    def _next_key_row(self, data):
        try:
            return next(data)
        except StopIteration:
            return ((),{})

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
        result_row[self._column] = row[self._column].translate(
            str.maketrans('', '', string.punctuation))
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


class CalculateDistance(Mapper):
    """Calculates the distance between 2 points using the haversine distance"""

    def __init__(self, start: str, finish: str, column: str, R: float = 6373.0) -> None:
        """
        :param start: column start point
        :param finish: column finish point
        :param column: column for distance
        :param R: radius of the planet on which the point are given
        """
        self._strat = start
        self._finish = finish
        self._column = column
        self._R = R

    def __call__(self, row: TRow) -> TRowsGenerator:
        start_lon, start_lat = row[self._strat]
        end_lon, end_lat = row[self._finish]

        lat1_rad = math.radians(start_lat)
        lon1_rad = math.radians(start_lon)
        lat2_rad = math.radians(end_lat)
        lon2_rad = math.radians(end_lon)
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * \
            math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance_m = self._R * c * 1000

        result_row = copy.deepcopy(row)
        result_row[self._column] = distance_m

        yield result_row


class CalculateSpeed(Mapper):
    """Calculates speed based on distance and time"""

    def __init__(self, dist: str, time_second: str, column: str) -> None:
        """
        :param dist: column distance in meters
        :param time_second: time  in seconds
        :param column: column for spead
        """
        self._dist = dist
        self._time_second = time_second
        self._column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        result_row = copy.deepcopy(row)
        result_row[self._column] = row[self._dist] * \
            3.6 / row[self._time_second]
        yield result_row


class CalculateTime(Mapper):
    """Calculates time using the %Y%m%dT%H%M%S pattern in uts"""

    weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    def __init__(self, start: str, finish: str, column_weekday: str, column_hour: str, column_second: str) -> None:
        """
        :param start: start time
        :param finish: finish time
        :param column_weakdey: column time in weak day
        :param column_hour: column for time in hour
        :param column_second: column for time in second
        """
        self._strat = start
        self._finish = finish
        self._column_weekday = column_weekday
        self._column_hour = column_hour
        self._column_second = column_second

    def __call__(self, row: TRow) -> TRowsGenerator:
        st_time = self._parse_time(row[self._strat])
        f_time = self._parse_time(row[self._finish])
        result_row = copy.deepcopy(row)
        result_row[self._column_second] = (f_time - st_time).total_seconds()
        result_row[self._column_weekday] = self.weekdays[st_time.weekday()]
        result_row[self._column_hour] = st_time.hour
        yield result_row

    def _parse_time(self, time: str) -> datetime.datetime:
        main_part, fractional = time.split('.')
        dt = datetime.datetime.strptime(main_part, '%Y%m%dT%H%M%S')
        microseconds = int(float('0.' + fractional) * 1_000_000)
        dt = dt.replace(microsecond=microseconds)
        return dt


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

class CountRow(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str) -> None:
        """
        :param column: column name to get top by
        """
        self._column_max = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        ln = 0
        for row in rows:
            ln += 1
        yield {self._column_max: ln}


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
            yield copy.deepcopy(result_row)


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
                row_dict[row[self._words_column]] = {
                    key: row[key] for key in group_key}

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
        fl: bool = True
        result: TRow = {}
        count: int = 0
        for row in rows:
            count += 1
            if fl:
                result = {key: row[key] for key in group_key}
                fl = False
        result[self._column] = count
        yield result


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
        fl: bool = True
        result: TRow = {}
        sum: int = 0
        for row in rows:
            sum += row[self._column]
            if fl:
                result = {key: row[key] for key in group_key}
                fl = False
        result[self._column] = sum
        yield result


class Mean(Reducer):
    """
    Mean values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 2.5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self._column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        fl: bool = True
        result: TRow = {}
        sum: int = 0
        count: int = 0
        for row in rows:
            sum += row[self._column]
            count += 1
            if fl:
                result = {key: row[key] for key in group_key}
                fl = False
        result[self._column] = sum / count
        yield result


class Tf_idf(Reducer):
    """
    tf_idf values aggregated by key
    """

    def __init__(self, freq: str, ln: str, col_word: str, column: str) -> None:
        """
        :param freq: column frequency of word in doc
        :param ln: column total number of docs
        :param col_word: colum docs where word is present
        :param column: name for tm_idf column
        """
        self._freq = freq
        self._ln = ln
        self._col_word = col_word
        self._column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            result_row = {key: row[key] for key in group_key}
            result_row[self._column] = row[self._freq] * \
                math.log(row[self._ln] / row[self._col_word])
            yield result_row


class Pmi(Reducer):
    """
    pmi values aggregated by key
    """

    def __init__(self, frec: str, freq_all: str, column: str) -> None:
        """
        :param freq: column frequency of word in doc
        :param freq_all: column frequency of word in all documents combined
        :param column: name for pmi column
        """
        self._frec = frec
        self._freq_all = freq_all
        self._column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            result_row = {key: row[key] for key in group_key}
            result_row[self._column] = math.log(
                row[self._frec] / row[self._freq_all])
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
