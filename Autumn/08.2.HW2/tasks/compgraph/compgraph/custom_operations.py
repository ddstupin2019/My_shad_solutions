import calendar
import math
import copy
import datetime

from compgraph.operations import Mapper, Reducer, TRow, TRowsGenerator, TRowsIterable

# Custom Mappers


class CalculateDistance(Mapper):
    """Calculates the distance between 2 points using the haversine distance"""

    def __init__(self, start: str, finish: str, column: str, R: float = 6373.0) -> None:
        """
        :param start: column start point
        :param finish: column finish point
        :param column: column for distance
        :param R: radius of the planet on which the point are given
        """
        self._start = start
        self._finish = finish
        self._column = column
        self._R = R

    def __call__(self, row: TRow) -> TRowsGenerator:
        start_lon, start_lat = row[self._start]
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

    def __init__(self, dist: str, time_second: str, column: str, const: float = 3.6) -> None:
        """
        :param dist: column distance in meters
        :param time_second: time in seconds
        :param column: column for spead
        :param const: const for expresion: const * lenght / time = km / h
        """
        self._dist = dist
        self._time_second = time_second
        self._column = column
        self._const = const

    def __call__(self, row: TRow) -> TRowsGenerator:
        result_row = copy.deepcopy(row)
        result_row[self._column] = row[self._dist] * \
            self._const / row[self._time_second]
        yield result_row


class CalculateTime(Mapper):
    """Calculates time using the %Y%m%dT%H%M%S pattern in uts"""

    def __init__(self, start: str, finish: str, column_weekday: str, column_hour: str, column_second: str) -> None:
        """
        :param start: start time
        :param finish: finish time
        :param column_weakdey: column time in weak day
        :param column_hour: column for time in hour
        :param column_second: column for time in second
        """
        self._start = start
        self._finish = finish
        self._column_weekday = column_weekday
        self._column_hour = column_hour
        self._column_second = column_second

    def __call__(self, row: TRow) -> TRowsGenerator:
        st_time = self._parse_time(row[self._start])
        f_time = self._parse_time(row[self._finish])
        result_row = copy.deepcopy(row)
        result_row[self._column_second] = (f_time - st_time).total_seconds()
        result_row[self._column_weekday] = calendar.day_abbr[st_time.weekday()]
        result_row[self._column_hour] = st_time.hour
        yield result_row

    def _parse_time(self, time: str) -> datetime.datetime:
        if time.find('.') == -1:
            time += '.0'
        main_part, fractional = time.split('.')
        dt = datetime.datetime.strptime(main_part, '%Y%m%dT%H%M%S')
        microseconds = int(float('0.' + fractional) * 1_000_000)
        dt = dt.replace(microsecond=microseconds)
        return dt

# custom Reducer


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
        :param column: name for mean column
        """
        self._column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result: TRow = {}
        result_count = 0
        for row in rows:
            if result == {}:
                result = {key: row[key] for key in group_key}
                result[self._column] = 0
            result[self._column] += row[self._column]
            result_count += 1
        result[self._column] /= result_count
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
