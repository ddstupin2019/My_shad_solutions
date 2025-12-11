import copy
import dataclasses
import typing as tp

import pytest
from pytest import approx
from compgraph import operations as ops


class _Key:
    def __init__(self, *args: str) -> None:
        self._items = args

    def __call__(self, d: tp.Mapping[str, tp.Any]) -> tuple[str, ...]:
        return tuple(str(d.get(key)) for key in self._items)


@dataclasses.dataclass
class MapCase:
    mapper: ops.Mapper
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]
    mapper_item: int = 0
    mapper_ground_truth_items: tuple[int, ...] = (0,)


MAP_CASES = [
    MapCase(
        mapper=ops.CalculateDistance('start', 'end', 'dist'),
        data=[
            {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
                'test_id': 1},
            {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824],
                'test_id': 2},
        ],
        ground_truth=[
            {'test_id': 1, 'start': [37.84870228730142, 55.73853974696249], 'end': [
                37.8490418381989, 55.73832445777953], 'dist': approx(32.023, 0.01)},
            {'test_id': 2, 'start': [37.524768467992544, 55.88785375468433], 'end': [
                37.52415172755718, 55.88807155843824], 'dist': approx(45.464, 0.01)},
        ],
        cmp_keys=('test_id', 'dict')
    ),
    MapCase(
        mapper=ops.CalculateTime(
            'enter_time', 'leave_time', 'week', 'hour', 'sec'),
        data=[
            {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
                'test_id': 1},
            {'leave_time': '20171022T131828.330000', 'enter_time': '20171022T131820.842000',
                'test_id': 2},

        ],
        ground_truth=[
            {'test_id': 1, 'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
                'week': 'Fri', 'hour': 11, 'sec': approx(1.296, 0.01)},
            {'test_id': 2, 'leave_time': '20171022T131828.330000', 'enter_time': '20171022T131820.842000',
                'week': 'Sun', 'hour': 13, 'sec': approx(7.488, 0.01)},
        ],
        cmp_keys=('test_id', 'week', 'hour', 'sec')
    ),
    MapCase(
        mapper=ops.CalculateSpeed('dist', 'time', 'speed'),
        data=[
            {'test_id': 1, 'time': 1.296, 'dist': 32.023},
            {'test_id': 2, 'time': 7.488, 'dist': 45.464},
        ],
        ground_truth=[
            {'test_id': 1, 'time': 1.296, 'dist': 32.023,
                'speed': approx(88.955, 0.01)},
            {'test_id': 2, 'time': 7.488, 'dist': 45.464,
                'speed': approx(21.858, 0.01)},
        ],
        cmp_keys=('test_id', 'speed')
    ),


]


@pytest.mark.parametrize('case', MAP_CASES)
def test_mapper(case: MapCase) -> None:
    mapper_data_row = copy.deepcopy(case.data[case.mapper_item])
    mapper_ground_truth_rows = [copy.deepcopy(
        case.ground_truth[i]) for i in case.mapper_ground_truth_items]

    key_func = _Key(*case.cmp_keys)

    mapper_result = case.mapper(mapper_data_row)
    assert isinstance(mapper_result, tp.Iterator)
    assert sorted(mapper_result, key=key_func) == sorted(
        mapper_ground_truth_rows, key=key_func)

    result = ops.Map(case.mapper)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    assert sorted(result, key=key_func) == sorted(
        case.ground_truth, key=key_func)


@dataclasses.dataclass
class ReduceCase:
    reducer: ops.Reducer
    reducer_keys: tuple[str, ...]
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]
    reduce_data_items: tuple[int, ...] = (0,)
    reduce_ground_truth_items: tuple[int, ...] = (0,)


REDUCE_CASES = [
    ReduceCase(
        reducer=ops.CountRow('rez'),
        reducer_keys=(()),
        data=[
            {'match_id': 1, 'player_id': 1, 'rank': 42},
            {'match_id': 1, 'player_id': 2, 'rank': 7},
            {'match_id': 1, 'player_id': 3, 'rank': 0},
            {'match_id': 1, 'player_id': 4, 'rank': 39},

            {'match_id': 2, 'player_id': 5, 'rank': 15},
            {'match_id': 2, 'player_id': 6, 'rank': 39},
            {'match_id': 2, 'player_id': 7, 'rank': 27},
            {'match_id': 2, 'player_id': 8, 'rank': 7}
        ],
        ground_truth=[
            {'rez': 8}
        ],
        cmp_keys=(tuple('rez')),
        reduce_data_items=(0, 1, 2, 3, 4, 5, 6, 7),
        reduce_ground_truth_items=(0,)
    ),
    ReduceCase(
        reducer=ops.Mean(column='score'),
        reducer_keys=('match_id',),
        data=[
            {'match_id': 1, 'player_id': 1, 'score': 42},
            {'match_id': 1, 'player_id': 2, 'score': 7},
        ],
        ground_truth=[
            {'match_id': 1, 'score': approx(24.5, 0.01)},
        ],
        cmp_keys=('match_id', 'score'),
        reduce_data_items=(0, 1),
        reduce_ground_truth_items=(0,)
    ),
    ReduceCase(
        reducer=ops.Tf_idf('f', 'l', 'c', 'score'),
        reducer_keys=('match_id',),
        data=[
            {'match_id': 1, 'f': 0.43, 'c': 4, 'l': 5},
            {'match_id': 2, 'f': 0.1, 'c': 10, 'l': 20},
        ],
        ground_truth=[
            {'match_id': 1, 'score': approx(0.0959517270651102, 0.0001)},
            {'match_id': 2, 'score': approx(0.06931471805599453, 0.01)}
        ],
        cmp_keys=('match_id', 'score'),
        reduce_data_items=(0, 1),
        reduce_ground_truth_items=(0, 1)
    ),
    ReduceCase(
        reducer=ops.Pmi('f', 'a', 'score'),
        reducer_keys=('match_id',),
        data=[
            {'match_id': 1, 'f': 0.3, 'a': 0.5},

            {'match_id': 2, 'f': 0.3, 'a': 0.2},
        ],
        ground_truth=[
            {'match_id': 1, 'score': approx(-0.5108256237659907, 0.0001)},

            {'match_id': 2, 'score': approx(0.4054651081081642, 0.00001)}
        ],
        cmp_keys=('match_id', 'score'),
        reduce_data_items=(0, 1),
        reduce_ground_truth_items=(0, 1)
    ),
]


@pytest.mark.parametrize('case', REDUCE_CASES)
def test_reducer(case: ReduceCase) -> None:
    reducer_data_rows = [copy.deepcopy(case.data[i])
                         for i in case.reduce_data_items]
    reducer_ground_truth_rows = [copy.deepcopy(
        case.ground_truth[i]) for i in case.reduce_ground_truth_items]

    key_func = _Key(*case.cmp_keys)

    reducer_result = case.reducer(case.reducer_keys, iter(reducer_data_rows))
    assert isinstance(reducer_result, tp.Iterator)
    assert sorted(reducer_result, key=key_func) == sorted(
        reducer_ground_truth_rows, key=key_func)

    result = ops.Reduce(case.reducer, case.reducer_keys)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    assert sorted(result, key=key_func) == sorted(
        case.ground_truth, key=key_func)
