import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any

import pytest
from pytest import approx
from click.testing import CliRunner
from operator import itemgetter

from compgraph.cli import cli


def create_temp_file(data: List[Dict[str, Any]]) -> Path:
    """Helper to create temporary JSONL file"""
    temp_file = tempfile.NamedTemporaryFile(
        mode='w', suffix='.jsonl', delete=False)
    for item in data:
        temp_file.write(json.dumps(item) + '\n')
    temp_file.close()
    return Path(temp_file.name)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_word_count_cli(runner: CliRunner) -> None:
    """Test word count CLI command"""
    test_data = [
        {"doc_id": 1, "text": "hello world hello"},
        {"doc_id": 2, "text": "world test"},
    ]

    input_file = create_temp_file(test_data)
    output_file = Path(tempfile.mktemp(suffix='.jsonl'))

    try:
        result = runner.invoke(
            cli,
            ['word-count', str(input_file), str(output_file)]
        )

        assert result.exit_code == 0
        assert "Word count completed" in result.output

        # Check output file
        with open(output_file, 'r') as f:
            output = [json.loads(line) for line in f]

        expected = [
            {"text": "hello", "count": 2},
            {"text": "world", "count": 2},
            {"text": "test", "count": 1},
        ]

        # Sort for comparison
        output_sorted = sorted(output, key=lambda x: x['text'])
        expected_sorted = sorted(expected, key=lambda x: x['text'])

        assert output_sorted == expected_sorted

    finally:
        input_file.unlink(missing_ok=True)
        output_file.unlink(missing_ok=True)


def test_tf_idf_cli(runner: CliRunner) -> None:
    """Test inverted index CLI command"""
    test_data = [
        {'doc_id': 1, 'text': 'hello, little world'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? hello little world'},
        {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!!'}
    ]

    input_file = create_temp_file(test_data)
    output_file = Path(tempfile.mktemp(suffix='.jsonl'))

    try:
        result = runner.invoke(
            cli, ['inverted-index', str(input_file), str(output_file)])
        assert result.exit_code == 0

        # Check output file
        with open(output_file, 'r') as f:
            output = [json.loads(line) for line in f]

        expected = [
            {'doc_id': 1, 'text': 'hello', 'tf_idf': approx(0.1351, 0.001)},
            {'doc_id': 1, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},

            {'doc_id': 2, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},

            {'doc_id': 3, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},

            {'doc_id': 4, 'text': 'hello', 'tf_idf': approx(0.1013, 0.001)},
            {'doc_id': 4, 'text': 'little', 'tf_idf': approx(0.2027, 0.001)},

            {'doc_id': 5, 'text': 'hello', 'tf_idf': approx(0.2703, 0.001)},
            {'doc_id': 5, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},

            {'doc_id': 6, 'text': 'world', 'tf_idf': approx(0.3243, 0.001)}
        ]

        assert sorted(output, key=itemgetter('doc_id', 'text')) == expected

    finally:
        input_file.unlink(missing_ok=True)
        output_file.unlink(missing_ok=True)


def test_pmi_cli(runner: CliRunner) -> None:
    """Test inverted index CLI command"""
    test_data = [
        {'doc_id': 1, 'text': 'hello, little world'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? hello little world'},
        {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!! HELLO!!!!!!!'}
    ]

    input_file = create_temp_file(test_data)
    output_file = Path(tempfile.mktemp(suffix='.jsonl'))

    try:
        result = runner.invoke(cli, ['pmi', str(input_file), str(output_file)])
        assert result.exit_code == 0

        # Check output file
        with open(output_file, 'r') as f:
            output = [json.loads(line) for line in f]

        expected = [  # Mind the order !!!
            {'doc_id': 3, 'text': 'little', 'pmi': approx(0.9555, 0.001)},
            {'doc_id': 4, 'text': 'little', 'pmi': approx(0.9555, 0.001)},
            {'doc_id': 5, 'text': 'hello', 'pmi': approx(1.1786, 0.001)},
            {'doc_id': 6, 'text': 'world', 'pmi': approx(0.7731, 0.001)},
            {'doc_id': 6, 'text': 'hello', 'pmi': approx(0.0800, 0.001)},
        ]

        assert output == expected

    finally:
        input_file.unlink(missing_ok=True)
        output_file.unlink(missing_ok=True)


def test_yandex_maps_cli(runner: CliRunner) -> None:
    """Test inverted index CLI command"""
    lengths = [
        {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
         'edge_id': 8414926848168493057},
        {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824],
         'edge_id': 5342768494149337085},
        {'start': [37.56963176652789, 55.846845586784184], 'end': [37.57018438540399, 55.8469259692356],
         'edge_id': 5123042926973124604},
        {'start': [37.41463478654623, 55.654487907886505], 'end': [37.41442892700434, 55.654839486815035],
         'edge_id': 5726148664276615162},
        {'start': [37.584684155881405, 55.78285809606314], 'end': [37.58415022864938, 55.78177368734032],
         'edge_id': 451916977441439743},
        {'start': [37.736429711803794, 55.62696328852326], 'end': [37.736344216391444, 55.626937723718584],
         'edge_id': 7639557040160407543},
        {'start': [37.83196756616235, 55.76662947423756], 'end': [37.83191015012562, 55.766647034324706],
         'edge_id': 1293255682152955894},
    ]

    times = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171020T090548.939000', 'enter_time': '20171020T090547.463000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171024T144101.879000', 'enter_time': '20171024T144059.102000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171022T131828.330000', 'enter_time': '20171022T131820.842000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171014T134826.836000', 'enter_time': '20171014T134825.215000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171010T060609.897000', 'enter_time': '20171010T060608.344000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171027T082600.201000', 'enter_time': '20171027T082557.571000',
         'edge_id': 5342768494149337085}
    ]

    lengths_file = create_temp_file(lengths)
    times_file = create_temp_file(times)
    plot_file = Path(tempfile.mktemp(suffix='.png'))
    print(plot_file)
    output_file = Path(tempfile.mktemp(suffix='.jsonl'))

    try:
        result = runner.invoke(
            cli, ['yandex-maps', str(times_file), str(lengths_file), str(output_file), '--plot', '--plot-file', str(plot_file)])

        print(result.exception)
        print(result.exc_info)
        print(result.stderr_bytes)
        print(result.stdout_bytes)

        assert result.exit_code == 0

        # Check output file
        with open(output_file, 'r') as f:
            output = [json.loads(line) for line in f]

        expected = [
            {'weekday': 'Fri', 'hour': 8, 'speed': approx(62.2322, 0.001)},
            {'weekday': 'Fri', 'hour': 9, 'speed': approx(78.1070, 0.001)},
            {'weekday': 'Fri', 'hour': 11, 'speed': approx(88.9552, 0.001)},
            {'weekday': 'Sat', 'hour': 13, 'speed': approx(100.9690, 0.001)},
            {'weekday': 'Sun', 'hour': 13, 'speed': approx(21.8577, 0.001)},
            {'weekday': 'Tue', 'hour': 6, 'speed': approx(105.3901, 0.001)},
            {'weekday': 'Tue', 'hour': 14, 'speed': approx(41.5145, 0.001)},
            {'weekday': 'Wed', 'hour': 14, 'speed': approx(106.4505, 0.001)}
        ]

        assert sorted(output, key=itemgetter('weekday', 'hour')) == expected

    finally:
        lengths_file.unlink(missing_ok=True)
        times_file.unlink(missing_ok=True)
        output_file.unlink(missing_ok=True)
