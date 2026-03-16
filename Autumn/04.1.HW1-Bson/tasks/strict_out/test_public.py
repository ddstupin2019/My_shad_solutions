# pyrefly: ignore-errors

import bson
import pytest
import random
import string
from typing import Any, Dict
from datetime import datetime, timezone


def test_marshal_dict_empty() -> None:
    inout_test(
        inp={},
        exp=bytes([5, 0, 0, 0, 0]),
    )


def test_marshal_dict_simple_string() -> None:
    inout_test(
        inp={"q": "abc"},
        exp=bytes([16, 0, 0, 0, 2, 113, 0, 4, 0, 0, 0, 97, 98, 99, 0, 0]),
    )


def test_marshal_dict_zero_string() -> None:
    inout_test(
        inp={"q": chr(0)},
        exp=bytes([14, 0, 0, 0, 2, 113, 0, 2, 0, 0, 0, 00, 0, 0]),
    )


def test_marshal_dict_zero_inside_string() -> None:
    inout_test(
        inp={"q": "a\x00c"},
        exp=bytes([16, 0, 0, 0, 2, 113, 0, 4, 0, 0, 0, 97, 0, 99, 0, 0]),
    )


def test_marshal_dict_single_float() -> None:
    inout_test(
        inp={"f": 123.45},
        exp=bytes([0x10, 0, 0, 0, 1, 0x66, 0, 0xCD, 0xCC, 0xCC, 0xCC, 0xCC, 0xDC, 0x5E, 0x40, 0]),
    )


def test_marshal_dict_single_float_empty_key() -> None:
    inout_test(inp={"": 0.0}, exp=b"\x0f\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")


def test_marshal_dict_single_float_cyrillic_infinity() -> None:
    inout_test(
        inp={"вася": float("inf")},
        exp=b"\x17\x00\x00\x00\x01\xd0\xb2\xd0\xb0\xd1\x81\xd1\x8f\x00\x00\x00\x00\x00\x00\x00\xf0\x7f\x00",
    )


def test_marshal_dict_many_floats() -> None:
    inout_test(
        inp={
            "вася": float("0.0"),
            "vasya": float("-0.0"),
            "12345": float("inf"),
            "": float("-inf"),
            "\t": float("nan"),
        },
        exp=b"J\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xf0\xff\x01\t\x00\x00\x00\x00\x00\x00\x00\xf8\x7f\x0112345\x00\x00\x00\x00\x00\x00\x00\xf0\x7f\x01vasya\x00\x00\x00\x00\x00\x00\x00\x00\x80\x01\xd0\xb2\xd0\xb0\xd1\x81\xd1\x8f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
    )


def test_marshal_bool() -> None:
    for v in [False, True]:
        inout_test(
            inp={
                "b": v,
            },
            exp=bytes([9, 0, 0, 0, 8, 98, 0, int(v), 0]),
        )


def test_marshal_bytes() -> None:
    inout_test(
        inp={
            "k": bytes([0, 255, 128]),
        },
        exp=bytes([16, 0, 0, 0, 5, 107, 0, 3, 0, 0, 0, 0, 0, 255, 128, 0]),
    )


def test_marshal_bytearray() -> None:
    inout_test(
        inp={
            "k": bytearray([0, 255, 128]),
        },
        exp=bytearray([16, 0, 0, 0, 5, 107, 0, 3, 0, 0, 0, 0, 0, 255, 128, 0]),
    )


def test_marshal_datetime_epoch() -> None:
    for d in (
        datetime(1970, 1, 1, tzinfo=timezone.utc),
        datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc),
        datetime(1970, 1, 1, 0, 0, 0, 999, tzinfo=timezone.utc),
    ):
        inout_test(
            inp={
                "k": d,
            },
            exp=bytes([16, 0, 0, 0, 9, 107, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        )


def test_marshal_datetime_millisecond() -> None:
    for ms in 1, 2, 10, 23:
        inout_test(
            inp={
                "k": datetime(1970, 1, 1, 0, 0, 0, ms * 1000, tzinfo=timezone.utc),
            },
            exp=bytes([16, 0, 0, 0, 9, 107, 0, ms, 0, 0, 0, 0, 0, 0, 0, 0]),
        )


def test_marshal_dict_inside_dict() -> None:
    inout_test(
        inp={
            "d1": {},
            "d2": {},
        },
        exp=bytes([23, 0, 0, 0, 3, 100, 49, 0, 5, 0, 0, 0, 0, 3, 100, 50, 0, 5, 0, 0, 0, 0, 0]),
    )


def test_marshal_dict_inside_second_level() -> None:
    inout_test(
        inp={
            "d1": {"d2": {}},
        },
        exp=bytes([23, 0, 0, 0, 3, 100, 49, 0, 14, 0, 0, 0, 3, 100, 50, 0, 5, 0, 0, 0, 0, 0, 0]),
    )


def test_marshal_array_empty() -> None:
    for ctr in list, tuple:
        inout_test(
            inp={
                "ea": ctr(),
            },
            exp=bytes([14, 0, 0, 0, 4, 101, 97, 0, 5, 0, 0, 0, 0, 0]),
        )


def test_marshal_array_single() -> None:
    for ctr in list, tuple:
        inout_test(
            inp={
                "sa": ctr([0x55]),
            },
            exp=bytes([21, 0, 0, 0, 4, 115, 97, 0, 12, 0, 0, 0, 16, 48, 0, 85, 0, 0, 0, 0, 0]),
        )


def test_marshal_array_many() -> None:
    for ctr in list, tuple:
        inout_test(
            inp={
                "ma": ctr(range(5, 16)),
            },
            exp=bytes(
                [
                    92,
                    0,
                    0,
                    0,
                    4,
                    109,
                    97,
                    0,
                    83,
                    0,
                    0,
                    0,
                    16,
                    48,
                    0,
                    5,
                    0,
                    0,
                    0,
                    16,
                    49,
                    0,
                    6,
                    0,
                    0,
                    0,
                    16,
                    50,
                    0,
                    7,
                    0,
                    0,
                    0,
                    16,
                    51,
                    0,
                    8,
                    0,
                    0,
                    0,
                    16,
                    52,
                    0,
                    9,
                    0,
                    0,
                    0,
                    16,
                    53,
                    0,
                    10,
                    0,
                    0,
                    0,
                    16,
                    54,
                    0,
                    11,
                    0,
                    0,
                    0,
                    16,
                    55,
                    0,
                    12,
                    0,
                    0,
                    0,
                    16,
                    56,
                    0,
                    13,
                    0,
                    0,
                    0,
                    16,
                    57,
                    0,
                    14,
                    0,
                    0,
                    0,
                    16,
                    49,
                    48,
                    0,
                    15,
                    0,
                    0,
                    0,
                    0,
                    0,
                ]
            ),
        )


def test_marshal_array_many_rev() -> None:
    for ctr in list, tuple:
        inout_test(
            inp={
                "ma": ctr(range(5, 16))[::-1],
            },
            exp=bytes(
                [
                    92,
                    0,
                    0,
                    0,
                    4,
                    109,
                    97,
                    0,
                    83,
                    0,
                    0,
                    0,
                    16,
                    48,
                    0,
                    15,
                    0,
                    0,
                    0,
                    16,
                    49,
                    0,
                    14,
                    0,
                    0,
                    0,
                    16,
                    50,
                    0,
                    13,
                    0,
                    0,
                    0,
                    16,
                    51,
                    0,
                    12,
                    0,
                    0,
                    0,
                    16,
                    52,
                    0,
                    11,
                    0,
                    0,
                    0,
                    16,
                    53,
                    0,
                    10,
                    0,
                    0,
                    0,
                    16,
                    54,
                    0,
                    9,
                    0,
                    0,
                    0,
                    16,
                    55,
                    0,
                    8,
                    0,
                    0,
                    0,
                    16,
                    56,
                    0,
                    7,
                    0,
                    0,
                    0,
                    16,
                    57,
                    0,
                    6,
                    0,
                    0,
                    0,
                    16,
                    49,
                    48,
                    0,
                    5,
                    0,
                    0,
                    0,
                    0,
                    0,
                ]
            ),
        )


def test_unmarshal_dict_empty() -> None:
    unmarshal_inout_test(
        exp={},
        inp=bytes([5, 0, 0, 0, 0]),
    )


def test_unmarshal_dict_simple_string() -> None:
    unmarshal_inout_test(
        exp={"q": "abc"},
        inp=bytes([16, 0, 0, 0, 2, 113, 0, 4, 0, 0, 0, 97, 98, 99, 0, 0]),
    )


def test_unmarshal_dict_zero_string() -> None:
    unmarshal_inout_test(
        exp={"q": chr(0)},
        inp=bytes([14, 0, 0, 0, 2, 113, 0, 2, 0, 0, 0, 00, 0, 0]),
    )


def test_unmarshal_dict_zero_inside_string() -> None:
    unmarshal_inout_test(
        exp={"q": "a\x00c"},
        inp=bytes([16, 0, 0, 0, 2, 113, 0, 4, 0, 0, 0, 97, 0, 99, 0, 0]),
    )


def test_unmarshal_dict_single_float() -> None:
    unmarshal_inout_test(
        exp={"f": 123.45},
        inp=bytes([0x10, 0, 0, 0, 1, 0x66, 0, 0xCD, 0xCC, 0xCC, 0xCC, 0xCC, 0xDC, 0x5E, 0x40, 0]),
    )


def test_unmarshal_dict_single_float_empty_key() -> None:
    unmarshal_inout_test(exp={"": 0.0}, inp=b"\x0f\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")


def test_unmarshal_dict_single_float_cyrillic_infinity() -> None:
    unmarshal_inout_test(
        exp={"вася": float("inf")},
        inp=b"\x17\x00\x00\x00\x01\xd0\xb2\xd0\xb0\xd1\x81\xd1\x8f\x00\x00\x00\x00\x00\x00\x00\xf0\x7f\x00",
    )


def test_unmarshal_dict_many_floats() -> None:
    unmarshal_inout_test(
        exp={
            "вася": float("0.0"),
            "vasya": float("-0.0"),
            "12345": float("inf"),
            "": float("-inf"),
            "\t": float("nan"),
        },
        inp=b"J\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xf0\xff\x01\t\x00\x00\x00\x00\x00\x00\x00\xf8\x7f\x0112345\x00\x00\x00\x00\x00\x00\x00\xf0\x7f\x01vasya\x00\x00\x00\x00\x00\x00\x00\x00\x80\x01\xd0\xb2\xd0\xb0\xd1\x81\xd1\x8f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        nan_keys=("\t",),
    )


def test_unmarshal_bool() -> None:
    for v in [False, True]:
        unmarshal_inout_test(
            exp={
                "b": v,
            },
            inp=bytes([9, 0, 0, 0, 8, 98, 0, int(v), 0]),
        )


def test_unmarshal_binary() -> None:
    unmarshal_inout_test(
        exp={
            "k": bytes([0, 255, 128]),
        },
        inp=bytes([16, 0, 0, 0, 5, 107, 0, 3, 0, 0, 0, 0, 0, 255, 128, 0]),
    )


def test_unmarshal_datetime_millisecond() -> None:
    for ms in 0, 1, 2, 10, 23:
        unmarshal_inout_test(
            exp={
                "k": datetime(1970, 1, 1, 0, 0, 0, ms * 1000, tzinfo=timezone.utc),
            },
            inp=bytes([16, 0, 0, 0, 9, 107, 0, ms, 0, 0, 0, 0, 0, 0, 0, 0]),
        )


def test_unmarshal_dict_inside_dict() -> None:
    unmarshal_inout_test(
        exp={
            "d1": {},
            "d2": {},
        },
        inp=bytes([23, 0, 0, 0, 3, 100, 49, 0, 5, 0, 0, 0, 0, 3, 100, 50, 0, 5, 0, 0, 0, 0, 0]),
    )


def test_unmarshal_dict_inside_second_level() -> None:
    unmarshal_inout_test(
        exp={
            "d1": {"d2": {}},
        },
        inp=bytes([23, 0, 0, 0, 3, 100, 49, 0, 14, 0, 0, 0, 3, 100, 50, 0, 5, 0, 0, 0, 0, 0, 0]),
    )


def test_unmarshal_array_empty() -> None:
    unmarshal_inout_test(
        exp={
            "ea": [],
        },
        inp=bytes([14, 0, 0, 0, 4, 101, 97, 0, 5, 0, 0, 0, 0, 0]),
    )


def test_unmarshal_array_single() -> None:
    unmarshal_inout_test(
        exp={
            "sa": [0x55],
        },
        inp=bytes([21, 0, 0, 0, 4, 115, 97, 0, 12, 0, 0, 0, 16, 48, 0, 85, 0, 0, 0, 0, 0]),
    )


def test_unmarshal_array_many() -> None:
    unmarshal_inout_test(
        exp={"ma": list(range(5, 16))},
        inp=bytes(
            [
                92,
                0,
                0,
                0,
                4,
                109,
                97,
                0,
                83,
                0,
                0,
                0,
                16,
                48,
                0,
                5,
                0,
                0,
                0,
                16,
                49,
                0,
                6,
                0,
                0,
                0,
                16,
                50,
                0,
                7,
                0,
                0,
                0,
                16,
                51,
                0,
                8,
                0,
                0,
                0,
                16,
                52,
                0,
                9,
                0,
                0,
                0,
                16,
                53,
                0,
                10,
                0,
                0,
                0,
                16,
                54,
                0,
                11,
                0,
                0,
                0,
                16,
                55,
                0,
                12,
                0,
                0,
                0,
                16,
                56,
                0,
                13,
                0,
                0,
                0,
                16,
                57,
                0,
                14,
                0,
                0,
                0,
                16,
                49,
                48,
                0,
                15,
                0,
                0,
                0,
                0,
                0,
            ]
        ),
    )


def test_unmarshal_array_many_rev() -> None:
    unmarshal_inout_test(
        exp={
            "ma": list(range(5, 16))[::-1],
        },
        inp=bytes(
            [
                92,
                0,
                0,
                0,
                4,
                109,
                97,
                0,
                83,
                0,
                0,
                0,
                16,
                48,
                0,
                15,
                0,
                0,
                0,
                16,
                49,
                0,
                14,
                0,
                0,
                0,
                16,
                50,
                0,
                13,
                0,
                0,
                0,
                16,
                51,
                0,
                12,
                0,
                0,
                0,
                16,
                52,
                0,
                11,
                0,
                0,
                0,
                16,
                53,
                0,
                10,
                0,
                0,
                0,
                16,
                54,
                0,
                9,
                0,
                0,
                0,
                16,
                55,
                0,
                8,
                0,
                0,
                0,
                16,
                56,
                0,
                7,
                0,
                0,
                0,
                16,
                57,
                0,
                6,
                0,
                0,
                0,
                16,
                49,
                48,
                0,
                5,
                0,
                0,
                0,
                0,
                0,
            ]
        ),
    )


def test_round_dict_bool() -> None:
    dicts = [
        {
            "prop1": True,
        },
        {
            "prop1": True,
            "prop2": False,
        },
        {"prop1": True, "prop2": None},
    ]
    for d in dicts:
        round_dict_test(d)


def round_dict_string() -> None:
    dicts = [
        {
            "prop1": "abc",
        },
        {
            "prop1": "\x00",
            "prop2": "a\x00",
        },
        {"prop1": "a\x00bc", "prop2": "ab\x00c"},
    ]
    for d in dicts:
        round_dict_test(d)


def test_round_dict_unicode() -> None:
    d = {"p" + str(i): "".join(map(chr, range(1, 2**i))) for i in range(16)}
    round_dict_test(d)


def test_round_dict_small_ints() -> None:
    d = {"p" + str(i): i for i in range(-1000, 1000)}
    round_dict_test(d)


def test_round_dict_around_pos_boundary() -> None:
    d = {"p" + str(i): (1 << 31) + i for i in range(-1000, 1000)}
    round_dict_test(d)


def test_round_dict_around_neg_boundary() -> None:
    d = {"p" + str(i): -(1 << 31) + i for i in range(-1000, 1000)}
    round_dict_test(d)


def test_round_dict_dict() -> None:
    dicts = [
        {
            "prop1": {},
        },
        {
            "prop1": {"k": {"k": "value"}},
            "prop2": False,
        },
        {"prop1": True, "prop2": {"p": None}},
    ]
    for d in dicts:
        round_dict_test(d)


def test_round_dict_list() -> None:
    dicts = [
        {
            "prop1": [],
        },
        {
            "prop1": [12, 34, 65],
            "prop2": [False],
        },
        {"prop1": True, "prop2": []},
    ]
    for d in dicts:
        round_dict_test(d)


def test_round_dict_list_multidim() -> None:
    dicts = [
        {
            "prop1": [],
        },
        {
            "prop1": [12, (34, 65)],
            "prop2": [False, [123, {"kkkkk": [[]]}]],
        },
        {"prop1": True, "prop2": []},
    ]
    for d in dicts:
        round_dict_test(d)


def test_round_dict_tuple() -> None:
    dicts = [
        {
            "prop1": [],
        },
        {
            "prop1": (12, 34, 65),
            "prop2": (False,),
        },
        {"prop1": True, "prop2": ()},
    ]

    for d in dicts:
        round_dict_test(d)


def test_round_dict_bytes() -> None:
    dicts = [
        {
            "prop1": bytes(),
        },
        {"prop1": bytes([12, 34, 65]), "prop2": bytes(1000), "prop3": 100 * bytes([123]), "prop4": bytes(range(256))},
    ]

    for d in dicts:
        round_dict_test(d)


def test_round_dict_bytearray() -> None:
    dicts = [
        {
            "prop1": bytearray(),
        },
        {
            "prop1": bytearray([12, 34, 65]),
            "prop2": bytearray(1000),
            "prop3": 100 * bytearray([123]),
            "prop4": bytearray(range(256)),
        },
    ]

    for d in dicts:
        round_dict_test(d)


def test_round_datetime_epoch() -> None:
    for d in (
        datetime(1970, 1, 1, tzinfo=timezone.utc),
        datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc),
        datetime(1970, 1, 1, 0, 0, 0, 999, tzinfo=timezone.utc),
    ):
        round_dict_test({"p1": d})


def test_round_list_long() -> None:
    random.seed(42)
    for _ in range(10):
        data = list(range(random.randint(0, 1000)))
        random.shuffle(data)
        round_dict_test({"p1": data})


def test_key_order() -> None:
    random.seed(42)
    base_chars = list(string.ascii_letters)
    random.shuffle(base_chars)
    chars = "".join(base_chars)
    keys = sorted({(chars + chars)[(r := random.randint(0, len(chars))) : r + i % len(chars)] for i in range(5, 100)})
    pairs = [(k, k[::-1]) for k in keys]
    round_dict_test(dict(pairs))
    blob = bson.marshal(dict(pairs))
    for i in range(50):
        sh_pairs = pairs[:]
        random.shuffle(sh_pairs)
        sh_blob = bson.marshal(dict(sh_pairs))
        assert blob == sh_blob
        d = bson.unmarshal(sh_blob)
        assert list(pairs) == list(d.items())


def test_marshal_dict_int32() -> None:
    inout_test(
        inp={
            "v1": 0x7FFFFFFF,
            "v2": -0x80000000,
            "s": "abc",
            "v": 0,
        },
        exp=bytes(
            [
                39,
                0,
                0,
                0,
                2,
                115,
                0,
                4,
                0,
                0,
                0,
                97,
                98,
                99,
                0,
                16,
                118,
                0,
                0,
                0,
                0,
                0,
                16,
                118,
                49,
                0,
                255,
                255,
                255,
                127,
                16,
                118,
                50,
                0,
                0,
                0,
                0,
                128,
                0,
            ]
        ),
    )


def test_marshal_dict_int64() -> None:
    inout_test(
        inp={
            "abc": 0x80000000,
        },
        exp=bytes([18, 0, 0, 0, 18, 97, 98, 99, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0]),
    )


def test_unmarshal_dict_int32() -> None:
    unmarshal_inout_test(
        exp={
            "v1": 0x7FFFFFFF,
            "v2": -0x80000000,
            "s": "abc",
            "v": 0,
        },
        inp=bytes(
            [
                39,
                0,
                0,
                0,
                2,
                115,
                0,
                4,
                0,
                0,
                0,
                97,
                98,
                99,
                0,
                16,
                118,
                0,
                0,
                0,
                0,
                0,
                16,
                118,
                49,
                0,
                255,
                255,
                255,
                127,
                16,
                118,
                50,
                0,
                0,
                0,
                0,
                128,
                0,
            ]
        ),
    )


def test_unmarshal_dict_int64() -> None:
    unmarshal_inout_test(
        exp={
            "abc": 0x80000000,
        },
        inp=bytes([18, 0, 0, 0, 18, 97, 98, 99, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0]),
    )


def test_exceptions() -> None:
    for exc_name in [
        "BsonError",
        "BsonMarshalError",
        "BsonUnsupportedObjectError",
        "BsonUnsupportedKeyError",
        "BsonKeyWithZeroByteError",
        "BsonInputTooBigError",
        "BsonBinaryTooBigError",
        "BsonIntegerTooBigError",
        "BsonStringTooBigError",
        "BsonDocumentTooBigError",
        "BsonCycleDetectedError",
        "BsonUnmarshalError",
        "BsonBrokenDataError",
        "BsonIncorrectSizeError",
        "BsonTooManyDataError",
        "BsonNotEnoughDataError",
        "BsonInvalidElementTypeError",
        "BsonInvalidStringError",
        "BsonStringSizeError",
        "BsonInconsistentStringSizeError",
        "BsonBadStringDataError",
        "BsonBadKeyDataError",
        "BsonRepeatedKeyDataError",
        "BsonInvalidBinarySubtypeError",
        "BsonBadArrayIndexError",
        "MapperConfigError",
    ]:
        assert hasattr(bson, exc_name)
        v = getattr(bson, exc_name)
        assert type(v) is type


def test_exceptions_inheritance() -> None:
    for sub, sup in [
        (bson.MapperConfigError, ValueError),
        (bson.BsonError, ValueError),
        (bson.BsonMarshalError, bson.BsonError),
        (bson.BsonUnsupportedObjectError, bson.BsonMarshalError),
        (bson.BsonUnsupportedKeyError, bson.BsonMarshalError),
        (bson.BsonKeyWithZeroByteError, bson.BsonUnsupportedKeyError),
        (bson.BsonInputTooBigError, bson.BsonMarshalError),
        (bson.BsonBinaryTooBigError, bson.BsonInputTooBigError),
        (bson.BsonIntegerTooBigError, bson.BsonInputTooBigError),
        (bson.BsonStringTooBigError, bson.BsonInputTooBigError),
        (bson.BsonDocumentTooBigError, bson.BsonInputTooBigError),
        (bson.BsonCycleDetectedError, bson.BsonMarshalError),
        (bson.BsonUnmarshalError, bson.BsonError),
        (bson.BsonBrokenDataError, bson.BsonUnmarshalError),
        (bson.BsonIncorrectSizeError, bson.BsonBrokenDataError),
        (bson.BsonTooManyDataError, bson.BsonBrokenDataError),
        (bson.BsonNotEnoughDataError, bson.BsonBrokenDataError),
        (bson.BsonInvalidElementTypeError, bson.BsonBrokenDataError),
        (bson.BsonInvalidStringError, bson.BsonBrokenDataError),
        (bson.BsonStringSizeError, bson.BsonBrokenDataError),
        (bson.BsonInconsistentStringSizeError, bson.BsonBrokenDataError),
        (bson.BsonBadStringDataError, bson.BsonBrokenDataError),
        (bson.BsonBadKeyDataError, bson.BsonBrokenDataError),
        (bson.BsonRepeatedKeyDataError, bson.BsonBrokenDataError),
        (bson.BsonInvalidBinarySubtypeError, bson.BsonBrokenDataError),
        (bson.BsonBadArrayIndexError, bson.BsonBrokenDataError),
    ]:
        print(sub, sup)
        assert sub.__bases__ == (sup,)


def test_marshal_unsupported() -> None:
    for unsupported in [
        object(),
        None,
        "",
        "hello",
        [],
    ]:
        print(unsupported)
        with pytest.raises(bson.BsonUnsupportedObjectError):
            bson.marshal(unsupported)


def test_marshal_unsupported_value() -> None:
    for unsupported in [
        object(),
        set(),
    ]:
        print(unsupported)
        with pytest.raises(bson.BsonUnsupportedObjectError):
            bson.marshal({"key": unsupported})


def test_marshal_unsupported_key() -> None:
    for unsupported in [
        object(),
        frozenset(),
        None,
    ]:
        print(unsupported)
        with pytest.raises(bson.BsonUnsupportedKeyError):
            bson.marshal({unsupported: "value"})


def test_marshal_unsupported_key_unsupported_value() -> None:
    for unsup_key in [
        object(),
        frozenset(),
        None,
    ]:
        for unsup_value in [
            object(),
            frozenset(),
        ]:
            with pytest.raises(bson.BsonUnsupportedKeyError):
                bson.marshal({unsup_key: unsup_value})
            with pytest.raises(bson.BsonUnsupportedKeyError):
                bson.marshal({"": unsup_value, unsup_key: ""})
            with pytest.raises(bson.BsonUnsupportedKeyError):
                bson.marshal({None: unsup_value, unsup_key: ""})


def test_marshal_key_with_zero() -> None:
    for unsupported in [
        chr(0),
        "\x00abc",
        "abc\x00",
        "hello\x00world",
    ]:
        print(unsupported)
        with pytest.raises(bson.BsonKeyWithZeroByteError):
            bson.marshal({unsupported: "value"})


def test_marshal_key_with_zero_unsupported_value() -> None:
    for unsup_key in [
        chr(0),
        "\x00abc",
        "abc\x00",
        "hello\x00world",
    ]:
        for unsup_value in [
            object(),
            frozenset(),
            None,
        ]:
            with pytest.raises(bson.BsonKeyWithZeroByteError):
                bson.marshal({unsup_key: unsup_value})
            with pytest.raises(bson.BsonKeyWithZeroByteError):
                bson.marshal({"": unsup_value, unsup_key: ""})


def test_marshal_unsupported_key_key_with_zero() -> None:
    for key1 in [
        object(),
        frozenset(),
        None,
    ]:
        for key2 in [
            chr(0),
            "\x00abc",
            "abc\x00",
            "hello\x00world",
        ]:
            with pytest.raises(bson.BsonUnsupportedKeyError):
                bson.marshal({key1: "", key2: ""})
            with pytest.raises(bson.BsonUnsupportedKeyError):
                bson.marshal({key2: "", key1: ""})


def test_marshal_cycle_simplest() -> None:
    data: Dict[str, Any] = {}
    data["k"] = data
    with pytest.raises(bson.BsonCycleDetectedError):
        bson.marshal(data)


def test_marshal_cycle_implicit() -> None:
    data1: Dict[str, Any] = {"k0": 123}
    data2: Dict[str, Any] = {"k1": data1, "k0": 234}
    data1["k2"] = data2
    with pytest.raises(bson.BsonCycleDetectedError):
        bson.marshal(data1)
    with pytest.raises(bson.BsonCycleDetectedError):
        bson.marshal(data2)


def test_marshal_cycle_simple_list() -> None:
    data = {"lst": ["", 123, None]}
    data["lst"].append(data["lst"])
    with pytest.raises(bson.BsonCycleDetectedError):
        bson.marshal(data)


def test_marshal_cycle_mixed() -> None:
    data = {"lst": ["", 123, None]}
    data["lst"].append((222, data))
    with pytest.raises(bson.BsonCycleDetectedError):
        bson.marshal(data)


def test_marshal_pseudo_cycle_list() -> None:
    data = {"k1": [123, "hello"]}
    data["k2"] = data["k1"]
    round_dict_test(data)


def test_marshal_pseudo_cycle_map() -> None:
    data = {"k1": [{"hhh": 123}]}
    data["k1"].extend(data["k1"])
    round_dict_test(data)


def inout_test(inp: Any, exp: Any, mapper: Any = None) -> None:
    if mapper is None:
        mapper = bson
    result = mapper.marshal(inp)
    print(repr(result), repr(exp))
    assert type(result) is bytes or type(result) is bytearray  # noqa: E721
    assert len(result) == len(exp)
    assert bytes(result) == exp


def check_types(inp: Any, exp: Any) -> None:
    for k in inp.keys():
        assert type(inp[k]) is type(exp[k])  # noqa: E721
        if type(inp[k]) is dict:  # noqa: E721
            check_types(inp[k], exp[k])


def unmarshal_inout_test(inp: Any, exp: Any, nan_keys: Any = (), mapper: Any = None) -> None:
    print("unmarshal_inout_test-1", inp, exp)
    if mapper is None:
        mapper = bson
    result = mapper.unmarshal(inp)
    print("result", result, type(result))
    assert type(result) is type(exp)
    assert len(result) == len(exp)
    for nk in nan_keys:
        assert nk in result
        assert result[nk] != result[nk]
        assert nk in exp
        assert exp[nk] != exp[nk]
        del result[nk]
        del exp[nk]
    assert result == exp
    check_types(result, exp)


def to_canonical(data: Any) -> Any:
    if type(data) in (list, tuple):
        return [to_canonical(e) for e in data]
    elif type(data) in (bytes, bytearray):
        return bytes(data)
    elif type(data) in (datetime,):
        return datetime(
            data.year,
            data.month,
            data.day,
            data.hour,
            data.minute,
            data.second,
            data.microsecond // 1000 * 1000,
            tzinfo=data.tzinfo,
        )
    elif type(data) is dict:  # noqa: E721
        return {k: to_canonical(e) for k, e in data.items()}
    return data


def round_dict_test(inp: Any, mapper: Any = None) -> None:
    if mapper is None:
        mapper = bson
    result = mapper.unmarshal(mapper.marshal(inp))
    assert type(result) is dict  # noqa: E721
    assert len(result) == len(inp)
    assert result == to_canonical(inp)


def round_dict_with_mapper_test(inp: Any) -> None:
    result = bson.unmarshal(bson.Mapper().marshal(inp))
    assert type(result) is dict  # noqa: E721
    assert len(result) == len(inp)
    assert result == to_canonical(inp)

    result = bson.Mapper().unmarshal(bson.marshal(inp))
    assert type(result) is dict  # noqa: E721
    assert len(result) == len(inp)
    assert result == to_canonical(inp)

    result = bson.Mapper().unmarshal(bson.Mapper().marshal(inp))
    assert type(result) is dict  # noqa: E721
    assert len(result) == len(inp)
    assert result == to_canonical(inp)
