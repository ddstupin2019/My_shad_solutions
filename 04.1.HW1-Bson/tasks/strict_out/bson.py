import math
from typing import Any
import struct
from datetime import datetime, timezone

PROMPT = '>>> '


def run_calc(context: dict[str, Any] | None = None) -> None:
    """Run interactive calculator session in specified namespace"""
    pass

class MapperConfigError(ValueError):
    pass

class BsonError(ValueError):
    pass


class BsonMarshalError(BsonError):
    pass


class BsonUnsupportedObjectError(BsonMarshalError):
    pass


class BsonUnsupportedKeyError(BsonMarshalError):
    pass


class BsonKeyWithZeroByteError(BsonUnsupportedKeyError):
    pass


class BsonInputTooBigError(BsonMarshalError):
    pass


class BsonBinaryTooBigError(BsonInputTooBigError):
    pass


class BsonIntegerTooBigError(BsonInputTooBigError):
    pass


class BsonStringTooBigError(BsonInputTooBigError):
    pass


class BsonDocumentTooBigError(BsonInputTooBigError):
    pass


class BsonCycleDetectedError(BsonMarshalError):
    pass

class BsonUnmarshalError(BsonError):
    pass

class BsonBrokenDataError(BsonUnmarshalError):
    pass

class BsonIncorrentSizeError(BsonBrokenDataError):
    pass

class BsonTooManyDataError(BsonBrokenDataError):
    pass

class BsonNotEnoughDataError(BsonBrokenDataError):
    pass

class BsonInvalidElementTypeError(BsonBrokenDataError):
    pass

class BsonInvalidStringError(BsonBrokenDataError):
    pass

class BsonStringSizeError(BsonBrokenDataError):
    pass

class BsonInconsistentStringSizeError(BsonBrokenDataError):
    pass

class BsonBadStringDataError(BsonBrokenDataError):
    pass

class BsonBadKeyDataError(BsonBrokenDataError):
    pass

class BsonRepeatedKeyDataError(BsonBrokenDataError):
    pass

class BsonBadArrayIndexError(BsonBrokenDataError):
    pass

class BsonInvalidBinarySubtypeError(BsonBrokenDataError):
    pass

class BsonIncorrectSizeError(BsonBrokenDataError):
    pass

def unmarshal(data: bytes) -> dict[str, Any]:
    r, _ = read_doc(data, 0)
    return r


def read_cs_string(a: bytes, ind: int) -> tuple[str, int]:
    d = 0
    while a[ind + d] != 0:
        d += 1
    return a[ind:ind + d].decode(encoding='utf-8'), ind + d + 1


def read_string(a: bytes, ind: int) -> tuple[str, int]:
    dl, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 4
    return a[ind:ind + dl - 1].decode(encoding='utf-8'), ind + dl


def read(a: bytes, ind: int) -> tuple[Any, int]:
    num = a[ind]
    name, ind = read_cs_string(a, ind + 1)

    if num == 1:
        val = struct.unpack('<d', a[ind:ind + 8])[0]
        ind += 8
    elif num == 2:
        val, ind = read_string(a, ind)
    elif num == 3:
        val, ind = read_doc(a, ind)
    elif num == 4:
        dc, ind = read_doc(a, ind)
        if dc.keys():
            val = [0] * (max(map(int, dc.keys())) + 1)
            for i, j in dc.items():
                val[int(i)] = j
        else:
            val = []
    elif num == 5:
        col, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 5
        val, ind = a[ind:ind + col], ind + col
    elif num == 6:
        val, ind = None, ind + 1
    elif num == 7:
        val, ind = a[ind:ind + 12], ind + 12
    elif num == 8:
        val, ind = bool(a[ind]), ind + 1
    elif num == 9:
        val, ind = struct.unpack('<q', a[ind:ind + 8])[0], ind + 8
        val = datetime.fromtimestamp(val / 1000.0, tz=timezone.utc)
    elif num == 10:
        val, ind = None, ind
    elif num == 16:
        val, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 4
    elif num == 17:
        val, ind = struct.unpack('<Q', a[ind:ind + 8])[0], ind + 8
        val = datetime.fromtimestamp(val, timezone.utc)
    elif num == 18:
        val, ind = struct.unpack('<q', a[ind:ind + 8])[0], ind + 8
    else:
        assert 0 == 1
    return {name: val}, ind


def read_doc(a: bytes, ind: int = 0) -> tuple[Any, int]:
    _, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 4
    r = {}
    while a[ind].to_bytes() != b'\x00':
        q, ind = read(a, ind)
        r.update(q)
    return r, ind + 1

vis = []

def marshal(data: dict[str, Any]) -> bytes:
    if type(data) is dict:
        return to_document(data)
    else:
        raise BsonUnsupportedObjectError


def my_is_int32(n):
    from struct import error
    try:
        struct.pack("i", n)
    except error:
        try:
            struct.pack('q', n)
        except error:
            raise BsonIntegerTooBigError
        return False
    return True

def cor_document(a: dict[str, Any]) -> None:
    my_types = [float, int, bool, bytes, bytearray, datetime, dict, list, tuple, str, ]
    for key in a.keys():
        if type(key) is not str:
            raise BsonUnsupportedKeyError
    for key in a.keys():
        if b'\x00' in key.encode('utf-8'):
            raise BsonKeyWithZeroByteError
    for val in a.values():
        if not (type(val) in my_types or val is None):
            raise BsonUnsupportedObjectError

def to_document(a: dict[str, Any]) -> bytes:
    cor_document(a)
    r = b''
    if id(a) in vis:
        raise BsonCycleDetectedError
    else:
        vis.append(id(a))

    for name, val in sorted(a.items()):
        r += to_elem(name, val)
        if not my_is_int32(len(r)):
            raise BsonDocumentTooBigError

    vis.remove(id(a))
    return struct.pack('<i', (len(r) + 5)) + r + b'\x00'


def to_list(a: dict[str, Any]) -> bytes:
    r = b''
    for name, val in a.items():
        r += to_elem(name, val)
    return struct.pack('<i', (len(r) + 5)) + r + b'\x00'




def to_elem(name: str, a: Any) -> bytes:
    if type(a) is float:
        my_type = 1
        r = struct.pack('<d', a)
    elif type(a) is int:
        if my_is_int32(a):
            my_type = 16
            r = struct.pack('<i', a)
        else:
            my_type = 18
            r = struct.pack('<q', a)
    elif type(a) is bool:
        my_type = 8
        r = int(a).to_bytes()
    elif type(a) is datetime:
        my_type = 9
        r = struct.pack('<q', int(a.timestamp() * 1000))
    elif type(a) is bytearray or type(a) is bytes:
        my_type = 5
        if not my_is_int32(len(a)):
            raise BsonBinaryTooBigError
        r = struct.pack('<i', len(a)) + b'\x00' + bytes(a)
    elif type(a) is dict:
        my_type = 3
        r = to_document(a)
    elif type(a) is list or type(a) is tuple:
        if id(a) in vis:
            raise BsonCycleDetectedError
        else:
            vis.append(id(a))
        my_type = 4
        dc = {}
        for i in range(len(a)):
            dc[str(i)] = a[i]
        r = to_list(dc)
        vis.remove(id(a))
    elif type(a) is str:
        my_type = 2
        en_a = a.encode(encoding='utf-8')
        if not my_is_int32(len(en_a)):
            raise BsonStringTooBigError
        r = struct.pack('<i', len(en_a) + 1) + en_a + b'\x00'
    elif a is None:
        my_type = 10
        r = b''
    else:
        raise BsonUnsupportedObjectError

    return my_type.to_bytes() + name.encode() + b'\x00' + r


if __name__ == '__main__':
    context = {'math': math}
    run_calc(context)
