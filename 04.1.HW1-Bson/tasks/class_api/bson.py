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

class BsonInvalidArrayError(BsonBrokenDataError):
    pass

class MapperUnsupportedOptionError(ValueError):
    pass

python_only = False

def unmarshal(data: bytes) -> dict[str, Any]:
    return read_doc(data, 0)[0]


def read_cs_string(a: bytes, ind: int, is_key: bool) -> tuple[str, int]:
    d = 0
    while a[ind + d] != 0:
        d += 1
    try:
        return a[ind:ind + d].decode(encoding='utf-8'), ind + d + 1
    except Exception:
        if is_key:
            raise BsonBadKeyDataError
        else:
            raise BsonBadStringDataError


def read_string(a: bytes, ind: int) -> tuple[str, int]:
    dl, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 4
    if dl < 1:
        raise BsonStringSizeError
    global doc_size
    if ind + dl >= doc_size:
        raise BsonInconsistentStringSizeError
    try:
        r = a[ind:ind + dl - 1].decode(encoding='utf-8')
        if a[ind + dl - 1] != 0:
            raise BsonBrokenDataError
        return r, ind + dl
    except Exception:
        raise BsonBadStringDataError


def read(a: bytes, ind: int) -> tuple[dict[str, Any] | None, int]:
    num = a[ind]
    name, ind = read_cs_string(a, ind + 1, True)

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
            for i in dc.keys():
                if not i.isdigit() or len(i) == 0:
                    raise BsonBadArrayIndexError
                elif i[0] == '0' and len(i) > 1:
                    raise BsonBadArrayIndexError

            val = [None] * (max(map(int, dc.keys())) + 1)
            for i, j in dc.items():
                val[int(i)] = j
            if python_only and len(val) != len(dc.keys()):
                raise BsonInvalidArrayError

        else:
            val = []
    elif num == 5:
        col, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 5
        if 9 < a[ind - 1] < 128:
            raise BsonInvalidBinarySubtypeError
        elif a[ind - 1] != 0:
            if python_only:
                raise BsonInvalidBinarySubtypeError
            return None, ind + col
        val, ind = a[ind:ind + col], ind + col
    elif num == 6:
        return None, ind
    elif num == 7:
        val, ind = a[ind:ind + 12], ind + 12
        return None, ind
    elif num == 8:
        val, ind = bool(a[ind]), ind + 1
    elif num == 9:
        val, ind = struct.unpack('<q', a[ind:ind + 8])[0], ind + 8
        val = datetime.fromtimestamp(val / 1000.0, tz=timezone.utc)
    elif num == 10:
        val, ind = None, ind
    elif num == 11:
        _, ind = read_cs_string(a, ind, False)
        _, ind = read_cs_string(a, ind, False)
        return None, ind
    elif num == 12:
        _, ind = read_string(a, ind)
        return None, ind + 12
    elif num == 13:
        _, ind = read_string(a, ind)
        return None, ind
    elif num == 14:
        _, ind = read_string(a, ind)
        return None, ind
    elif num == 15:
        _, ind = read_string(a, ind)
        _, ind = read_doc(a, ind)
        return None, ind
    elif num == 16:
        val, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 4
    elif num == 17:
        return None, ind + 8
    elif num == 18:
        val, ind = struct.unpack('<q', a[ind:ind + 8])[0], ind + 8
    elif num == 19:
        return None, ind + 16
    elif num == 127 or num == 255:
        return None, ind
    else:
        raise BsonInvalidElementTypeError
    return {name: val}, ind


doc_size = 99999999999


def read_doc(a: bytes, ind: int = 0) -> tuple[dict[str, Any], int]:
    if len(a[ind:]) < 4:
        raise BsonBrokenDataError
    size, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 4
    r = {}
    if size == -1:
        raise BsonNotEnoughDataError
    if size < 4:
        raise BsonIncorrectSizeError
    if len(a[ind - 4:]) > size and ind == 4:
        raise BsonTooManyDataError
    elif len(a[ind - 4:]) < size:
        raise BsonNotEnoughDataError

    mx = ind - 4 + size
    global doc_size
    b_d = doc_size
    doc_size = mx
    while ind < mx - 1:
        q, ind = read(a, ind)
        if q is None:
            if python_only:
                raise BsonInvalidElementTypeError
            continue
        if list(q.keys())[0] in r.keys():
            raise BsonRepeatedKeyDataError
        r.update(q)
        if ind >= mx:
            raise BsonBrokenDataError
    doc_size = b_d

    if ind == mx - 1:
        if a[mx - 1] != 0:
            raise BsonBrokenDataError
        return r, ind + 1
    else:
        raise BsonTooManyDataError


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


class Mapper:
    def marshal(self, data: dict[str, Any]) -> bytes:
        return marshal(data)

    def unmarshal(self, data: bytes) -> dict[str, Any]:
        global python_only
        python_only = self.python_only
        return unmarshal(data)

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            if key == 'python_only' and type(val) is bool:
                self.__python_only = val
            else:
                raise MapperConfigError
        if 'python_only' not in kwargs.keys():
            self.__python_only = False

    @property
    def python_only(self) -> bool:
        return self.__python_only

    @python_only.setter
    def python_only(self, value):
        raise AttributeError("Cannot set attribute 'python_only'")


if __name__ == '__main__':
    context = {'math': math}
    run_calc(context)
