import math
from typing import Any
import struct
from datetime import datetime, timezone
from dataclasses import is_dataclass
from collections import namedtuple as np

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
keep_types1 = False
nt_types = {}
nt_dict = {}


def unmarshal(data: bytes, python_onl=False, keep_types=False) -> dict[str, Any]:
    global python_only, keep_types1, nt_types, nt_dict
    python_only = python_onl
    keep_types1 = keep_types
    nt_types = {}
    nt_dict = {}
    tmp = read_doc(data, 0, False)[0]

    if keep_types1:
        create_nt_par()
        tmp = parse_dict_nt(tmp, True)
    return tmp


def parse_nt(a: Any, key: str):
    return nt_par[key](*a.values())


nt_par = {}


def create_nt_par():
    if 'types' not in nt_types:
        return

    global nt_par
    nt_par = {}

    for key, val in nt_types['types'].items():
        nt_par[key] = np(val['name'], val['fields'], defaults=val['defaults'].values())


def parse_dict_nt(a: Any, start=False) -> Any:
    r = {}
    for i, j in a.items():
        r[i] = j
        if type(j) in [tuple, list]:
            r[i] = parse_list_nt(j)
        elif type(j) is dict:
            r[i] = parse_dict_nt(j)
        if id(j) in nt_dict:
            r[i] = parse_nt(r[i], nt_dict[id(j)])
            del nt_dict[id(j)]

    if 'self' in nt_types and start:
        r = parse_nt(r, nt_types['self'])
    return r


def parse_list_nt(a: Any) -> Any:
    r = [Any] * len(a)
    for i in range(len(a)):
        j = a[i]
        r[i] = j
        if type(j) in [tuple, list]:
            r[i] = parse_list_nt(j)
        elif type(j) is dict:
            r[i] = parse_dict_nt(j)
        if id(j) in nt_dict:
            r[i] = parse_nt(r[i], nt_dict[id(j)])
            del nt_dict[id(j)]

    if type(a) is tuple:
        r = tuple(r)
    return r


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
    val = ''

    if num == 1:
        val = struct.unpack('<d', a[ind:ind + 8])[0]
        ind += 8
    elif num == 2:
        val, ind = read_string(a, ind)
    elif num == 3:
        val, ind = read_doc(a, ind, False)

    elif num == 4:
        dc, ind = read_doc(a, ind, True)
        if dc.keys():
            keep = bytearray(b'')
            nm = '__metadata__'
            if nm in dc.keys() and keep_types1:
                if type(dc[nm]) is bytearray:
                    keep = dc[nm]
                    del dc[nm]
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
            if keep_types1:
                val = parse_keep_list(val, keep)
        else:
            val = []
    elif num == 5:
        col, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 5
        if keep_types1 and a[ind - 1] == 128:
            val, ind = bytearray(a[ind:ind + col]), ind + col
        elif a[ind - 1] == 0:
            val, ind = bytes(a[ind:ind + col]), ind + col
        elif 9 < a[ind - 1] < 128:
            raise BsonInvalidBinarySubtypeError
        elif a[ind - 1] != 0:
            if python_only:
                raise BsonInvalidBinarySubtypeError
            return None, ind + col
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
        _, ind = read_doc(a, ind, False)
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


def parse_metadata(a: bytearray) -> str:
    if a[0] == 0:
        global nt_types
        nt_types, _ = read_doc(bytes(a), 1)
        if 'children' in nt_types.keys():
            return nt_types['children']
        else:
            return ''
    else:
        return a.decode('utf-8')


def parse_keep_list(a: list[Any], keep_inp: bytearray) -> list[Any]:
    if keep_inp == bytearray(b''):
        return a
    keep = parse_metadata(keep_inp).split(':')
    for i in range(len(a)):
        if type(a[i]) is list and keep[i] != '':
            a[i] = tuple(a[i])
        elif type(a[i]) is bytes and keep[i] != '':
            a[i] = bytearray(a[i])
        elif type(a[i]) is dict and keep[i] != '':
            nt_dict[id(a[i])] = keep[i]
    return a


def parse_keep_dict(a: list[tuple[str, Any]]) -> dict[str, Any]:
    nm = '__metadata__'
    keep = []
    for key in a:
        if key[0] == nm and type(key[1]) is bytearray:
            keep = parse_metadata(key[1]).split(':')
            a.remove(key)
            break
    r = {}
    ind = 0
    for key, val in a:
        if type(val) is list and ind < len(keep):
            if keep[ind] == '':
                r[key] = val
            else:
                r[key] = tuple(val)
            ind += 1
        elif type(val) is bytes and ind < len(keep):
            if keep[ind] == '':
                r[key] = val
            else:
                r[key] = bytearray(val)
            ind += 1
        elif type(val) is dict and ind < len(keep):
            if keep[ind] != '':
                nt_dict[id(val)] = keep[ind]
                ind += 1
            r[key] = val
        else:
            r[key] = val
    return r


def read_doc(a: bytes, ind: int = 0, is_list: bool = False) -> tuple[dict[str, Any], int]:
    if len(a[ind:]) < 4:
        raise BsonBrokenDataError
    size, ind = struct.unpack('<i', a[ind:ind + 4])[0], ind + 4
    r = {}
    r1 = []
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
        if list(q.keys())[0] == '__metadata__' and type(list(q.values())[0]) is bytearray and not is_list:
            if keep_types1:
                r1.append((list(q.keys())[0], list(q.values())[0]))
        else:
            if list(q.keys())[0] in r.keys():
                raise BsonRepeatedKeyDataError
            r.update(q)
            if keep_types1:
                r1.append((list(q.keys())[0], list(q.values())[0]))

        if ind >= mx:
            raise BsonBrokenDataError
    doc_size = b_d

    if ind == mx - 1:
        if a[mx - 1] != 0:
            raise BsonBrokenDataError

        if keep_types1 and not is_list:
            r = parse_keep_dict(r1)

        return r, ind + 1
    else:
        raise BsonTooManyDataError


vis = []
nt_mas = {}
c_mt = 0


def marshal(data: dict[str, Any], kepp_types=False) -> bytes:
    global vis, keep_types1, nt_mas, c_mt
    keep_types1 = kepp_types
    vis = []
    nt_mas = {}
    c_mt = 0

    if is_valid_dict(data):
        if is_nt(data):
            return to_nt(data)
        elif is_dt(data):
            return to_dt(data)
        elif is_prop(data):
            return to_prop(data)
        else:
            tmp = to_document(data)
            return tmp
    else:
        raise BsonUnsupportedObjectError


def is_valid_dict(a: Any) -> bool:
    if type(a) is dict:
        return True
    return is_nt(a) or is_dt(a) or is_prop(a)


def is_nt(a: Any) -> bool:
    return (
            isinstance(a, tuple) and
            hasattr(a, '_fields') and
            hasattr(a, '_asdict') and
            hasattr(a, '_replace')
    )


def is_dt(a: Any) -> bool:
    return is_dataclass(a)


def is_prop(a: Any) -> bool:
    return len(find_p(a)) > 0


def is_valid_type(a: Any) -> bool:
    my_types = [float, int, bool, bytes, bytearray, datetime, dict, list, tuple, str]
    if type(a) in my_types or a is None:
        return True
    return is_valid_dict(a)


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
    for key in a.keys():
        if type(key) is not str:
            raise BsonUnsupportedKeyError
    for key in a.keys():
        if b'\x00' in key.encode('utf-8'):
            raise BsonKeyWithZeroByteError
    for val in a.values():
        if not is_valid_type(val):
            raise BsonUnsupportedObjectError


def to_keep(a: Any) -> str | None:
    if type(a) is tuple:
        return 'tuple'
    elif type(a) is bytearray:
        return 'bytearray'
    elif type(a) in [list, bytes]:
        return ''
    elif is_nt(a) and keep_types1:
        r, _ = nt_mas[a.__class__]
        return r
    else:
        return None


def from_metadata_dt(a: list[str] | None, is_self=False) -> bytes:
    tmp_res = {}
    my_type = 5
    my_type_2 = 128

    for j, k in nt_mas.values():
        tmp_res[j] = {'fields': list(k._fields), 'name': k.__class__.__name__,
                      'defaults': k._field_defaults}

    tmp_r_2 : dict[str,Any]
    if a is None:
        tmp_r_2 = {'types': tmp_res}
    else:
        tmp_r_2 = {'types': tmp_res, 'children': ':'.join(a)}

    if is_self:
        tmp_r_2['self'] = 'nt-0'

    r = to_elem('', tmp_r_2)[1:]

    return (my_type.to_bytes() + '__metadata__'.encode('utf-8') + b'\x00' +
            struct.pack('<i', len(r)) + my_type_2.to_bytes() + r)


def form_keep(a: list[str], is_self=False) -> bytes:
    my_type = 5
    my_type_2 = 128
    r = ':'.join(a).encode('utf_8')

    if len(vis) == 1 and len(nt_mas) > 0:
        return from_metadata_dt(a, is_self)

    if len(r) == 0:
        return b''

    return (my_type.to_bytes() + '__metadata__'.encode('utf-8') + b'\x00' +
            struct.pack('<i', len(r)) + my_type_2.to_bytes() + r)


def find_p(obj: Any) -> list[tuple[str, Any]]:
    try:
        r = []
        for attr_name in dir(type(obj)):
            attr = getattr(type(obj), attr_name)
            if isinstance(attr, property):
                try:
                    value = getattr(obj, attr_name)
                    r.append((attr_name, value))
                except ValueError:
                    pass
        return r
    except TypeError:
        return []


def to_prop(a: Any) -> bytes:
    if id(a) in vis:
        raise BsonCycleDetectedError
    else:
        vis.append(id(a))
    tmp = to_document(dict(find_p(a)))
    vis.remove(id(a))
    return tmp


def to_dt(a: Any) -> bytes:
    a = a.__dict__
    cor_document(a)
    r = b''
    if id(a) in vis:
        raise BsonCycleDetectedError
    else:
        vis.append(id(a))

    keep = []
    for name, val in a.items():
        r += to_elem(name, val)
        if not my_is_int32(len(r)):
            raise BsonDocumentTooBigError
        tmp = to_keep(val)
        if tmp is not None:
            keep.append(tmp)

    if keep_types1 and len(keep) > 0:
        r += form_keep(keep)
    elif keep_types1 and len(nt_mas) > 0 and len(vis) == 1:
        r += from_metadata_dt(None)

    vis.remove(id(a))
    return struct.pack('<i', (len(r) + 5)) + r + b'\x00'


def to_document(a: dict[str, Any]) -> bytes:
    cor_document(a)
    r = b''
    if id(a) in vis:
        raise BsonCycleDetectedError
    else:
        vis.append(id(a))

    keep = []
    for name, val in sorted(a.items()):
        r += to_elem(name, val)
        if not my_is_int32(len(r)):
            raise BsonDocumentTooBigError

        tmp = to_keep(val)
        if tmp is not None:
            keep.append(tmp)

    if keep_types1 and len(keep) > 0:
        r += form_keep(keep)
    elif keep_types1 and len(nt_mas) > 0 and len(vis) == 1:
        r += from_metadata_dt(None)

    vis.remove(id(a))
    return struct.pack('<i', (len(r) + 5)) + r + b'\x00'


def to_nt(a: Any) -> bytes:
    cor_document(dict(zip(a._fields, a)))

    if a.__class__ not in nt_mas.keys():
        global c_mt
        nt_mas[a.__class__] = f'nt-{c_mt}', a
        c_mt += 1

    r = b''
    if id(a) in vis:
        raise BsonCycleDetectedError
    else:
        vis.append(id(a))

    keep = []
    for name, val in zip(a._fields, a):
        r += to_elem(name, val)
        if not my_is_int32(len(r)):
            raise BsonDocumentTooBigError

        tmp = to_keep(val)
        if tmp is not None:
            keep.append(tmp)

    if keep_types1 and len(keep) > 0:
        r += form_keep(keep, True)
    elif keep_types1 and len(nt_mas) > 0 and len(vis) == 1:
        r += from_metadata_dt(None, True)

    vis.remove(id(a))
    return struct.pack('<i', (len(r) + 5)) + r + b'\x00'


def to_list(a: list[Any] | tuple[Any]) -> bytes:
    r = b''
    keep = []
    for i in range(len(a)):
        name, val = str(i), a[i]
        r += to_elem(name, val)

        tmp = to_keep(val)
        if tmp is None:
            tmp = ''
        keep.append(tmp)

    if keep_types1 and len(keep) > 0:
        r += form_keep(keep)
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
        r = to_list(a)
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
        if is_nt(a):
            my_type = 3
            r = to_nt(a)
        elif is_dt(a):
            my_type = 3
            r = to_dt(a)
        elif is_prop(a):
            my_type = 3
            r = to_prop(a)
        else:
            raise BsonUnsupportedObjectError

    return my_type.to_bytes() + name.encode() + b'\x00' + r


class Mapper:
    def marshal(self, data: dict[str, Any]) -> bytes:
        return marshal(data, self.keep_types)

    def unmarshal(self, data: bytes) -> dict[str, Any]:
        return unmarshal(data, self.python_only, self.keep_types)

    def __init__(self, **kwargs):
        self.__keep_types = False
        self.__python_only = False

        for key, val in kwargs.items():
            if key == 'python_only' and type(val) is bool:
                self.__python_only = val
            elif key == 'keep_types' and type(val) is bool:
                self.__keep_types = val
            else:
                raise MapperConfigError

    @property
    def keep_types(self) -> bool:
        return self.__keep_types

    @keep_types.setter
    def keep_types(self, value):
        raise AttributeError("Cannot set attribute 'keep_types'")

    @property
    def python_only(self) -> bool:
        return self.__python_only

    @python_only.setter
    def python_only(self, value):
        raise AttributeError("Cannot set attribute 'python_only'")


if __name__ == '__main__':
    context = {'math': math}
    run_calc(context)
