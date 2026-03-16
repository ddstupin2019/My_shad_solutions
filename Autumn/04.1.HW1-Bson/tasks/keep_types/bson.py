import struct
from datetime import datetime, timezone
import dataclasses
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Type
from collections import namedtuple

_TYPE_DOUBLE = b'\x01'
_TYPE_STRING = b'\x02'
_TYPE_DOCUMENT = b'\x03'
_TYPE_ARRAY = b'\x04'
_TYPE_BINARY = b'\x05'
_TYPE_BOOLEAN = b'\x08'
_TYPE_DATETIME = b'\x09'
_TYPE_NULL = b'\x0A'
_TYPE_INT32 = b'\x10'
_TYPE_INT64 = b'\x12'

_TYPE_UNDEFINED = b'\x06'
_TYPE_OBJECTID = b'\x07'
_TYPE_REGEX = b'\x0B'
_TYPE_DBPOINTER = b'\x0C'
_TYPE_JAVASCRIPT = b'\x0D'
_TYPE_SYMBOL = b'\x0E'
_TYPE_JAVASCRIPT_W_SCOPE = b'\x0F'
_TYPE_TIMESTAMP = b'\x11'
_TYPE_DECIMAL128 = b'\x13'
_TYPE_MIN_KEY = b'\xFF'
_TYPE_MAX_KEY = b'\x7F'

_KNOWN_TYPES = {
    _TYPE_DOUBLE, _TYPE_STRING, _TYPE_DOCUMENT, _TYPE_ARRAY, _TYPE_BINARY,
    _TYPE_UNDEFINED, _TYPE_OBJECTID, _TYPE_BOOLEAN, _TYPE_DATETIME, _TYPE_NULL,
    _TYPE_REGEX, _TYPE_DBPOINTER, _TYPE_JAVASCRIPT, _TYPE_SYMBOL,
    _TYPE_JAVASCRIPT_W_SCOPE, _TYPE_INT32, _TYPE_TIMESTAMP, _TYPE_INT64,
    _TYPE_DECIMAL128, _TYPE_MIN_KEY, _TYPE_MAX_KEY
}

_NULL_TERMINATOR = b'\x00'

_MAX_INT32 = 2 ** 31 - 1
_MIN_INT64 = -2 ** 63
_MAX_INT64 = 2 ** 63 - 1

_METADATA_KEY = "__metadata__"
_METADATA_SUBTYPE = b'\x80'


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


class BsonIncorrectSizeError(BsonBrokenDataError):
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


class BsonInvalidArrayError(BsonUnmarshalError):
    pass


class MapperConfigError(ValueError):
    pass


class MapperUnsupportedOptionError(MapperConfigError):
    pass


class _BsonReader:
    def __init__(self, data: Union[bytes, bytearray]):
        if isinstance(data, bytearray):
            data = bytes(data)
        self.data = data
        self.pos = 0
        self.limit = len(data)

    def _check_remaining(self, n: int):
        if self.pos + n > self.limit:
            raise BsonNotEnoughDataError("Неожиданный конец данных при чтении.")

    def read_bytes(self, n: int) -> bytes:
        self._check_remaining(n)
        chunk = self.data[self.pos: self.pos + n]
        self.pos += n
        return chunk

    def read_int32(self) -> int:
        return struct.unpack('<i', self.read_bytes(4))[0]

    def read_int64(self) -> int:
        return struct.unpack('<q', self.read_bytes(8))[0]

    def read_double(self) -> float:
        return struct.unpack('<d', self.read_bytes(8))[0]

    def read_byte(self) -> bytes:
        return self.read_bytes(1)

    def read_cstring(self, is_key: bool = False) -> str:
        try:
            null_pos = self.data.index(_NULL_TERMINATOR, self.pos)
        except ValueError:
            raise BsonBrokenDataError("Не найден \x00 для cstring.")

        if null_pos >= self.limit:
            raise BsonNotEnoughDataError("cstring выходит за границы.")

        s_bytes = self.data[self.pos: null_pos]
        self.pos = null_pos + 1
        try:
            return s_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            if is_key:
                raise BsonBadKeyDataError(f"Ключ не в UTF-8: {e}")
            else:
                raise BsonBadStringDataError(f"cstring не в UTF-8: {e}")

    def read_string(self) -> str:
        str_len = self.read_int32()

        if str_len < 1:
            raise BsonStringSizeError(f"Некорректный размер строки: {str_len}.")

        if self.pos + str_len > self.limit:
            raise BsonInconsistentStringSizeError(
                "Размер строки выходит за границы."
            )

        bytes_to_read = str_len - 1
        s_bytes = self.read_bytes(bytes_to_read)

        terminator = self.read_byte()
        if terminator != _NULL_TERMINATOR:
            raise BsonBrokenDataError("Строка не заканчивается на \x00.")

        try:
            return s_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise BsonBadStringDataError(f"Строка (string) не в UTF-8: {e}")

    def skip_bytes(self, n: int):
        self._check_remaining(n)
        self.pos += n

    def skip_value(self, e_type: bytes):
        if e_type == _TYPE_DOUBLE or e_type == _TYPE_DATETIME or e_type == _TYPE_INT64:
            self.skip_bytes(8)
        elif e_type == _TYPE_STRING:
            str_len = self.read_int32()
            if str_len < 1:
                raise BsonStringSizeError("Некорректная длина строки в skip_value")
            self.skip_bytes(str_len)
        elif e_type == _TYPE_DOCUMENT or e_type == _TYPE_ARRAY:
            doc_len = self.read_int32()
            if doc_len < 5:
                raise BsonIncorrectSizeError(f"Вложенный док-т < 5 байт в skip_value: {doc_len}")
            self.skip_bytes(doc_len - 4)
        elif e_type == _TYPE_BINARY:
            bin_len = self.read_int32()
            if bin_len < 0:
                raise BsonBrokenDataError("Отрицательная длина binary в skip_value")
            subtype = self.read_byte()
            subtype_int = subtype[0]
            if 10 <= subtype_int <= 127:
                raise BsonInvalidBinarySubtypeError(f"Недопустимый subtype: {subtype_int}")
            self.skip_bytes(bin_len)
        elif e_type == _TYPE_OBJECTID:
            self.skip_bytes(12)
        elif e_type == _TYPE_BOOLEAN:
            self.skip_bytes(1)
        elif e_type == _TYPE_NULL or e_type == _TYPE_UNDEFINED:
            pass
        elif e_type == _TYPE_INT32:
            self.skip_bytes(4)
        elif e_type == _TYPE_REGEX:
            _ = self.read_cstring(is_key=False)
            _ = self.read_cstring(is_key=False)
        elif e_type == _TYPE_DECIMAL128:
            self.skip_bytes(16)
        elif e_type == _TYPE_DBPOINTER:
            _ = self.read_string()
            self.skip_bytes(12)
        elif e_type == _TYPE_JAVASCRIPT:
            _ = self.read_string()
        elif e_type == _TYPE_SYMBOL:
            _ = self.read_string()
        elif e_type == _TYPE_JAVASCRIPT_W_SCOPE:
            _ = self.read_string()
            doc_len = self.read_int32()
            if doc_len < 5:
                raise BsonIncorrectSizeError(f"Вложенный док-т < 5 байт в skip_value: {doc_len}")
            self.skip_bytes(doc_len - 4)
        elif e_type == _TYPE_TIMESTAMP:
            self.skip_bytes(8)
        elif e_type == _TYPE_MIN_KEY or e_type == _TYPE_MAX_KEY:
            pass
        else:
            raise BsonInvalidElementTypeError(
                f"Неизвестный тип элемента: {e_type.hex()}"
            )


class _Serializer:
    def __init__(self, options: dict):
        self.keep_types = options.get('keep_types', False)
        self._current_path: Set[int] = set()

        self.type_registry: Dict[int, str] = {}
        self.type_definitions: Dict[str, Any] = {}
        self.type_counter: int = 0
        self.root_object_type_id: Optional[str] = None

    def _register_namedtuple_class(self, cls: type) -> str:
        cls_id = id(cls)
        if cls_id not in self.type_registry:
            type_id = f"nt-{self.type_counter}"
            self.type_counter += 1
            self.type_registry[cls_id] = type_id

            defaults = getattr(cls, '_field_defaults', {})
            fields = list(getattr(cls, '_fields', []))

            self.type_definitions[type_id] = {
                "name": cls.__name__,
                "fields": fields,
                "defaults": {k: defaults[k] for k in fields if k in defaults}
            }
            return type_id
        return self.type_registry[cls_id]

    def _register_dataclass_class(self, cls: type) -> str:
        cls_id = id(cls)
        if cls_id not in self.type_registry:
            type_id = f"dc-{self.type_counter}"
            self.type_counter += 1
            self.type_registry[cls_id] = type_id

            ann_dict = {}
            type_map = {str: "str", int: "int", bool: "bool"}

            for field_name, ann_type in getattr(cls, '__annotations__', {}).items():
                ann_dict[field_name] = type_map.get(ann_type, "any")

            params_obj = cls.__dataclass_params__
            params_dict = {
                'init': params_obj.init,
                'repr': params_obj.repr,
                'eq': params_obj.eq,
                'order': params_obj.order,
                'unsafe_hash': params_obj.unsafe_hash,
                'frozen': params_obj.frozen
            }

            self.type_definitions[type_id] = {
                "name": cls.__name__,
                "annotations": ann_dict,
                "params": params_dict
            }
            return type_id
        return self.type_registry[cls_id]

    def marshal(self, data: Any) -> bytes:
        doc_to_pack = None
        key_order = None
        props = None

        if isinstance(data, dict):
            doc_to_pack = data
        elif dataclasses.is_dataclass(data) and not isinstance(data, type):
            doc_to_pack = {f.name: getattr(data, f.name) for f in dataclasses.fields(data)}
            key_order = [f.name for f in dataclasses.fields(data)]
            if self.keep_types:
                self.root_object_type_id = self._register_dataclass_class(type(data))
        elif isinstance(data, tuple) and hasattr(data, '_fields'):
            doc_to_pack = data._asdict()
            key_order = data._fields
            if self.keep_types:
                self.root_object_type_id = self._register_namedtuple_class(type(data))
        elif isinstance(data, (list, tuple)):
            raise BsonUnsupportedObjectError("Корневой объект не может быть list или tuple, только document-like.")
        else:
            props = self._get_properties(data)
            if props is not None:
                doc_to_pack = props
            else:
                raise BsonUnsupportedObjectError(
                    f"Неподдерживаемый тип для BSON: {type(data)}"
                )

        if doc_to_pack is None:
            raise BsonUnsupportedObjectError(f"Не удалось определить поля для: {type(data)}")

        if not doc_to_pack:
            if isinstance(data, (dict, type(None))):
                pass
            elif dataclasses.is_dataclass(data) or (isinstance(data, tuple) and hasattr(data, '_fields')):
                pass
            elif props is not None:
                raise BsonUnsupportedObjectError(
                    f"Объект {type(data)} имеет property, но ни одна не читаема.")
            else:
                pass

        try:
            val_id = id(data)
            if val_id in self._current_path:
                raise BsonCycleDetectedError("Обнаружена циклическая ссылка (корневой объект).")
            self._current_path.add(val_id)

            doc_bytes = self._pack_document_recursive(doc_to_pack, key_order=key_order, is_root=True)

            self._current_path.remove(val_id)

            total_len = len(doc_bytes)
            if total_len > _MAX_INT32:
                raise BsonDocumentTooBigError(f"Размер корневого документа {total_len} > {_MAX_INT32}")

            return doc_bytes

        except (BsonUnsupportedKeyError, BsonKeyWithZeroByteError,
                BsonUnsupportedObjectError, BsonInputTooBigError):
            raise
        finally:
            val_id = id(data)
            if val_id in self._current_path:
                self._current_path.remove(val_id)

    def _get_properties(self, obj: Any) -> Optional[Dict[str, Any]]:
        props = {}
        has_props = False

        for name in dir(obj):
            if name.startswith('_'):
                continue

            try:
                attr = getattr(type(obj), name, None)
                if isinstance(attr, property):
                    has_props = True
                    if attr.fget:
                        try:
                            props[name] = getattr(obj, name)
                        except (AttributeError, ValueError, TypeError, BsonError):
                            pass
            except (AttributeError, ValueError, TypeError, BsonError):
                pass

        return props if has_props else None

    def _pack_element(self, key_bytes: bytes, value: Any) -> bytes:
        val_id = id(value)
        if val_id in self._current_path:
            raise BsonCycleDetectedError("Обнаружена циклическая ссылка.")

        is_container = False

        try:
            if isinstance(value, float):
                return _TYPE_DOUBLE + key_bytes + struct.pack('<d', value)

            if isinstance(value, str):
                s_bytes = value.encode('utf-8')
                s_len = len(s_bytes) + 1
                if s_len > _MAX_INT32:
                    raise BsonStringTooBigError(f"Строка слишком велика: {s_len} > {_MAX_INT32}")
                s_len_bytes = struct.pack('<i', s_len)
                return _TYPE_STRING + key_bytes + s_len_bytes + s_bytes + _NULL_TERMINATOR

            doc_to_pack = None
            key_order = None

            if dataclasses.is_dataclass(value) and not isinstance(value, type):
                is_container = True
                doc_to_pack = {f.name: getattr(value, f.name) for f in dataclasses.fields(value)}
                key_order = [f.name for f in dataclasses.fields(value)]
            elif isinstance(value, tuple) and hasattr(value, '_fields'):
                is_container = True
                doc_to_pack = value._asdict()
                key_order = value._fields
            elif isinstance(value, dict):
                is_container = True
                doc_to_pack = value

            if doc_to_pack is not None:
                self._current_path.add(val_id)
                packed_doc = self._pack_document_recursive(
                    doc_to_pack,
                    key_order=key_order,
                    is_root=False
                )
                self._current_path.remove(val_id)
                return _TYPE_DOCUMENT + key_bytes + packed_doc

            if isinstance(value, (list, tuple)):
                is_container = True
                self._current_path.add(val_id)
                packed_array = self._pack_array_recursive(value)
                self._current_path.remove(val_id)
                return _TYPE_ARRAY + key_bytes + packed_array

            if isinstance(value, (bytes, bytearray)):
                val_len = len(value)
                if val_len > _MAX_INT32:
                    raise BsonBinaryTooBigError(f"Бинарные данные слишком велики: {val_len} > {_MAX_INT32}")

                val_len_bytes = struct.pack('<i', val_len)
                subtype = b'\x00'
                return _TYPE_BINARY + key_bytes + val_len_bytes + subtype + value

            if isinstance(value, bool):
                val_byte = b'\x01' if value else b'\x00'
                return _TYPE_BOOLEAN + key_bytes + val_byte

            if isinstance(value, datetime):
                if value.tzinfo is None:
                    raise BsonUnsupportedObjectError("datetime должен иметь tzinfo")

                ts_ms = int(value.timestamp() * 1000)
                return _TYPE_DATETIME + key_bytes + struct.pack('<q', ts_ms)

            if value is None:
                return _TYPE_NULL + key_bytes

            if isinstance(value, int):
                if not (_MIN_INT64 <= value <= _MAX_INT64):
                    raise BsonIntegerTooBigError(f"Целое число {value} вне диапазона 64-bit.")

                if -2 ** 31 <= value < 2 ** 31:
                    return _TYPE_INT32 + key_bytes + struct.pack('<i', value)
                else:
                    return _TYPE_INT64 + key_bytes + struct.pack('<q', value)

            props = self._get_properties(value)
            if props is not None:
                is_container = True
                self._current_path.add(val_id)
                packed_props = self._pack_document_recursive(props, key_order=None, is_root=False)
                self._current_path.remove(val_id)
                return _TYPE_DOCUMENT + key_bytes + packed_props

            raise BsonUnsupportedObjectError(
                f"Неподдерживаемый тип для BSON: {type(value)}"
            )

        except BsonInputTooBigError as e:
            raise e
        except Exception as e:
            if isinstance(e, (BsonError, RecursionError)):
                raise
            raise BsonMarshalError(f"Ошибка при упаковке {type(value)}: {e}")
        finally:
            if is_container:
                if val_id in self._current_path:
                    self._current_path.remove(val_id)

    def _build_metadata_field(self, metadata_list: List[str]) -> bytes:
        metadata_str = ":".join(metadata_list)
        if not metadata_str.strip(':'):
            return b''

        key_bytes = _METADATA_KEY.encode('utf-8') + _NULL_TERMINATOR
        val_bytes = metadata_str.encode('utf-8')
        val_len_bytes = struct.pack('<i', len(val_bytes))

        return _TYPE_BINARY + key_bytes + val_len_bytes + _METADATA_SUBTYPE + val_bytes

    def _pack_document_recursive(self, doc: Dict, key_order: Optional[List[str]] = None,
                                 is_root: bool = False) -> bytes:
        e_list_bytes = []
        current_len = 0
        metadata_list: List[str] = []

        if key_order is None:
            try:
                keys_to_pack = sorted(doc.keys())
            except TypeError:
                for k in doc.keys():
                    if not isinstance(k, str):
                        raise BsonUnsupportedKeyError(
                            f"Ключ {k} (тип {type(k)}) не является строкой."
                        )
                raise
        else:
            keys_to_pack = key_order

        for key in keys_to_pack:
            if not isinstance(key, str):
                raise BsonUnsupportedKeyError(
                    f"Ключ {key} (тип {type(key)}) не является строкой."
                )
            try:
                key_bytes_utf8 = key.encode('utf-8')
            except UnicodeEncodeError as e:
                raise BsonKeyWithZeroByteError(f"Ошибка кодирования ключа {key}: {e}")

            if b'\x00' in key_bytes_utf8:
                raise BsonKeyWithZeroByteError(f"Ключ '{key}' содержит нулевой байт.")

        for key in keys_to_pack:
            if key not in doc and key_order is not None:
                continue

            value = doc[key]

            if self.keep_types:
                if dataclasses.is_dataclass(value) and not isinstance(value, type):
                    type_id = self._register_dataclass_class(type(value))
                    metadata_list.append(type_id)
                elif isinstance(value, tuple) and hasattr(value, '_fields'):
                    type_id = self._register_namedtuple_class(type(value))
                    metadata_list.append(type_id)
                elif isinstance(value, tuple):
                    metadata_list.append('tuple')
                elif isinstance(value, bytearray):
                    metadata_list.append('bytearray')
                else:
                    metadata_list.append('')

            key_bytes = key.encode('utf-8') + _NULL_TERMINATOR

            element_bytes = self._pack_element(key_bytes, value)
            e_list_bytes.append(element_bytes)

            current_len += len(element_bytes)
            if 4 + current_len + 1 > _MAX_INT32:
                raise BsonDocumentTooBigError(
                    f"Документ превысил лимит {current_len} > {_MAX_INT32}"
                )

        if self.keep_types:
            meta_field_bytes = b''
            if is_root and (self.type_definitions or self.root_object_type_id):
                meta_doc: Dict[str, Any] = {"types": self.type_definitions}
                children_meta_str = ":".join(metadata_list)
                if children_meta_str.strip(':'):
                    meta_doc["children"] = children_meta_str
                if self.root_object_type_id:
                    meta_doc["self"] = self.root_object_type_id

                meta_serializer = _Serializer({'keep_types': False, 'python_only': False})
                meta_doc_bytes = meta_serializer.marshal(meta_doc)
                meta_value_bytes = b'\x00' + meta_doc_bytes

                key_bytes = _METADATA_KEY.encode('utf-8') + _NULL_TERMINATOR
                val_len_bytes = struct.pack('<i', len(meta_value_bytes))
                meta_field_bytes = _TYPE_BINARY + key_bytes + val_len_bytes + _METADATA_SUBTYPE + meta_value_bytes

            elif (metadata_list or (key_order is not None and len(key_order) > 0) or doc):
                meta_field_bytes = self._build_metadata_field(metadata_list)

            if meta_field_bytes:
                e_list_bytes.append(meta_field_bytes)
                current_len += len(meta_field_bytes)

        if 4 + current_len + 1 > _MAX_INT32:
            raise BsonDocumentTooBigError(
                "Документ превысил лимит при добавлении __metadata__"
            )

        e_list = b''.join(e_list_bytes)
        total_len = 4 + len(e_list) + 1
        len_bytes = struct.pack('<i', total_len)

        return len_bytes + e_list + _NULL_TERMINATOR

    def _pack_array_recursive(self, arr: Union[List, Tuple]) -> bytes:
        e_list_bytes = []
        current_len = 0
        metadata_list: List[str] = []

        for i, value in enumerate(arr):
            key = str(i)
            key_bytes = key.encode('utf-8') + _NULL_TERMINATOR

            if self.keep_types:
                if dataclasses.is_dataclass(value) and not isinstance(value, type):
                    type_id = self._register_dataclass_class(type(value))
                    metadata_list.append(type_id)
                elif isinstance(value, tuple) and hasattr(value, '_fields'):
                    type_id = self._register_namedtuple_class(type(value))
                    metadata_list.append(type_id)
                elif isinstance(value, tuple):
                    metadata_list.append('tuple')
                elif isinstance(value, bytearray):
                    metadata_list.append('bytearray')
                else:
                    metadata_list.append('')

            element_bytes = self._pack_element(key_bytes, value)
            e_list_bytes.append(element_bytes)

            current_len += len(element_bytes)
            if 4 + current_len + 1 > _MAX_INT32:
                raise BsonDocumentTooBigError(f"Массив превысил лимит {current_len} > {_MAX_INT32}")

        if self.keep_types and (metadata_list or arr is not None):
            meta_field = self._build_metadata_field(metadata_list)
            if meta_field:
                e_list_bytes.append(meta_field)
                current_len += len(meta_field)
                if 4 + current_len + 1 > _MAX_INT32:
                    raise BsonDocumentTooBigError(
                        "Документ превысил лимит при добавлении __metadata__"
                    )

        e_list = b''.join(e_list_bytes)
        total_len = 4 + len(e_list) + 1
        len_bytes = struct.pack('<i', total_len)

        return len_bytes + e_list + _NULL_TERMINATOR


class _Deserializer:
    def __init__(self, options: dict):
        self.python_only = options.get('python_only', False)
        self.keep_types = options.get('keep_types', False)
        self.type_definitions: Dict[str, Any] = {}
        self.type_cache: Dict[str, Type] = {}
        self.root_object_type_id: Optional[str] = None
        self.reader: _BsonReader = _BsonReader(b'')

    def _prescan_for_types(self, data: bytes):
        try:
            reader = _BsonReader(data)
            doc_len = reader.read_int32()
            if doc_len < 5 or doc_len > len(data):
                return

            doc_end_pos = doc_len - 1
            reader.limit = doc_end_pos

            while reader.pos < reader.limit:
                e_type = reader.read_byte()
                if e_type == _NULL_TERMINATOR:
                    break
                if e_type not in _KNOWN_TYPES:
                    break

                key = reader.read_cstring(is_key=True)

                if key == _METADATA_KEY and e_type == _TYPE_BINARY:
                    bin_len = reader.read_int32()
                    if bin_len < 0:
                        break
                    subtype = reader.read_byte()
                    if subtype == _METADATA_SUBTYPE:
                        if bin_len > reader.limit - reader.pos:
                            break
                        metadata_bytes = reader.read_bytes(bin_len)
                        if metadata_bytes.startswith(b'\x00'):
                            meta_deserializer = _Deserializer({'python_only': self.python_only, 'keep_types': False})
                            meta_doc = meta_deserializer.unmarshal(metadata_bytes[1:])
                            if isinstance(meta_doc, dict):
                                self.type_definitions.update(meta_doc.get("types", {}))
                                self.root_object_type_id = meta_doc.get("self")
                    break
                else:
                    reader.skip_value(e_type)
        except (BsonError, struct.error, IndexError):
            pass

    def unmarshal(self, data: Union[bytes, bytearray]) -> Union[Dict, List]:
        if isinstance(data, bytearray):
            data = bytes(data)

        if len(data) < 4:
            raise BsonBrokenDataError("Данные слишком малы (меньше 4 байт).")

        try:
            doc_len = struct.unpack('<i', data[:4])[0]
        except struct.error:
            raise BsonBrokenDataError("Невозможно прочитать длину документа.")

        if doc_len < 0:
            raise BsonNotEnoughDataError(f"Заявлена отрицательная длина: {doc_len}")
        if doc_len > len(data):
            raise BsonNotEnoughDataError(
                f"Размер данных {len(data)} < заявленного {doc_len}."
            )
        if doc_len < len(data):
            if doc_len < 5:
                if doc_len == 4:
                    raise BsonTooManyDataError(
                        f"Размер данных {len(data)} > заявленного {doc_len}."
                    )
                else:
                    raise BsonIncorrectSizeError(f"Заявленная длина {doc_len} < 5.")
            else:
                raise BsonTooManyDataError(
                    f"Размер данных {len(data)} > заявленного {doc_len}."
                )
        if doc_len < 5:
            raise BsonIncorrectSizeError(f"Заявленная длина {doc_len} < 5.")
        if data[-1] != 0:
            raise BsonBrokenDataError("Документ не заканчивается на \x00.")

        if self.keep_types:
            self._prescan_for_types(data)

        self.reader = _BsonReader(data)

        result = self._unpack_document_recursive(is_array=False)

        if self.keep_types and self.root_object_type_id and isinstance(result, dict):
            if self.root_object_type_id.startswith('nt-'):
                result = self._apply_namedtuple_conversion(result, self.root_object_type_id)
            elif self.root_object_type_id.startswith('dc-'):
                result = self._apply_dataclass_conversion(result, self.root_object_type_id)

        return result

    def _apply_namedtuple_conversion(self, data: Any, type_id: str) -> Any:
        if not isinstance(data, dict):
            return data

        NewType: Optional[Type] = self.type_cache.get(type_id)

        if NewType is None:
            definition = self.type_definitions.get(type_id)
            if not definition:
                return data
            try:
                name = definition['name']
                fields = definition['fields']
                defaults = definition.get('defaults', {})

                defaults_list = None
                first_default_idx = -1
                for i, f in enumerate(fields):
                    if f in defaults:
                        first_default_idx = i
                        break

                if first_default_idx != -1:
                    defaults_list = []
                    for f in fields[first_default_idx:]:
                        if f not in defaults:
                            return data
                        defaults_list.append(defaults[f])

                NewType = namedtuple(name, fields, defaults=defaults_list)
                self.type_cache[type_id] = NewType

            except Exception:
                return data

        if NewType is None:
            return data

        try:
            filtered_data = {k: v for k, v in data.items() if k in NewType._fields}
            return NewType(**filtered_data)
        except Exception:
            return data

    def _apply_dataclass_conversion(self, data: Any, type_id: str) -> Any:
        if not isinstance(data, dict):
            return data

        definition = self.type_definitions.get(type_id)
        if not definition:
            return data

        NewType: Optional[Type] = self.type_cache.get(type_id)

        if NewType is None:
            try:
                name = definition['name']
                params = definition['params']
                annotations_dict = definition['annotations']

                type_map = {"str": str, "int": int, "bool": bool, "any": Any}

                cls_annotations = {
                    field_name: type_map.get(type_str, Any)
                    for field_name, type_str in annotations_dict.items()
                }

                cls_body = {
                    '__annotations__': cls_annotations
                }

                NewType = type(name, (object,), cls_body)
                NewType = dataclasses.dataclass(**params)(NewType)

                self.type_cache[type_id] = NewType

            except Exception:
                return data

        if NewType is None:
            return data

        try:
            filtered_data = {k: v for k, v in data.items() if k in definition['annotations']}
            return NewType(**filtered_data)
        except Exception:
            return data

    def _apply_metadata(self,
                        result: Union[Dict, List],
                        metadata_field: Optional[bytes],
                        is_array: bool):

        if not self.keep_types or metadata_field is None:
            return result

        try:
            metadata_str = metadata_field.decode('utf-8')
            types = metadata_str.split(':')
        except Exception:
            return result

        if is_array:
            assert isinstance(result, list)
            for i, type_str in enumerate(types):
                if i >= len(result):
                    break

                if type_str == 'tuple' and isinstance(result[i], list):
                    result[i] = tuple(result[i])
                elif type_str == 'bytearray' and isinstance(result[i], bytes):
                    result[i] = bytearray(result[i])
                elif type_str.startswith('nt-'):
                    result[i] = self._apply_namedtuple_conversion(result[i], type_str)
                elif type_str.startswith('dc-'):
                    result[i] = self._apply_dataclass_conversion(result[i], type_str)
        else:
            assert isinstance(result, dict)
            sorted_keys = sorted(result.keys())

            if len(types) != len(sorted_keys):
                if types == [''] and not sorted_keys:
                    pass
                else:
                    return result

            for key, type_str in zip(sorted_keys, types):
                if type_str == 'tuple' and isinstance(result[key], list):
                    result[key] = tuple(result[key])
                elif type_str == 'bytearray' and isinstance(result[key], bytes):
                    result[key] = bytearray(result[key])
                elif type_str.startswith('nt-'):
                    result[key] = self._apply_namedtuple_conversion(result[key], type_str)
                elif type_str.startswith('dc-'):
                    result[key] = self._apply_dataclass_conversion(result[key], type_str)

        return result

    def _unpack_document_recursive(self, is_array: bool) -> Union[Dict, List]:

        doc_len = self.reader.read_int32()
        if doc_len < 5:
            raise BsonIncorrectSizeError(f"Вложенный документ < 5 байт: {doc_len}")

        doc_end_pos = self.reader.pos + doc_len - 4
        if doc_end_pos > self.reader.limit:
            raise BsonNotEnoughDataError("Вложенный документ выходит за границы.")

        original_limit = self.reader.limit
        self.reader.limit = doc_end_pos - 1

        keys_seen: Set[str] = set()
        array_elements: Dict[int, Any] = {}
        doc_elements: Dict[str, Any] = {}
        metadata_field: Optional[bytes] = None

        try:
            while self.reader.pos < self.reader.limit:
                e_type = self.reader.read_byte()

                if e_type == _NULL_TERMINATOR:
                    raise BsonBrokenDataError("Неожиданный \x00 в середине e_list.")

                if e_type not in _KNOWN_TYPES:
                    raise BsonInvalidElementTypeError(
                        f"Неизвестный тип элемента: {e_type.hex()}"
                    )

                key = self.reader.read_cstring(is_key=True)
                is_metadata = (key == _METADATA_KEY)

                if is_array:
                    if not key.isdigit() and not is_metadata:
                        raise BsonBadArrayIndexError(f"Нечисловой ключ в массиве: {key}")

                    if key.isdigit():
                        if len(key) > 1 and key.startswith('0'):
                            raise BsonBadArrayIndexError(f"Неканонический ключ (начинается с 0): {key}")

                        array_index = int(key)
                        if array_index < 0:
                            raise BsonBadArrayIndexError(f"Отрицательный ключ в массиве: {key}")

                        if key in keys_seen:
                            raise BsonRepeatedKeyDataError(f"Повторяющийся ключ: {key}")
                        keys_seen.add(key)

                elif key in keys_seen:
                    if key == _METADATA_KEY:
                        pass
                    else:
                        raise BsonRepeatedKeyDataError(f"Повторяющийся ключ: {key}")
                keys_seen.add(key)

                value = None

                if e_type == _TYPE_DOUBLE:
                    value = self.reader.read_double()
                elif e_type == _TYPE_STRING:
                    value = self.reader.read_string()
                elif e_type == _TYPE_DOCUMENT:
                    value = self._unpack_document_recursive(is_array=False)
                elif e_type == _TYPE_ARRAY:
                    value = self._unpack_document_recursive(is_array=True)

                elif e_type == _TYPE_BINARY:
                    bin_len = self.reader.read_int32()
                    if bin_len < 0:
                        raise BsonBrokenDataError("Отрицательная длина binary")
                    subtype = self.reader.read_byte()
                    subtype_int = subtype[0]

                    if 10 <= subtype_int <= 127:
                        raise BsonInvalidBinarySubtypeError(f"Недопустимый subtype: {subtype_int}")

                    is_generic = (subtype == b'\x00')
                    is_metadata_subtype = (subtype == _METADATA_SUBTYPE)

                    if is_metadata and is_metadata_subtype:
                        metadata_bytes = self.reader.read_bytes(bin_len)
                        if self.keep_types and metadata_bytes.startswith(b'\x00'):
                            try:
                                meta_deserializer = _Deserializer(
                                    {'python_only': self.python_only, 'keep_types': False})
                                meta_doc = meta_deserializer.unmarshal(metadata_bytes[1:])
                                if isinstance(meta_doc, dict):
                                    metadata_field = meta_doc.get("children", "").encode('utf-8')
                            except BsonError:
                                metadata_field = None
                        else:
                            metadata_field = metadata_bytes
                        continue

                    if self.python_only and not is_generic:
                        raise BsonInvalidBinarySubtypeError(
                            f"python_only: недопустимый subtype {subtype.hex()}"
                        )

                    if is_generic:
                        value = self.reader.read_bytes(bin_len)
                    else:
                        self.reader.skip_bytes(bin_len)
                        continue

                elif e_type == _TYPE_BOOLEAN:
                    bool_val = self.reader.read_byte()
                    value = (bool_val != b'\x00')
                    if self.python_only and bool_val not in (b'\x00', b'\x01'):
                        pass

                elif e_type == _TYPE_DATETIME:
                    ts_ms = self.reader.read_int64()
                    value = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                elif e_type == _TYPE_NULL:
                    value = None
                elif e_type == _TYPE_INT32:
                    value = self.reader.read_int32()
                elif e_type == _TYPE_INT64:
                    value = self.reader.read_int64()

                else:
                    if self.python_only:
                        raise BsonInvalidElementTypeError(
                            f"python_only: недопустимый тип элемента {e_type.hex()}"
                        )
                    try:
                        self.reader.skip_value(e_type)
                    except BsonError as e:
                        raise e
                    continue

                if is_array:
                    if not is_metadata:
                        array_elements[array_index] = value
                else:
                    doc_elements[key] = value

            if self.reader.pos != self.reader.limit:
                raise BsonBrokenDataError("Чтение вышло за границы e_list.")

        finally:
            self.reader.limit = original_limit
            self.reader.pos = doc_end_pos - 1

        final_byte = self.reader.read_byte()
        if final_byte != _NULL_TERMINATOR:
            raise BsonBrokenDataError("Документ не заканчивается на \x00.")

        if is_array:
            if not array_elements:
                return self._apply_metadata([], metadata_field, is_array=True)

            numeric_keys = array_elements.keys()
            max_index = max(numeric_keys)
            result_list = []

            for i in range(max_index + 1):
                if i not in array_elements:
                    if self.python_only:
                        raise BsonInvalidArrayError(
                            f"python_only: 'дырка' в массиве на индексе {i}"
                        )
                    result_list.append(None)
                else:
                    result_list.append(array_elements[i])

            return self._apply_metadata(result_list, metadata_field, is_array=True)

        else:
            return self._apply_metadata(doc_elements, metadata_field, is_array=False)


_DEFAULT_OPTIONS = {
    'python_only': False,
    'keep_types': False,
}


class Mapper:

    def __init__(self, **kwargs):
        self._options = _DEFAULT_OPTIONS.copy()

        for key, value in kwargs.items():
            if key not in self._options:
                raise MapperUnsupportedOptionError(f"Неподдерживаемая опция: {key}")
            self._options[key] = value

    @property
    def python_only(self) -> bool:
        return self._options['python_only']

    @python_only.setter
    def python_only(self, value):
        raise AttributeError("Опции Mapper являются read-only.")

    @python_only.deleter
    def python_only(self):
        raise AttributeError("Опции Mapper являются read-only.")

    @property
    def keep_types(self) -> bool:
        return self._options['keep_types']

    @keep_types.setter
    def keep_types(self, value):
        raise AttributeError("Опции Mapper являются read-only.")

    @keep_types.deleter
    def keep_types(self):
        raise AttributeError("Опции Mapper являются read-only.")

    def marshal(self, data: Any) -> bytes:
        if isinstance(data, (list, tuple)) and not hasattr(data, '_fields'):
            raise BsonUnsupportedObjectError(
                "Корневой объект не может быть list или tuple."
            )

        serializer = _Serializer(self._options)
        return serializer.marshal(data)

    def unmarshal(self, data: Union[bytes, bytearray]) -> Union[Dict, List]:
        deserializer = _Deserializer(self._options)
        return deserializer.unmarshal(data)


_DEFAULT_MAPPER = Mapper()


def marshal(data: Any) -> bytes:
    return _DEFAULT_MAPPER.marshal(data)


def unmarshal(data: Union[bytes, bytearray]) -> Union[Dict, List]:
    return _DEFAULT_MAPPER.unmarshal(data)
