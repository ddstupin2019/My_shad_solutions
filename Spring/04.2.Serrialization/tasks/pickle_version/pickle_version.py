import dataclasses
import pickle


@dataclasses.dataclass
class PickleVersion:
    is_new_format: bool
    version: int


def get_pickle_version(data: bytes) -> PickleVersion:
    """
    Returns used protocol version for serialization.

    :param data: serialized object in pickle format.
    :return: protocol version.
    """
    if data[0] == pickle.PROTO[0]:
        return PickleVersion(is_new_format=True, version=data[1])
    return PickleVersion(is_new_format=False, version=-1)
