import types
import dis


def count_operations(source_code: types.CodeType) -> dict[str, int]:
    """Count byte code operations in given source code.

    :param source_code: the bytecode operation names to be extracted from
    :return: operation counts
    """
    result = {}
    for i in dis.get_instructions(source_code):
        result[i.opname] = result.get(i.opname, 0) + 1
        if isinstance(i.argval, types.CodeType):
            tmp = count_operations(i.argval)
            for j,k in tmp.items():
                result[j] = result.get(j, 0) + k
    return result
