import typing as tp
import heapq

from tensorflow.python.tpu.ops.gen_xla_ops import sort_list_of_sparse_core_coo_tensors


def merge(input_streams: tp.Sequence[tp.IO[bytes]], output_stream: tp.IO[bytes]) -> None:
    """
    Merge input_streams in output_stream
    :param input_streams: list of input streams. Contains byte-strings separated by "\n". Nonempty stream ends with "\n"
    :param output_stream: output stream. Contains byte-strings separated by "\n". Nonempty stream ends with "\n"
    :return: None
    """
    sort_list = []
    for input_stream in input_streams:
        sort_list = list(heapq.merge(list(map(int, input_stream.read().split())), sort_list))
    for i in sort_list:
        output_stream.write(bytes(str(i) + '\n', 'utf-8'))
    output_stream.flush()
