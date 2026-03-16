import typing as tp
from pathlib import Path
import sys


def tail(filename: Path, lines_amount: int = 10, output: tp.IO[bytes] | None = None):
    """
    :param filename: file to read lines from (the file can be very large)
    :param lines_amount: number of lines to read
    :param output: stream to write requested amount of last lines from file
                   (if nothing specified stdout will be used)
    """
    if output is None:
        output = sys.stdout.buffer
    buffer_size = 1000

    with open(filename, 'rb') as f:
        f.seek(0, 2)
        file_size = f.tell()
        position = file_size

        while position > 0:
            read_size = min(buffer_size, position)
            position -= read_size
            f.seek(position)
            buf = bytearray(read_size)
            f.readinto(buf)
            buf_mv = memoryview(buf)

            for i in range(read_size - 1, -1, -1):
                if buf_mv[i] == ord('\n'):
                    lines_amount -= 1
                if lines_amount < 0 or (position == 0 and i == 0):
                    if lines_amount < 0:
                        output.write(buf_mv[i + 1:])
                    else:
                        output.write(buf_mv[i:])
                    position += read_size
                    while position < file_size:
                        f.seek(position)
                        buf = bytearray(read_size)
                        f.readinto(buf)
                        buf_mv = memoryview(buf)
                        output.write(buf_mv)
                        position += buffer_size
                    return
