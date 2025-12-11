from collections.abc import Iterable, Iterator, Sized


class RangeIterator(Iterator[int]):
    """The iterator class for Range"""

    def __init__(self, range_: 'Range') -> None:
        self.ind = range_.start - range_.step
        self.step = range_.step
        self.stop = range_.stop

    def __iter__(self) -> 'RangeIterator':
        return self

    def __next__(self) -> int:
        if self.step > 0:
            if self.ind + self.step < self.stop:
                self.ind += self.step
                return self.ind
            else:
                raise StopIteration
        else:
            if self.ind + self.step > self.stop:
                self.ind += self.step
                return self.ind
            else:
                raise StopIteration


class Range(Sized, Iterable[int]):
    """The range-like type, which represents an immutable sequence of numbers"""

    def __init__(self, *args: int) -> None:
        """
        :param args: either it's a single `stop` argument
            or sequence of `start, stop[, step]` arguments.
        If the `step` argument is omitted, it defaults to 1.
        If the `start` argument is omitted, it defaults to 0.
        If `step` is zero, ValueError is raised.
        """
        try:
            self.start, self.stop, self.step = args
            if self.step == 0:
                raise ValueError
        except ValueError:
            try:
                self.start, self.stop = args
                self.step = 1
            except ValueError:
                self.start, self.stop, self.step = 0, int(args[0]), 1
        self.iter = RangeIterator(self)

    def __iter__(self) -> 'RangeIterator':
        return RangeIterator(self)

    def __repr__(self) -> str:
        if self.step == 1:
            return f'range({self.start}, {self.stop})'
        else:
            return f'range({self.start}, {self.stop}, {self.step})'

    def __str__(self) -> str:
        return self.__repr__()

    def __contains__(self, key: int) -> bool:
        return ((self.start <= key < self.stop and key % self.step == self.start % self.step and self.step > 0) or
                (self.start >= key > self.stop and key % self.step == self.start % self.step and self.step < 0))

    def __getitem__(self, key: int) -> int:
        if key < 0:
            key = len(self) + key

        if key < 0 or key >= len(self):
            raise IndexError("range object index out of range")
        return self.start + key * self.step

    def __len__(self) -> int:
        return max((self.stop - self.start) // self.step + bool((self.stop - self.start) % self.step),0)
