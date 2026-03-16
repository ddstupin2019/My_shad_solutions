import typing as tp
import heapq



class Elem:
    val : int
    it = iter([])

    def __init__(self, val: int, it):
        self.val = val
        self.it = it
    def __lt__(self, other):
        return self.val < other.val

def merge(seq: tp.Sequence[tp.Sequence[int]]) -> list[int]:
    """
    :param seq: sequence of sorted sequences
    :return: merged sorted list
    """
    r = []
    heap=[]
    for i in seq:
        if i:
            q = iter(i)
            heap.append(Elem(next(q),q))
    heapq.heapify(heap)
    while heap:
        elem= heapq.heappop(heap)
        r.append(elem.val)
        try:
            heapq.heappush(heap,Elem(next(elem.it),elem.it))
        except StopIteration:
            pass
    return r
