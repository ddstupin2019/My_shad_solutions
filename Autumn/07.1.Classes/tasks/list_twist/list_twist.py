from collections import UserList
import typing as tp


class ListTwist(UserList[tp.Any]):
    """
    List-like class with additional attributes:
        * reversed, R - return reversed list
        * first, F - insert or retrieve first element;
                     Undefined for empty list
        * last, L -  insert or retrieve last element;
                     Undefined for empty list
        * size, S -  set or retrieve size of list;
                     If size less than list length - truncate to size;
                     If size greater than list length - pad with Nones
    """

    def __getattr__(self, name):
        if name in ('reversed', 'R'):
            return self.data[::-1]
        elif name in ('first', 'F'):
            return self.data[0]
        elif name in ('last', 'L'):
            return self.data[-1]
        elif name in ('size', 'S'):
            return len(self.data)
        else:
            return super().__getattr__(name)

    def __setattr__(self, name, value):
        if name in ('first', 'F'):
            self.data[0] = value
        elif name in ('last', 'L'):
            self.data[-1] = value
        elif name in ('size', 'S'):
            current_size = len(self.data)
            if value > current_size:
                self.data.extend([None] * (value - current_size))
            elif value < current_size:
                del self.data[value:]
        else:
            super().__setattr__(name, value)

    def __init__(self, initial_list=[]):
        super().__init__(initial_list)
