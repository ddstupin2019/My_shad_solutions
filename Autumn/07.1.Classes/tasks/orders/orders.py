import abc
from dataclasses import dataclass, field, InitVar
from abc import ABC

DISCOUNT_PERCENTS = 15


@dataclass(init=True, frozen=True, order=True)
class Item:
    # note: you might want to change the order of fields
    item_id: int = field(compare=False)
    title: str
    cost: int

    def __post_init__(self):
        assert self.title != ''
        assert self.cost > 0


# You may set `# type: ignore` on this class
# It is [a really old issue](https://github.com/python/mypy/issues/5374)
# But seems to be solved
@dataclass
class Position(ABC):
    item: Item

    @property
    @abc.abstractmethod
    def cost(self):
        pass


@dataclass
class CountedPosition(Position):
    count: int = field(default=1)

    @property
    def cost(self):
        return self.item.cost * self.count


@dataclass
class WeightedPosition(Position):
    weight: float = field(default=1)

    @property
    def cost(self):
        return self.item.cost * self.weight


@dataclass
class Order:
    order_id: int
    positions: list[Position] = field(default_factory=list)
    cost: int = field(init=False, default=0)
    have_promo: InitVar[bool] = field(init=True, default=False)

    def __post_init__(self, have_promo: bool):
        tmp = 0.0
        for i in self.positions:
            tmp += i.cost
        if have_promo:
            tmp *= (1 - DISCOUNT_PERCENTS / 100)
        self.cost = int(tmp)
