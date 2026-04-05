import typing as tp

from animals import Cat, Cow, Dog
from abc import ABC, abstractmethod

class Animal(ABC):
    @abstractmethod
    def say(self) -> str:
        pass


class CatAdapter(Animal):
    def __init__(self, cat: Cat) -> None:
        self._cat = cat

    def say(self):
        return self._cat.say()


class DogAdapter(Animal):
    def __init__(self, dog: Dog) -> None:
        self._dog = dog

    def say(self):
        return self._dog.say('woof')

class CowAdapter(Animal):
    def __init__(self, cow: Cow) -> None:
        self._cow = cow

    def say(self):
        return self._cow.talk()

def animals_factory(animal: tp.Any) -> Animal:
    if type(animal) is Cat:
        return CatAdapter(animal)
    elif type(animal) is Dog:
        return DogAdapter(animal)
    elif type(animal) is Cow:
        return CowAdapter(animal)
    else:
        raise TypeError()
