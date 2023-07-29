from .scalar import Scalar
from typing import Type


class Vector:
    def __init__(self, type: Type[Scalar] = None, *args):
        self.data = []
        self.type = type
        for arg in args:
            self.append(arg)  # Use the append method for type checking and appending

    def append(self, item):
        if not isinstance(item, Scalar):
            raise TypeError("Item is not of type Scalar or its subclass")

        if self.type is None:
            self.type = type(item)
            self.data.append(item)
        elif isinstance(item, self.type):
            self.data.append(item)
        else:
            raise TypeError(f"Item is not of type {self.type.__name__}")

    def __eq__(self, other):
        if not isinstance(other, Vector):
            return False
        if len(self) != len(other):
            return False

        for self_item, other_item in zip(self.data, other.data):
            if self_item != other_item:
                return False

        return True

    def __getitem__(self, index):
        return self.data[index]

    def __len__(self):
        return len(self.data)

    def __repr__(self):
        return f"Vector of {self.type.__name__ if self.type else 'unknown type'}: {self.data}"
