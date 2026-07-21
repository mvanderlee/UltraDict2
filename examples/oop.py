#
# Class example linking attributes of two instances
#

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from UltraDict2 import UltraDict


class MyClass:
    link_name = 'shared-class'

    def __init__(self):
        object.__setattr__(self, '__dict__', UltraDict(name=self.link_name))

    @property
    def x(self):
        return self._x

    def __getattr__(self, name):
        return self.__dict__[name]

    def __setattr__(self, name, value):
        self.__dict__[name] = value


first = MyClass()
second = MyClass()

first.something = 'some value'

print(first.something, ' == ', second.something)
