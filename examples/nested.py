#
# Nested example
#
# Two dicts `ultra` and `other` are linked together using shared memory.
# Using `recurse=True` will transparently convert child dicts into UltraDict instances.

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, '../..')

from UltraDict2 import UltraDict

if __name__ == '__main__':
    # No name provided, create a new dict with random name
    ultra = UltraDict(name="my_name", recurse=True)
    # Connect `other` dict to `ultra` dict via `name`
    other = UltraDict(name=ultra.name)

    ultra['nested'] = {'deeper': {0: 1}}

    other['nested']['deeper'][0] += 1

    print(ultra, ' == ' if other == ultra else ' != ', other)
