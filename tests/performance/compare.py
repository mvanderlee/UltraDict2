"""
Read/write cost per operation: plain dict, UltraDict, Manager dict, Redis.

This is the script behind the numbers in the readme's performance comparison. Unlike
performance.py it reports seconds per operation rather than throughput, so candidates
that differ by four orders of magnitude stay comparable, and it gives each candidate its
own iteration count so a 350 us operation does not take a minute to measure.

Needs a Redis to talk to:

    docker run -d --rm --name redis -p 6379:6379 redis:7-alpine
    REDIS_HOST=127.0.0.1 python tests/performance/compare.py

Point REDIS_HOST/REDIS_PORT at another machine to see what a network hop costs.

The readme numbers were produced by running this inside the official python:3.X images
with Redis on the same host, once per supported Python version.
"""

import json
import multiprocessing
import os
import platform
import sys
import timeit

import redis

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from UltraDict2 import UltraDict

COUNT = 10_000
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))

# Enough iterations to be stable, few enough that a 350 us/op candidate still finishes
FAST = 200_000
SLOW = 20_000


def measure(stmt, variables, number):
    """Best of 3, converted to seconds per operation."""
    timer = timeit.Timer(stmt, globals=variables)
    return min(timer.repeat(repeat=3, number=number)) / number


def main():
    orig = {i: i for i in range(COUNT)}

    ultra = UltraDict(buffer_size=1_000_000)
    for key, value in orig.items():
        ultra[key] = value

    manager = multiprocessing.Manager()
    managed = manager.dict(orig)

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    client.flushall()
    client.mset(orig)

    variables = {'orig': orig, 'ultra': ultra, 'managed': managed, 'r': client}

    results = {
        'python': f'{sys.version_info.major}.{sys.version_info.minor}',
        'platform': f'{platform.system()} {platform.machine()}',
        'redis': f'{REDIS_HOST}:{REDIS_PORT}',
        'read': {
            'dict': measure('orig[1]', variables, FAST),
            'UltraDict': measure('ultra[1]', variables, FAST),
            'UltraDict.data': measure('ultra.data[1]', variables, FAST),
            'Manager dict': measure('managed[1]', variables, SLOW),
            'Redis': measure('r.get(1)', variables, SLOW),
        },
        'write': {
            'dict': measure('orig[1] = 1', variables, FAST),
            'UltraDict': measure('ultra[1] = 1', variables, FAST),
            'Manager dict': measure('managed[1] = 1', variables, SLOW),
            'Redis': measure('r.set(1, 1)', variables, SLOW),
        },
    }

    manager.shutdown()

    print(f"\nPython {results['python']} on {results['platform']}, {COUNT:,d} keys\n")
    for operation in ('read', 'write'):
        print(f'  {operation}')
        for name, seconds in sorted(results[operation].items(), key=lambda kv: kv[1]):
            shown = f'{seconds * 1e9:,.0f} ns' if seconds < 1e-6 else f'{seconds * 1e6:,.2f} µs'
            print(f'    {name:<16} {shown:>12}')
    print('\nRESULT_JSON ' + json.dumps(results))


if __name__ == '__main__':
    main()
