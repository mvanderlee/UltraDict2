"""
Sweep buffer_size to find where full dumps stop dominating the cost.

Every write that does not fit the update buffer forces a full dump: the whole dict is
serialized by the writer and re-loaded by every reader. Undersize the buffer and a tiny
write moves the entire dict, once per reader.

Run it against your own payload shape by editing PAYLOAD_KEYS / PAYLOAD_BYTES / WRITES.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from UltraDict2 import UltraDict

# Shape of the dict that has to be dumped
PAYLOAD_KEYS = 200
PAYLOAD_BYTES = 500

# Small writes to time, the kind that should be cheap
WRITES = 2_000
WRITE_BYTES = 50

BUFFER_SIZES = [4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024]


def run(buffer_size):
    """One sweep row: prefill, then time small writes with a reader following along."""
    writer = UltraDict(buffer_size=buffer_size)
    for i in range(PAYLOAD_KEYS):
        writer[f'payload_{i}'] = 'x' * PAYLOAD_BYTES

    # A second instance attached by name is a real peer: its own local data, its own
    # full_dump_counter. Same thread, so wall clock is not a parallel figure, but the
    # dump and reload counts are exact.
    reader = UltraDict(name=writer.name)

    value = 'y' * WRITE_BYTES
    dumps_before = writer.get_metrics().full_dump_total

    t_start = time.perf_counter()
    for i in range(WRITES):
        writer[f'hot_{i % 10}'] = value
        reader[f'hot_{i % 10}']
    t_end = time.perf_counter()

    metrics = writer.get_metrics()
    dumps = metrics.full_dump_total - dumps_before

    # Bytes the change itself is worth, against bytes actually moved to publish it
    changed_bytes = metrics.item_size_bytes_sum
    dumped_bytes = (metrics.full_dump_last_bytes or 0) * dumps
    amplification = dumped_bytes / changed_bytes if changed_bytes else 0

    writer.close()
    reader.close()

    return {
        'buffer_size': buffer_size,
        'writes_per_sec': WRITES / (t_end - t_start),
        'dumps_per_1k': dumps * 1000 / WRITES,
        'used_fraction': metrics.buffer_used_fraction,
        'item_max': metrics.item_size_bytes_max,
        'amplification': amplification,
    }


def main():
    dict_bytes = PAYLOAD_KEYS * PAYLOAD_BYTES
    print(f"\nDict payload ~{dict_bytes:,d} bytes in {PAYLOAD_KEYS} keys")
    print(f"{WRITES:,d} writes of {WRITE_BYTES} bytes, one reader following in the same thread\n")

    print(f"{'buffer_size':>12}  {'writes/sec':>11}  {'dumps/1k':>9}  {'used':>6}  {'max item':>9}  {'amplification':>14}")
    print(f"{'-' * 12}  {'-' * 11}  {'-' * 9}  {'-' * 6}  {'-' * 9}  {'-' * 14}")

    for buffer_size in BUFFER_SIZES:
        row = run(buffer_size)
        print(
            f"{row['buffer_size']:>12,d}  {row['writes_per_sec']:>11,.0f}  {row['dumps_per_1k']:>9,.1f}  "
            f"{row['used_fraction']:>6.1%}  {row['item_max']:>9,d}  {row['amplification']:>13,.1f}x"
        )

    print("\nAmplification is bytes moved per byte of real change. It counts the writer only;")
    print("multiply by the number of reader processes for the true cost.")
    print("Wall clock is indicative: writer and reader share one thread here.")
    print("A buffer below 'max item' dumps on every single write.\n")


if __name__ == '__main__':
    main()
