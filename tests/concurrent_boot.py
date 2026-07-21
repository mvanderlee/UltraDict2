#
# Processes that start together on one name must not see a half-built dict.
#
# Creating the control memory publishes it, so a peer can attach while the creator is still
# filling in the metadata and has not created the stream buffer yet. The zeroed metadata reads
# back as shared_lock=False, which used to raise ParameterMismatch against a dict that is in fact
# being built with shared_lock=True.
#
# A Barrier is essential: initialisation takes microseconds, so processes have to be released at
# the same instant to land in the window at all.
#
# Even then, landing in it is a matter of luck: with the fix reverted this caught the bug in only
# about three runs out of four, which is how one broken commit passed on some Python versions and
# failed on others. So this is an end to end smoke test, not the regression guard --
# test_attaching_to_an_unsized_segment builds the window directly and catches it every time.
#

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import multiprocessing
import time

from UltraDict2 import UltraDict

workers = 8
attempts = 5


def boot(barrier, name):
    barrier.wait()
    ultra = UltraDict(name=name, shared_lock=True, buffer_size=10_000)
    ultra[f'key_{os.getpid()}'] = 1
    # Stay attached so the segment is not torn down while the others are still starting
    time.sleep(0.5)


if __name__ == '__main__':
    ctx = multiprocessing.get_context('spawn')
    failed = 0

    for attempt in range(attempts):
        name = f'concurrent_boot_{attempt}'
        UltraDict.unlink_by_name(name, ignore_errors=True)
        UltraDict.unlink_by_name(name + '_memory', ignore_errors=True)

        barrier = ctx.Barrier(workers)
        processes = [ctx.Process(target=boot, args=(barrier, name)) for _ in range(workers)]

        for p in processes:
            p.start()
        for p in processes:
            p.join(120)

        bad = [p.exitcode for p in processes if p.exitcode != 0]
        if bad:
            failed += 1
            print(f'attempt {attempt}: {len(bad)}/{workers} processes failed with {bad}')

    print(f'Failed attempts: {failed} == 0')
    sys.exit(1 if failed else 0)
