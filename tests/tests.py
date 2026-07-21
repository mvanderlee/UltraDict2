import os
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from UltraDict2 import UltraDict

# Disable logging
if hasattr(UltraDict.log, 'disable'):
    UltraDict.log.disable(UltraDict.log.CRITICAL)
else:
    UltraDict.log.set_level(UltraDict.log.Levels.error)


class UltraDictTests(unittest.TestCase):
    def setUp(self):
        pass

    def exec(self, filepath):
        ret = subprocess.run([sys.executable, filepath], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # print(ret.stdout.decode())
        ret.stdout = ret.stdout.replace(b'\r\n', b'\n')
        self.assertEqual(
            ret.returncode,
            0,
            f"Running '{filepath}' returned exit code '{ret.returncode}' but expected exit code is '0'"
            f"{self.exec_show_output(ret)}",
        )
        return ret

    def exec_show_output(self, ret):
        return f"\n\n{ret}\n\nOutput:\n{ret.stdout.decode()}\n"

    def assertReturnCode(self, ret, target=0):
        return self.assertEqual(ret.returncode, target, self.exec_show_output(ret))

    def test_count(self):
        ultra = UltraDict()
        other = UltraDict(name=ultra.name)

        count = 100
        for i in range(count // 2):
            ultra[i] = i

        for i in range(count // 2, count):
            other[i] = i

        self.assertEqual(len(ultra), len(other))

    def test_huge_value(self):
        ultra = UltraDict()

        # One megabyte string
        self.assertEqual(ultra.full_dump_counter, 0)

        length = 1_000_000

        ultra['huge'] = ' ' * length

        self.assertEqual(ultra.full_dump_counter, 1)
        self.assertEqual(len(ultra.data['huge']), length)

        other = UltraDict(name=ultra.name)

        self.assertEqual(len(other.data['huge']), length)

    def test_metrics(self):
        """A fresh dict reports nothing observed; writes move the counters."""
        ultra = UltraDict()

        metrics = ultra.get_metrics()
        self.assertEqual(metrics.item_count, 0)
        self.assertEqual(metrics.item_size_observations_total, 0)
        self.assertIsNone(metrics.item_size_bytes_min)
        self.assertIsNone(metrics.item_size_bytes_max)
        self.assertIsNone(metrics.full_dump_last_bytes)
        self.assertEqual(metrics.buffer_used_bytes, 0)
        self.assertEqual(metrics.buffer_used_fraction, 0)

        ultra['a'] = 1
        ultra['b'] = ' ' * 1000

        metrics = ultra.get_metrics()
        self.assertEqual(metrics.item_count, 2)
        self.assertEqual(metrics.item_size_observations_total, 2)
        self.assertLessEqual(metrics.item_size_bytes_min, metrics.item_size_bytes_max)
        self.assertGreaterEqual(metrics.item_size_bytes_sum, metrics.item_size_bytes_max)
        self.assertGreater(metrics.buffer_used_bytes, 0)
        self.assertLess(metrics.buffer_used_fraction, 1)
        self.assertEqual(metrics.buffer_size_bytes, ultra.buffer_size)
        self.assertEqual(metrics.buffer_full_forced_dump_total, 0)
        self.assertIsNone(metrics.full_dump_size_bytes)

    def test_metrics_buffer_full(self):
        """A value too big for the buffer counts as a forced full dump, not an error."""
        ultra = UltraDict()

        ultra['huge'] = ' ' * 1_000_000

        metrics = ultra.get_metrics()
        self.assertEqual(metrics.buffer_full_forced_dump_total, 1)
        self.assertEqual(metrics.full_dump_total, 1)
        self.assertGreater(metrics.full_dump_last_bytes, 1_000_000)
        self.assertEqual(metrics.full_dump_memory_full_total, 0)
        # The forced dump resets the stream
        self.assertEqual(metrics.buffer_used_bytes, 0)

    def test_metrics_full_dump_memory_full(self):
        """A static full dump memory too small to hold the dict raises and is counted."""
        ultra = UltraDict(buffer_size=4096, full_dump_size=4096)

        # The OS rounds a segment up to a page, and a page is 16 KiB on Apple Silicon, so a
        # 4096 byte request can hand back four times that. Ask the segment how big it
        # actually is instead of assuming the requested size is what we got.
        with self.assertRaises(UltraDict.Exceptions.FullDumpMemoryFull):
            ultra['huge'] = ' ' * (ultra.full_dump_memory.size * 2)

        metrics = ultra.get_metrics()
        self.assertEqual(metrics.full_dump_memory_full_total, 1)
        self.assertEqual(metrics.full_dump_size_bytes, 4096)
        self.assertEqual(metrics.buffer_full_forced_dump_total, 1)
        self.assertEqual(metrics.full_dump_total, 0)

    def test_parameter_passing(self):
        ultra = UltraDict(shared_lock=True, buffer_size=4096 * 8, full_dump_size=4096 * 8)
        # Connect `other` dict to `ultra` dict via `name`
        other = UltraDict(name=ultra.name)

        self.assertIsInstance(ultra.lock, ultra.SharedLock)
        self.assertIsInstance(other.lock, other.SharedLock)

        self.assertEqual(ultra.buffer_size, other.buffer_size)

    def test_iter(self):
        ultra = UltraDict()
        # Connect `other` dict to `ultra` dict via `name`
        other = UltraDict(name=ultra.name)

        ultra[1] = 1
        ultra[2] = 2

        counter = 0
        for i in other.items():
            counter += 1

        self.assertEqual(counter, 2)

        self.assertEqual(ultra.items(), other.items())

    def test_delete(self):
        import random
        import string

        letters = string.ascii_lowercase
        rand_str = ''.join(random.choice(letters) for i in range(1000))
        my_dict = UltraDict(buffer_size=10_000_000)
        for i in range(100_000):
            my_dict[i] = rand_str
        for i in list(my_dict.keys()):
            del my_dict[i]
        self.assertEqual(len(my_dict), 0)

    def test_already_exists(self):
        name = 'ultra_test'
        # Ensure we have a clean state before the test
        UltraDict.unlink_by_name(name, ignore_errors=True)
        UltraDict.unlink_by_name(name + '_memory', ignore_errors=True)
        # Create should be possible now
        u1 = UltraDict(name=name, create=True)
        with self.assertRaises(UltraDict.Exceptions.AlreadyExists):
            u2 = UltraDict(name=name, create=True)

    def test_not_already_exists(self):
        name = 'ultra_test'
        # Ensure we have a clean state before the test
        UltraDict.unlink_by_name(name, ignore_errors=True)
        UltraDict.unlink_by_name(name + '_memory', ignore_errors=True)

        with self.assertRaises(UltraDict.Exceptions.CannotAttachSharedMemory):
            ultra = UltraDict(name=name, create=False)

    def test_lock_blocking(self):
        pass

    def test_full_dump(self):
        # TODO
        pass

    # Turns out MacOS can only do 24 characters in total
    # def test_longest_name(self):
    #    for i in range(5, 50):
    #        print('Loop ', i)
    #        ultra = UltraDict(name='x' * i)

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux")
    def test_cleanup(self):
        # TODO
        import psutil

        p = psutil.Process()
        # Counted as a delta, not an absolute: handles the process already holds are none
        # of this test's business. On the CI runner under 3.14 a read handle on the
        # interpreter binary is open before this test starts, which an absolute count
        # reports as a leak.
        baseline = len(p.open_files())

        def assert_open_files(expected, what):
            files = p.open_files()
            self.assertEqual(len(files) - baseline, expected, f"{what} should be {expected}, open files are {files}")

        assert_open_files(0, "file handle count before before tests")
        ultra = UltraDict(nested={1: 1})
        assert_open_files(4, "file handle count with one simple UltraDict")
        del ultra
        assert_open_files(0, "file handle count after deleting the UltraDict")
        ultra = UltraDict(nested={1: 1}, recurse=True)
        assert_open_files(12, "nested file handle count")
        del ultra
        assert_open_files(0, "nested file handle count after deleting UltraDict")

    def test_example_simple(self):
        filename = "examples/simple.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(ret.stdout.splitlines()[-1], b"Length:  100000  ==  100000  ==  100000", self.exec_show_output(ret))

    def test_example_parallel(self):
        filename = "examples/parallel.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(ret.stdout.splitlines()[-1], b'Counter:  100000  ==  100000', self.exec_show_output(ret))

    def test_example_nested(self):
        filename = "examples/nested.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(
            ret.stdout.splitlines()[-1],
            b"{'nested': {'deeper': {0: 2}}}  ==  {'nested': {'deeper': {0: 2}}}",
            self.exec_show_output(ret),
        )

    def test_example_recover_from_stale_lock(self):
        filename = "examples/recover_from_stale_lock.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(ret.stdout.splitlines()[-1], b"Counter: 100 == 100", self.exec_show_output(ret))

    def test_plain_with_recovers_lock_from_dead_owner(self):
        """A bare `with lock:` recovers a lock whose owner died holding it."""
        timeout = 1.0
        ultra = UltraDict(shared_lock=True, lock_timeout=timeout)

        repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # Acquire the shared lock in another process, then die hard without releasing it
        code = (
            f"import os, sys; sys.path.insert(0, {repo_dir!r});"
            "from UltraDict2 import UltraDict;"
            f"u = UltraDict(name={ultra.name!r}, shared_lock=True);"
            "u.lock.acquire();"
            "os._exit(0)"
        )
        ret = subprocess.run([sys.executable, '-c', code], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.assertEqual(ret.returncode, 0, ret.stdout.decode())
        self.assertEqual(ultra.lock.get_remote_lock(), 1, "expected the dead child to still hold the lock")

        start = time.monotonic()
        with ultra.lock:
            ultra['recovered'] = True
        elapsed = time.monotonic() - start

        self.assertGreaterEqual(elapsed, timeout, f"recovered before the timeout elapsed ({elapsed:.2f}s)")
        self.assertLess(elapsed, 30, f"never recovered, waited {elapsed:.2f}s")
        self.assertEqual(ultra['recovered'], True)

    def test_steal_from_live_owner_returns_false(self):
        """A live owner is contention, not corruption: nothing to steal and no exception."""
        ultra = UltraDict(shared_lock=True)
        other = UltraDict(name=ultra.name, shared_lock=True)

        ultra.lock.acquire()
        try:
            self.assertFalse(other.lock.steal_from_dead(from_pid=os.getpid()))
        finally:
            ultra.lock.release()

    def test_frozen_phantom_lock_is_stolen(self):
        """A phantom lock (byte set, pid 0, no activity) left by a dead owner is recovered."""
        ultra = UltraDict(shared_lock=True)
        ultra.lock.lock_remote[0:1] = b'\x01'

        self.assertTrue(ultra.lock.steal_from_dead(from_pid=0, release=True))
        self.assertEqual(ultra.lock.get_remote_lock(), 0)
        self.assertEqual(ultra.lock.get_remote_pid(), 0)

    def test_live_release_window_is_not_stolen_as_phantom(self):
        """A live holder caught mid-release also shows (byte set, pid 0); any activity
        during the phantom watch means it is not a phantom and must not be stolen."""
        ultra = UltraDict(shared_lock=True)
        ultra.lock.lock_remote[0:1] = b'\x01'

        # The activity is injected into the watch loop's own sleep rather than raced in
        # from a timer thread: a timer has to win against a 100ms watch, and on a loaded
        # runner it can lose, failing the test for scheduling reasons rather than logic.
        real_sleep = time.sleep

        def touch_lock_time_while_watching(seconds):
            real_sleep(seconds)
            ultra.lock.lock_time_remote[:] = int(time.time() * 1000).to_bytes(8, 'little')

        with mock.patch.object(time, 'sleep', touch_lock_time_while_watching):
            self.assertFalse(ultra.lock.steal_from_dead(from_pid=0))
        self.assertEqual(ultra.lock.get_remote_lock(), 1, "lock must be left untouched")

    def _sized_probe(self, path):
        """Stub shm_open with a real fd on `path`, so fstat/close behave like the real thing."""
        import UltraDict2.UltraDict2 as ud

        class FakeShm:
            @staticmethod
            def shm_open(name, flags, mode=0o600):
                return os.open(path, os.O_RDONLY)

        return ud, FakeShm

    def test_wait_until_sized_waits_for_the_creator(self):
        """A segment with no size yet is waited on, not attached to."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'segment')
            open(path, 'wb').close()  # exists, size 0 -- as after shm_open but before ftruncate
            ud, fake = self._sized_probe(path)

            def give_it_a_size():
                with open(path, 'wb') as f:
                    f.write(b'x' * 1000)

            timer = threading.Timer(0.1, give_it_a_size)
            original = ud._posixshmem
            ud._posixshmem = fake
            try:
                timer.start()
                start = time.monotonic()
                ready = ud.UltraDict.wait_until_sized('segment', time.monotonic() + 10)
                elapsed = time.monotonic() - start
            finally:
                ud._posixshmem = original
                timer.cancel()

            self.assertTrue(ready, "a sized segment must be reported as attachable")
            self.assertGreaterEqual(elapsed, 0.05, "returned before the segment had a size")
            self.assertLess(elapsed, 9, "did not notice the segment being sized")

    def test_wait_until_sized_times_out_on_a_dead_creator(self):
        """A creator that died before ftruncate never sizes it, so we give up rather than hang."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'segment')
            open(path, 'wb').close()
            ud, fake = self._sized_probe(path)

            original = ud._posixshmem
            ud._posixshmem = fake
            try:
                with self.assertRaises(UltraDict.Exceptions.CannotAttachSharedMemory):
                    ud.UltraDict.wait_until_sized('segment', time.monotonic() + 0.2)
            finally:
                ud._posixshmem = original

    def test_wait_until_sized_reports_a_missing_segment_as_not_attachable(self):
        """A segment that does not exist must not be attached to.

        Reporting it as attachable is what let a process race the creator into the window between
        its shm_open and its ftruncate, which is the failure this whole check exists to prevent.
        """
        import UltraDict2.UltraDict2 as ud

        class Missing:
            @staticmethod
            def shm_open(name, flags, mode=0o600):
                raise FileNotFoundError(name)

        original = ud._posixshmem
        ud._posixshmem = Missing
        try:
            self.assertFalse(ud.UltraDict.wait_until_sized('nope', time.monotonic() + 10))
        finally:
            ud._posixshmem = original

    @unittest.skipIf(os.name == 'nt', "Windows supplies the size when the mapping is created")
    def test_attaching_to_an_unsized_segment(self):
        """Attach to a segment that exists but has no size yet, deterministically.

        That is exactly the state a creator leaves behind between its shm_open and its ftruncate.
        Racing for the window is a coin flip, so the window is built here instead: create the
        segment, hand it to get_memory, and only give it a size once a waiter should already be
        blocked on it.

        Unfixed, get_memory attaches immediately, fails to mmap the empty segment, and CPython
        unlinks it before re-raising -- so this fails on the exception and, had it not, on the
        segment having been destroyed.
        """
        import _posixshmem

        name = 'ultra_unsized_segment'
        try:
            _posixshmem.shm_unlink('/' + name)
        except FileNotFoundError:
            pass

        fd = _posixshmem.shm_open('/' + name, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        timer = threading.Timer(0.3, lambda: os.ftruncate(fd, 1000))
        memory = None
        try:
            timer.start()
            start = time.monotonic()
            memory = UltraDict.get_memory(create=False, name=name)
            elapsed = time.monotonic() - start

            self.assertGreaterEqual(elapsed, 0.2, "attached before the segment had a size")
            self.assertGreaterEqual(memory.size, 1000)

            # The creator's segment must still be there: a failed attach unlinks it, which is what
            # silently splits one dict into two under the same name.
            probe = _posixshmem.shm_open('/' + name, os.O_RDONLY, 0o600)
            os.close(probe)
        finally:
            timer.cancel()
            if memory is not None:
                memory.close()
            os.close(fd)
            try:
                _posixshmem.shm_unlink('/' + name)
            except FileNotFoundError:
                pass

    def test_concurrent_boot(self):
        """Processes starting together on one name must not see a half-built dict."""
        filename = "tests/concurrent_boot.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(ret.stdout.splitlines()[-1], b"Failed attempts: 0 == 0", self.exec_show_output(ret))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        for i in range(0, len(sys.argv)):
            if sys.argv[i].startswith('-'):
                continue
            if '.' not in sys.argv[i]:
                sys.argv[i] = f"UltraDictTests.{sys.argv[i]}"
    unittest.main()
