import unittest
import subprocess
import os, sys, threading, time

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
        ret = subprocess.run([sys.executable, filepath],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        #print(ret.stdout.decode())
        ret.stdout = ret.stdout.replace(b'\r\n', b'\n');
        self.assertEqual(ret.returncode, 0,
                f"Running '{filepath}' returned exit code '{ret.returncode}' but expected exit code is '0'"
                f"{self.exec_show_output(ret)}")
        return ret

    def exec_show_output(self, ret):
        return f"\n\n{ret}\n\nOutput:\n{ret.stdout.decode()}\n"

    def assertReturnCode(self, ret, target=0):
        return self.assertEqual(ret.returncode, target, self.exec_show_output(ret))

    def test_count(self):
        ultra = UltraDict()
        other = UltraDict(name=ultra.name)

        count = 100
        for i in range(count//2):
            ultra[i] = i

        for i in range(count//2, count):
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

    def test_parameter_passing(self):
        ultra = UltraDict(shared_lock=True, buffer_size=4096*8, full_dump_size=4096*8)
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
        rand_str =   ''.join(random.choice(letters) for i in range(1000))
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
    #def test_longest_name(self):
    #    for i in range(5, 50):
    #        print('Loop ', i)
    #        ultra = UltraDict(name='x' * i)

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux")
    def test_cleanup(self):
        # TODO
        import psutil
        p = psutil.Process()
        file_count = len(p.open_files())
        self.assertEqual(file_count, 0, "file handle count before before tests should be 0")
        ultra = UltraDict(nested={ 1: 1})
        file_count = len(p.open_files())
        self.assertEqual(file_count, 4, "file handle count with one simple UltraDict should be 4")
        del ultra
        file_count = len(p.open_files())
        self.assertEqual(file_count, 0, "file handle count after deleting the UltraDict should be 0 again")
        ultra = UltraDict(nested={ 1: 1}, recurse=True)
        file_count = len(p.open_files())
        self.assertEqual(file_count, 12, "nested file handle count should be 12")
        del ultra
        file_count = len(p.open_files())
        self.assertEqual(file_count, 0, "nested file handle count after deleting UltraDict should be 0 again")

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
        self.assertEqual(ret.stdout.splitlines()[-1], b"{'nested': {'deeper': {0: 2}}}  ==  {'nested': {'deeper': {0: 2}}}", self.exec_show_output(ret))

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

        def touch_lock_time():
            ultra.lock.lock_time_remote[:] = int(time.time() * 1000).to_bytes(8, 'little')

        timer = threading.Timer(0.02, touch_lock_time)
        timer.start()
        try:
            self.assertFalse(ultra.lock.steal_from_dead(from_pid=0))
        finally:
            timer.join()
        self.assertEqual(ultra.lock.get_remote_lock(), 1, "lock must be left untouched")

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
