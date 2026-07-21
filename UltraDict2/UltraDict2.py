#
# UltraDict
#
# A sychronized, streaming Python dictionary that uses shared memory as a backend
#
# Copyright [2022] [Ronny Rentner] [ultradict.code@ronny-rentner.de]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__all__ = ['UltraDict']

import collections
import multiprocessing
import multiprocessing.shared_memory
import multiprocessing.synchronize
import os
import pickle
import shutil
import sys
import threading
import time
import weakref

try:
    # Needed for the shared lock
    import atomics2 as atomics
except ModuleNotFoundError:
    pass

try:
    # Needed to size-check a segment before attaching to it, see wait_until_sized().
    # Private, but it is what multiprocessing.shared_memory itself uses, so it is present
    # wherever SharedMemory is.
    import _posixshmem
except ImportError:
    _posixshmem = None

import logging as log

from . import Exceptions
from .Metrics import Metrics

# Seconds a waiter tolerates no progress before checking whether the lock owner died. It does not
# bound the wait: a live owner is waited on for as long as it keeps working.
DEFAULT_LOCK_TIMEOUT = 5.0

# How long a process attaching to a half-built dict waits for its creator to finish. Creation takes
# microseconds, so this only has to outlast a scheduling hiccup.
READY_TIMEOUT = 10.0
READY_INTERVAL = 0.0005


def remove_shm_from_resource_tracker():
    """
    Monkey-patch multiprocessing.resource_tracker so SharedMemory won't be tracked
    More details at: https://bugs.python.org/issue38119
    """
    # pylint: disable=protected-access, import-outside-toplevel
    # Ignore linting errors in this bug workaround hack
    from multiprocessing import resource_tracker

    def fix_register(name, rtype):
        if rtype == "shared_memory":
            return None
        return resource_tracker._resource_tracker.register(name, rtype)

    resource_tracker.register = fix_register

    def fix_unregister(name, rtype):
        if rtype == "shared_memory":
            return None
        return resource_tracker._resource_tracker.unregister(name, rtype)

    resource_tracker.unregister = fix_unregister
    if "shared_memory" in resource_tracker._CLEANUP_FUNCS:
        del resource_tracker._CLEANUP_FUNCS["shared_memory"]


# Python 3.13+ supports track=False on SharedMemory, so the global (and invasive)
# resource tracker monkey-patch is only needed on older versions.
# More details at: https://bugs.python.org/issue38119
if sys.version_info >= (3, 13):
    shm_track_kwargs = {'track': False}
else:
    shm_track_kwargs = {}
    remove_shm_from_resource_tracker()


class UltraDict(collections.UserDict, dict):
    Exceptions = Exceptions
    log = log

    class RLock(multiprocessing.synchronize.RLock):
        """Not yet used"""

        pass

    class SharedLock:
        """
        Lock stored in shared_memory to provide an additional layer of protection,
        e.g. when using spawned processes.

        Internally uses atomics package of patomics for atomic locking.

        This is needed if you write to the shared memory with independent processes.
        """

        __slots__ = (
            'parent',
            'has_lock',
            'ctx',
            'lock_atomic',
            'lock_remote',
            'pid',
            'pid_bytes',
            'pid_remote',
            'pid_remote_ctx',
            'pid_remote_atomic',
            'next_acquire_parameters',
            'lock_time_remote',
            'thread_lock',
            'owner_tid',
            'default_acquire_parameters',
        )

        # How long a suspected phantom lock must hold perfectly still before it is stolen
        phantom_watch_time = 0.1
        phantom_watch_interval = 0.001

        def __init__(
            self, parent, lock_name, pid_name, time_name='lock_time_remote', lock_timeout: "float | None" = DEFAULT_LOCK_TIMEOUT
        ):
            self.has_lock = 0
            # Applied to every `with lock:` because __enter__ resets to these afterwards. Waiting
            # forever on a holder that died is never useful, so recovery is on by default; set
            # lock_timeout=None to wait indefinitely instead.
            self.default_acquire_parameters = (True, lock_timeout, 0.000001, True) if lock_timeout else ()
            self.next_acquire_parameters = self.default_acquire_parameters

            # Gate for threads of the same process sharing this instance; `has_lock`
            # alone is not thread-safe (check-then-act race)
            self.thread_lock = threading.RLock()
            self.owner_tid = 0

            # `lock_name` contains the name of the attribute that the parent uses
            # to store the memory view on the remote lock, so `self.lock_remote` is
            # referring to a memory view
            self.lock_remote = getattr(parent, lock_name)
            self.pid_remote = getattr(parent, pid_name)
            # Timestamp (ms since epoch) of when the lock was acquired, used to
            # detect pid recycling when stealing from dead processes
            self.lock_time_remote = getattr(parent, time_name)

            self.init_pid()

            try:
                self.ctx = atomics.atomicview(buffer=self.lock_remote[0:1], atype=atomics.BYTES)
                self.pid_remote_ctx = atomics.atomicview(buffer=self.pid_remote[0:4], atype=atomics.BYTES)
            except NameError as e:
                self.cleanup()
                raise e
            self.lock_atomic = self.ctx.__enter__()
            self.pid_remote_atomic = self.pid_remote_ctx.__enter__()

            def after_fork():
                if self.has_lock:
                    raise Exception("Release the SharedLock before you fork the process")

                # After forking, we got a new pid and only the forking thread survives
                self.init_pid()
                self.thread_lock = threading.RLock()
                self.owner_tid = 0

            if sys.platform != 'win32':
                os.register_at_fork(after_in_child=after_fork)

        def init_pid(self):
            self.pid = multiprocessing.current_process().pid
            self.pid_bytes = self.pid.to_bytes(4, 'little')

        def acquire_with_timeout(self, block=True, sleep_time=0.000001, timeout=1.0, steal_after_timeout=False):
            # The block parameter will be ignored
            time_start = None
            blocking_pid = None
            while True:
                try:
                    return self.acquire(block=False, sleep_time=sleep_time)
                except Exceptions.CannotAcquireLock as e:
                    if not time_start:
                        time_start = e.timestamp
                        blocking_pid = e.blocking_pid

                    time_passed = time.monotonic() - time_start

                    if time_passed >= timeout:
                        if steal_after_timeout:
                            # A live holder is not an error, it is just contention, so the timeout
                            # only drives periodic dead-owner recovery and we keep waiting. Our own
                            # process is never stealable (another of our threads holds it and is by
                            # definition alive), and a changed blocking pid means someone else took
                            # or stole the lock meanwhile.
                            if blocking_pid != self.pid and blocking_pid == e.blocking_pid:
                                self.steal_from_dead(from_pid=blocking_pid, release=True)
                            time_start = None
                            blocking_pid = None
                            continue
                        raise Exceptions.CannotAcquireLockTimeout(blocking_pid=e.blocking_pid, timestamp=time_start) from None

        # @profile
        def acquire(self, block=True, sleep_time=0.000001, timeout=None, steal_after_timeout=False):
            if timeout:
                return self.acquire_with_timeout(sleep_time=sleep_time, timeout=timeout, steal_after_timeout=steal_after_timeout)

            # Serialize threads of our own process first
            if not self.thread_lock.acquire(blocking=block):
                raise Exceptions.CannotAcquireLock(blocking_pid=self.pid)

            try:
                # If we already own the lock, just increment our counter
                if self.has_lock:
                    self.has_lock += 1
                    return True

                while True:
                    # We need both, the shared lock to be False and the lock_pid to be 0
                    if self.test_and_inc():
                        # Claim ownership by writing our pid; cmpxchg because a stealer
                        # may legitimately grab a phantom lock (lock set, pid still 0)
                        result = self.pid_remote_atomic.cmpxchg_strong(expected=b'\x00\x00\x00\x00', desired=self.pid_bytes)
                        if result.success:
                            self.has_lock = 1
                            self.owner_tid = threading.get_ident()
                            self.lock_time_remote[:] = int(time.time() * 1000).to_bytes(8, 'little')
                            return True
                        # A stealer raced us between test_and_inc() and the pid write
                        # and now owns the lock; retry like anyone else

                    # If set to 0, we practically have a busy wait
                    if sleep_time:
                        # On Python < 3.10, this smallest possible time is actually rather big,
                        #  maybe around 10 ms, depending on your CPU.
                        time.sleep(sleep_time)

                    if not block:
                        raise Exceptions.CannotAcquireLock(blocking_pid=self.get_remote_pid())
            except BaseException:
                self.thread_lock.release()
                raise

        # @profile
        def test_and_inc(self):
            old = self.lock_atomic.exchange(b'\x01')
            if old != b'\x00':
                # Oops, someone else was faster than us
                return False
            return True

        # @profile
        def test_and_dec(self):
            old = self.lock_atomic.exchange(b'\x00')
            if old != b'\x01':
                raise Exception("Failed to release lock")
            return True

        # @profile
        def release(self, *args):
            # log.debug("Release lock, lock={}", self.has_lock)
            if self.has_lock > 0 and self.owner_tid == threading.get_ident():
                owner = int.from_bytes(self.pid_remote, 'little')
                if owner != self.pid:
                    raise Exception(f"Our lock for pid {self.pid} was stolen by pid {owner}")
                self.has_lock -= 1
                # Last local lock released, release shared lock
                if not self.has_lock:
                    self.owner_tid = 0
                    self.pid_remote[:] = b'\x00\x00\x00\x00'
                    self.test_and_dec()
                self.thread_lock.release()
                # log.debug("Relased lock, lock={} pid_remote={}", self.has_lock, int.from_bytes(self.pid_remote, 'little'))
                return True

            return False

        def reset(self):
            # Risky
            self.lock_remote[:] = b'\x00'
            self.pid_remote[:] = b'\x00\x00\x00\x00'
            self.has_lock = 0
            self.owner_tid = 0

        def reset_acquire_parameters(self):
            self.next_acquire_parameters = self.default_acquire_parameters

        def steal(self, from_pid=0, release=False):
            if self.has_lock:
                raise Exception("Cannot steal the lock because we have already acquired it. Use release() to release the lock.")

            # log.debug(f'Stealing from_pid={from_pid}, remote_pid={self.get_remote_pid()}')

            # It's not locked, so nothing to steal from
            if not self.get_remote_lock():
                return False

            # Someone else has stolen the lock
            if from_pid != self.get_remote_pid():
                return False

            # Take the local thread gate first so release() stays balanced
            if not self.thread_lock.acquire(blocking=False):
                # Another thread of our own process is interacting with the lock
                return False

            # Stealing the lock means actually just putting our pid into the shared memory overwriting the other pid.
            # This can go wrong if the lock owner is actually still alive and working.
            result = self.pid_remote_atomic.cmpxchg_strong(expected=from_pid.to_bytes(4, 'little'), desired=self.pid_bytes)
            if result.success:
                self.has_lock = 1
                self.owner_tid = threading.get_ident()
                self.lock_time_remote[:] = int(time.time() * 1000).to_bytes(8, 'little')
                if release:
                    self.release()
            else:
                self.thread_lock.release()
            return result.success

        def steal_from_dead(self, from_pid=0, release=False):
            """Check if from_pid is actually a dead process and if yes, steal the lock from it.
            Optionally, the lock can be directly released after stealing it.

            Returns True if the lock was stolen. A holder that is still alive is not an error,
            it just means there is nothing to recover: we return False so the caller can keep
            waiting for it to finish.
            """

            # Phantom lock: the owner died between setting the lock byte and writing its
            # pid (or between clearing the pid and the lock byte on release). There is no
            # process to check. A live holder caught mid-release shows the same state
            # (byte set, pid 0) for a moment though, and stealing from it double-releases
            # the lock. A true phantom is frozen: while the byte is stuck set nobody can
            # acquire, so pid, byte and timestamp cannot change. Watch the state and only
            # steal if it holds still the whole time.
            # ponytail: a holder preempted longer than the watch inside the two-write
            # release window is still misread as a phantom; raise the watch time if that
            # ever shows up in the wild
            if from_pid == 0:
                lock_time = bytes(self.lock_time_remote)
                for _ in range(int(self.phantom_watch_time / self.phantom_watch_interval)):
                    time.sleep(self.phantom_watch_interval)
                    if self.get_remote_pid() != 0 or not self.get_remote_lock() or bytes(self.lock_time_remote) != lock_time:
                        return False
                return self.steal(from_pid=0, release=release)

            try:
                import psutil
            except ModuleNotFoundError:
                raise Exceptions.MissingDependency("Install `psutil` Python package to use shared_lock=True") from None
            # No process must exist anymore with the from_pid or it must at least be dead (ie. zombie status)
            try:
                p = psutil.Process(from_pid)
                if p and p.is_running() and p.status() not in [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD]:
                    # The pid may have been recycled: if the process started after the
                    # lock was acquired, it cannot be the actual lock owner
                    lock_time_ms = int.from_bytes(self.lock_time_remote, 'little')
                    if not (lock_time_ms and p.create_time() * 1000 > lock_time_ms):
                        # Owner is alive and really is the owner, so it is simply still working.
                        return False
            except psutil.NoSuchProcess:
                # If the process is already gone, we cannot find information about it.
                # It will be safe to steal the lock.
                pass

            return self.steal(from_pid=from_pid, release=release)

        def status(self):
            return {
                'has_lock': self.has_lock,
                'lock_remote': int.from_bytes(self.lock_remote, 'little'),
                'pid': self.pid,
                'pid_remote': int.from_bytes(self.pid_remote, 'little'),
            }

        def print_status(self, status=None):
            import pprint

            if not status:
                status = self.status()
            pprint.pprint(status)

        def cleanup(self):
            if hasattr(self, 'ctx'):
                self.ctx.__exit__(None, None, None)
                del self.ctx
            if hasattr(self, 'lock_atomic'):
                del self.lock_atomic
            if hasattr(self, 'pid_remote_ctx'):
                self.pid_remote_ctx.__exit__(None, None, None)
                del self.pid_remote_ctx
            if hasattr(self, 'pid_remote_atomic'):
                del self.pid_remote_atomic
            del self.lock_remote
            del self.pid_remote
            del self.lock_time_remote
            del self.pid_bytes
            del self.pid

        def get_remote_pid(self):
            return int.from_bytes(self.pid_remote, 'little')

        def get_remote_lock(self):
            return int.from_bytes(self.lock_remote, 'little')

        def __repr__(self):
            return f"{self.__class__.__name__} @{hex(id(self))} lock_remote={int.from_bytes(self.lock_remote, 'little')}, has_lock={self.has_lock}, pid={self.pid}), pid_remote={int.from_bytes(self.pid_remote, 'little')}"

        def __enter__(self):
            self.acquire(*self.next_acquire_parameters)
            self.reset_acquire_parameters()
            return self

        def __exit__(self, type, value, traceback):
            self.release()
            # Make sure exceptions are not ignored
            return False

        def __call__(self, block=True, timeout=None, sleep_time=0.000001, steal_after_timeout=False):
            self.next_acquire_parameters = (block, timeout, sleep_time, steal_after_timeout)

            return self

    __slots__ = (
        'name',
        'control',
        'buffer',
        'buffer_size',
        'lock',
        'shared_lock',
        'lock_timeout',
        'update_stream_position',
        'update_stream_position_remote',
        'full_dump_counter',
        'full_dump_memory',
        'full_dump_size',
        'serializer',
        'lock_pid_remote',
        'lock_remote',
        'full_dump_counter_remote',
        'full_dump_static_size_remote',
        'shared_lock_remote',
        'ready_remote',
        'recurse',
        'recurse_remote',
        'recurse_register',
        'full_dump_memory_name_remote',
        'lock_time_remote',
        'data',
        'closed',
        'auto_unlink',
        'finalizer',
    )

    def __init__(
        self,
        *args,
        name=None,
        create=None,
        buffer_size=10_000,
        serializer=pickle,
        shared_lock=None,
        full_dump_size=None,
        auto_unlink=None,
        recurse=None,
        recurse_register=None,
        lock_timeout: "float | None" = DEFAULT_LOCK_TIMEOUT,
        **kwargs,
    ):
        # pylint: disable=too-many-branches, too-many-statements

        # On win32, only multiples of 4k are allowed
        if sys.platform == 'win32':
            buffer_size = -(buffer_size // -4096) * 4096
            if full_dump_size:
                full_dump_size = -(full_dump_size // -4096) * 4096

        if buffer_size >= 2**32:
            raise ValueError(f"buffer_size must be smaller than 2**32, got {buffer_size}")

        if recurse and serializer != pickle:
            raise ValueError("recurse=True requires the pickle serializer")

        self.data = {}

        # Local position, ie. the last position we have processed from the stream
        self.update_stream_position = 0

        # Local version counter for the full dumps, ie. if we find a higher version
        # remote, we need to load a full dump
        self.full_dump_counter = 0

        # Metrics, local to this instance and never shared through the control block.
        # ponytail: plain += relies on the GIL, needs a lock under free-threading
        self.item_size_min = None
        self.item_size_max = None
        self.item_size_sum = 0
        self.item_size_count = 0
        self.full_dump_length = None
        self.buffer_full_forced_dump = 0
        self.full_dump_memory_full = 0
        self.full_dump_too_fast = 0

        self.closed = False
        self.auto_unlink = auto_unlink

        # Small 1000 bytes of shared memory where we store the runtime state
        # of our update stream
        self.control = self.get_memory(create=create, name=name, size=1000)
        self.name = self.control.name

        def finalize(weak_self, name):
            # log.debug('Finalize', name)
            resolved_self = weak_self()
            if resolved_self is not None:
                # log.debug('Weakref is intact, closing')
                resolved_self.close(from_finalizer=True)
            # log.debug('Finalized')

        self.finalizer = weakref.finalize(self, finalize, weakref.ref(self), self.name)

        self.init_remotes()

        # Creating the control memory publishes it, so another process can attach to a dict that is
        # still being built: its metadata is zeroed and its buffer does not exist yet. Wait for the
        # creator to signal it is done before reading any of that.
        created_by_us = hasattr(self.control, 'created_by_ultra')
        if not created_by_us:
            self.wait_until_ready()

        self.serializer = serializer

        # Actual stream buffer that contains marshalled data of changes to the dict
        self.buffer = self.get_memory(create=create, name=self.name + '_memory', size=buffer_size)
        # TODO: Raise exception if buffer size mismatch
        self.buffer_size = self.buffer.size

        self.full_dump_memory = None

        # Dynamic full dump memory handling
        # Warning: Issues on Windows when the process ends that has created the full dump memory
        self.full_dump_size = None

        if created_by_us:
            if auto_unlink is None:
                self.auto_unlink = True

            if recurse:
                self.recurse_remote[0:1] = b'1'

            if shared_lock:
                self.shared_lock_remote[0:1] = b'1'

            # We created the control memory, thus let's check if we need to create the
            # full dump memory as well
            if full_dump_size:
                self.full_dump_size = full_dump_size
                self.full_dump_static_size_remote[:] = full_dump_size.to_bytes(4, 'little')

                self.full_dump_memory = self.get_memory(create=True, name=self.name + '_full', size=full_dump_size)
                self.full_dump_memory_name_remote[:] = self.full_dump_memory.name.encode('utf-8').ljust(255)

            # Published last: everything an attaching process reads is now in place.
            self.ready_remote[0:1] = b'1'

        # We just attached to the existing control
        else:
            # TODO: Detect configuration mismatch and raise an exception

            # Check if we have a fixed size full dump memory
            size = int.from_bytes(self.full_dump_static_size_remote, 'little')

            # Check if shared_lock parameter was not set to inconsistent value
            shared_lock_remote = self.shared_lock_remote[0:1] == b'1'
            if shared_lock is None:
                shared_lock = shared_lock_remote
            elif shared_lock != shared_lock_remote:
                raise Exceptions.ParameterMismatch(
                    f"shared_lock={shared_lock} was set but the creator has used shared_lock={shared_lock_remote}"
                )

            # Check if recurse parameter was not set to inconsistent value
            recurse_remote = self.recurse_remote[0:1] == b'1'
            if recurse is None:
                recurse = recurse_remote
            elif recurse != recurse_remote:
                raise Exceptions.ParameterMismatch(f"recure={recurse} was set but the creator has used recurse={recurse_remote}")

            # Got existing size of full dump memory, that must mean it's static size
            # and we should attach to it
            if size > 0:
                self.full_dump_size = size
                self.full_dump_memory = self.get_memory(create=False, name=self.name + '_full')

        # Local lock for all processes and threads created by the same interpreter
        if shared_lock:
            try:
                self.lock = self.SharedLock(self, 'lock_remote', 'lock_pid_remote', lock_timeout=lock_timeout)
            except NameError:
                # self.cleanup()
                raise Exceptions.MissingDependency("Install `atomics2` Python package to use shared_lock=True") from None
        else:
            self.lock = multiprocessing.RLock()

        self.shared_lock = shared_lock
        self.lock_timeout = lock_timeout

        # Parameters that could be read from remote if we are connecting to an existing UltraDict
        self.recurse = recurse

        # In recurse mode, we must ensure a recurse register
        if self.recurse:
            # Must be either the name of an UltraDict as a string or an UltraDict instance
            if recurse_register is not None:
                if type(recurse_register) is str:
                    self.recurse_register = UltraDict(name=recurse_register)
                elif type(recurse_register) is UltraDict:
                    self.recurse_register = recurse_register
                else:
                    raise Exception("Bad type for recurse_register")

            # If no register was defined, we should create one
            else:
                self.recurse_register = UltraDict(
                    name=f'{self.name}_register',
                    recurse=False,
                    auto_unlink=False,
                    shared_lock=self.shared_lock,
                    lock_timeout=lock_timeout,
                )
                # The register should not run its own finalizer if we need it later for unlinking our nested children
                if self.auto_unlink:
                    self.recurse_register.finalizer.detach()
                    # log.debug("Created recurse register with name={}", self.recurse_register.name)

        else:
            self.recurse_register = None

        super().__init__(*args, **kwargs)

        # Load all data from shared memory
        self.apply_update()

        if sys.platform == 'win32':
            if not shared_lock:
                log.warning('You are running on win32, potentially without locks. Consider setting shared_lock=True')

        # if auto_unlink:
        #    atexit.register(self.unlink)
        # else:
        #    atexit.register(self.cleanup)

        # log.debug("Initialized", self.name)

    def __del__(self):
        # log.debug("__del__", self.name)
        self.close()
        # if hasattr(self, 'recurse') and self.recurse:
        #    #log.debug("Close recurse register")
        #    self.recurse_register.close()
        #    del self.recurse_register

    def wait_until_ready(self, timeout=READY_TIMEOUT, interval=READY_INTERVAL):
        """Block until the process that created this dict has finished initialising it.

        Shared memory is published by the act of creating it, so a dict can be attached to
        while its creator is still filling in the control block and creating the stream
        buffer. Reading it before then sees zeroed metadata, which looks exactly like a dict
        that was created with different parameters.

        A creator that died mid-initialisation never sets the flag, so we time out rather than
        attach to a dict that will never be complete.
        """
        deadline = time.monotonic() + timeout
        while self.ready_remote[0:1] != b'1':
            if time.monotonic() >= deadline:
                raise Exceptions.CannotAttachSharedMemory(
                    f"Timed out after {timeout}s waiting for the creator of '{self.name}' to finish initialising it"
                )
            time.sleep(interval)

    def init_remotes(self):
        # Memoryviews to the right buffer position in self.control
        self.update_stream_position_remote = self.control.buf[0:4]
        self.lock_pid_remote = self.control.buf[4:8]
        self.lock_remote = self.control.buf[8:10]
        self.full_dump_counter_remote = self.control.buf[10:14]
        self.full_dump_static_size_remote = self.control.buf[14:18]
        self.shared_lock_remote = self.control.buf[18:19]
        self.recurse_remote = self.control.buf[19:20]
        self.full_dump_memory_name_remote = self.control.buf[20:275]
        self.lock_time_remote = self.control.buf[275:283]
        self.ready_remote = self.control.buf[283:284]

    def del_remotes(self):
        """
        Delete all instance attributes whose name ends with '_remote' from
        the instance for cleanup. This shall ensure there are no
        reference left to shared memory views so proper cleanup can happen.
        """
        remotes = [r for r in dir(self) if r.endswith('_remote')]
        for r in remotes:
            if hasattr(self, r):
                delattr(self, r)

    def __reduce__(self):
        from functools import partial

        return (partial(self.__class__, name=self.name, auto_unlink=self.auto_unlink, recurse_register=self.recurse_register), ())

    @staticmethod
    def wait_until_sized(name, deadline):
        """Report whether `name` can be attached to, waiting while it is still being set up.

        Returns True once the segment exists and has a size, False if it does not exist, in
        which case the caller must create it instead of attaching. Answering that question is
        the whole point: attaching to a segment that exists but has no size yet fails to mmap,
        and CPython unlinks the segment before re-raising -- destroying the creator's memory
        and leaving the next process to create a second, unrelated segment under the same name.

        POSIX publishes a name in shm_open but only gives it a size in the ftruncate that
        follows, so that window is real, and 'it did not exist a moment ago' is not a safe
        reason to try attaching: the creator may have got there in between. A size only ever
        goes from zero to its final value, so once seen there is nothing left to race against.

        Windows has no such window, as the size is supplied when the mapping is created, so
        there we always report True and let SharedMemory decide.
        """
        if _posixshmem is None:
            return True

        while True:
            try:
                fd = _posixshmem.shm_open('/' + name, os.O_RDONLY, mode=0o600)
            except FileNotFoundError:
                return False
            except (PermissionError, ValueError):
                # Cannot inspect it; let SharedMemory report whatever the real problem is
                return True

            try:
                if os.fstat(fd).st_size:
                    return True
            finally:
                os.close(fd)

            if time.monotonic() >= deadline:
                raise Exceptions.CannotAttachSharedMemory(
                    f"Timed out waiting for '{name}' to be given a size by the process creating it"
                )
            time.sleep(READY_INTERVAL)

    @staticmethod
    def get_memory(*, create=True, name=None, size=0):
        """
        Attach an existing SharedMemory object with `name`.

        If `create` is True, create the object if it does not exist.
        """
        if create and size <= 0:
            raise ValueError(f"Cannot create memory with size={size}")

        deadline = time.monotonic() + READY_TIMEOUT
        while True:
            # Only attach once it is known to be attachable. Trying anyway when it does not exist
            # yet races the creator into the window between its shm_open and its ftruncate.
            if name and UltraDict.wait_until_sized(name, deadline):
                try:
                    memory = multiprocessing.shared_memory.SharedMemory(name=name, **shm_track_kwargs)
                    # log.debug('Attached shared memory: ', memory.name)

                    if create:
                        memory.close()
                        raise Exceptions.AlreadyExists(f"Cannot create memory '{name}' because it already exists")

                    return memory
                except FileNotFoundError:
                    # Unlinked between the check and the attach; fall through and create it
                    pass

            # No existing memory found
            if create or create is None:
                try:
                    memory = multiprocessing.shared_memory.SharedMemory(create=True, size=size, name=name, **shm_track_kwargs)
                except FileExistsError:
                    if create:
                        raise Exceptions.AlreadyExists(f"Cannot create memory '{name}' because it already exists") from None
                    # We lost the creation race against another process; attach instead
                    continue
                except (OSError, OverflowError) as e:
                    # Anything other than the collision above means the host could not back
                    # the segment. Errno is not dependable here: an exhausted Windows paging
                    # file reports EINVAL, not ENOSPC, so the type is the filter. OverflowError
                    # joins it because a size beyond ssize_t never reaches the OS at all: on a
                    # 32-bit build mmap rejects it, which is a host limit like any other.
                    raise Exceptions.CannotCreateSharedMemory(
                        f"Cannot create shared memory of {size} bytes: {e}. The host is out of shared "
                        "memory or file descriptors. On Linux, grow /dev/shm (eg. `docker run "
                        "--shm-size=1g`) or look for segments left behind by crashed processes; on "
                        "Windows, grow the paging file. Setting full_dump_size stops a new segment "
                        "being allocated for every dump."
                    ) from e
                # Remember that we have created this memory
                memory.created_by_ultra = True
                # log.debug('Created shared memory: ', memory.name)

                return memory

            raise Exceptions.CannotAttachSharedMemory(f"Could not get memory '{name}'")

    # @profile
    def dump(self):
        """Dump the full dict into shared memory"""

        with self.lock:
            old = bytes(self.full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')

            self.apply_update()

            marshalled = self.serializer.dumps(self.data)
            length = len(marshalled)

            # If we don't have a fixed size, let's create full dump memory dynamically
            # TODO: This causes issues on Windows because the memory is not persistant
            #       Maybe switch to mmaped file?
            if self.full_dump_size and self.full_dump_memory:
                full_dump_memory = self.full_dump_memory
            else:
                # Dynamic full dump memory
                full_dump_memory = self.get_memory(create=True, size=length + 6)

            # log.debug("Full dump memory: ", full_dump_memory)

            if length + 6 > full_dump_memory.size:
                self.full_dump_memory_full += 1
                raise Exceptions.FullDumpMemoryFull(
                    f'Full dump memory too small for full dump: needed={length + 6} got={full_dump_memory.size}'
                )

            # Write header, 6 bytes
            # First byte is FF byte
            full_dump_memory.buf[0:1] = b'\xff'
            # Then comes 4 bytes of length of the body
            full_dump_memory.buf[1:5] = length.to_bytes(4, 'little')
            # Then another FF bytes, end of header
            full_dump_memory.buf[5:6] = b'\xff'

            # Write body
            full_dump_memory.buf[6 : 6 + length] = marshalled

            # On Windows, if we close it, it cannot be read anymore by anyone else.
            if not self.full_dump_size and sys.platform != 'win32':
                full_dump_memory.close()

            # TODO: There's a slight chance of something going wrong when we first update
            #       the remote memory name and then the counter.

            # Only after we have filled the new full dump memory with the marshalled data,
            # we update the remote name so other users can find it
            if not (self.full_dump_size and self.full_dump_memory):
                self.full_dump_memory_name_remote[:] = full_dump_memory.name.encode('utf-8').ljust(255)

            self.full_dump_length = length
            current = int.from_bytes(self.full_dump_counter_remote, 'little')
            # Remote first: raising in between with only the local counter bumped would put
            # us permanently ahead of everyone, and every reload guard compares local against
            # remote, so this instance would stop seeing peer updates for good
            self.full_dump_counter_remote[:] = int(current + 1).to_bytes(4, 'little')
            self.full_dump_counter += 1

            # Reset the stream position to zero as we have
            # just provided a fresh new full dump
            self.update_stream_position = 0
            self.update_stream_position_remote[:] = b'\x00\x00\x00\x00'

            # log.info("Dumped dict with {} elements to {} bytes, remote_counter={}", len(self), len(marshalled), current+1)

            # On Windows, we need to keep a reference to the full dump memory,
            # otherwise it's destoryed. Taken before the unlink below, which can raise:
            # without the reference that would destroy the dump we just published.
            self.full_dump_memory = full_dump_memory

            # If the old full dump memory was dynamically created, delete it. The dump is
            # already published at this point, so failing to reap the old segment costs a
            # leaked segment and must not fail the write.
            if old and old != full_dump_memory.name and not self.full_dump_size:
                self.unlink_by_name(old, ignore_errors=True)

            return full_dump_memory

    def get_full_dump_memory(self, max_retry=3, retry=0):
        """
        Attach to the full dump memory.

        Retry if necessary for a low number of times. It could happen that the full
        dump memory was removed because a new full dump was created before we had the
        chance to read the old full dump.

        """
        try:
            name = bytes(self.full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')
            # log.debug("Full dump name={}", name)
            if len(name) < 1:
                raise Exceptions.CorruptedStream("Full dump memory name is empty")
            return self.get_memory(create=False, name=name)
        except Exceptions.CannotAttachSharedMemory as e:
            if retry < max_retry:
                return self.get_full_dump_memory(max_retry=max_retry, retry=retry + 1)
            elif retry == max_retry:
                # On the last retry, let's use a lock to ensure we can safely import the dump
                with self.lock:
                    return self.get_full_dump_memory(max_retry=max_retry, retry=retry + 1)
            else:
                raise e

    # @profile
    def load(self, force=False, max_retry=3, retry=0):
        """
        Opportunistacally load full dumps without any locking.

        There is a rare case where a full dump is replaced with a newer full dump while
        we didn't have the chance to load the old one. In this case, we just retry.
        """
        full_dump_counter = int.from_bytes(self.full_dump_counter_remote, 'little')
        # log.debug("Loading full dump local_counter={} remote_counter={}", self.full_dump_counter, full_dump_counter)
        try:
            if force or (self.full_dump_counter < full_dump_counter):
                if self.full_dump_size and self.full_dump_memory:
                    full_dump_memory = self.full_dump_memory
                else:
                    # Retry if necessary
                    full_dump_memory = self.get_full_dump_memory()

                buf = full_dump_memory.buf
                pos = 0

                # Read header
                # The first byte should be a FF byte to introduce the header
                if bytes(buf[pos : pos + 1]) != b'\xff':
                    raise Exceptions.CorruptedStream("Full dump header start marker missing")
                pos += 1
                # Then comes 4 bytes of length
                length = int.from_bytes(bytes(buf[pos : pos + 4]), 'little')
                if length <= 0:
                    raise Exceptions.CorruptedStream(f"Full dump length invalid: {(self.status(), full_dump_memory, len(buf))}")
                pos += 4
                # log.debug("Found update, pos={} length={}", pos, length)
                if bytes(buf[pos : pos + 1]) != b'\xff':
                    raise Exceptions.CorruptedStream("Full dump header end marker missing")
                pos += 1
                # Unserialize the update data, we expect a tuple of key and value
                self.data = self.serializer.loads(bytes(buf[pos : pos + length]))
                self.full_dump_counter = full_dump_counter
                self.update_stream_position = 0

                if full_dump_memory is not self.full_dump_memory:
                    if sys.platform == 'win32':
                        # Cannot close on Windows or the memory is destroyed; keep only
                        # the newest handle so the old one gets garbage collected
                        self.full_dump_memory = full_dump_memory
                    else:
                        full_dump_memory.close()
            else:
                raise Exception("Cannot load full dump, no new data available")
        except AssertionError as e:
            full_dump_delta = int.from_bytes(self.full_dump_counter_remote, 'little') - self.full_dump_counter
            if full_dump_delta > 1:
                # If more than one new full dump was created during the time we were trying to load one full dump
                # it can happen that our full dump has just disappeared
                if retry < max_retry:
                    return self.load(force=True, max_retry=max_retry, retry=retry + 1)
                elif retry == max_retry:
                    # On the last retry, take the lock so nobody can dump while we read
                    with self.lock:
                        return self.load(force=True, max_retry=max_retry, retry=retry + 1)
                raise Exceptions.FullDumpsTooFast(
                    f"Full dumps too fast, gave up loading after {max_retry} retries "
                    f"full_dump_counter={self.full_dump_counter} "
                    f"full_dump_counter_remote={int.from_bytes(self.full_dump_counter_remote, 'little')}. "
                    "Consider increasing buffer_size."
                )
            self.print_status()
            raise e

    # @profile
    def append_update(self, key, item, delete=False):
        """Append dict changes to shared memory stream"""

        # If mode is 0, it means delete the key from the dict
        # If mode is 1, it means update the key
        # mode = not delete
        marshalled = self.serializer.dumps((not delete, key, item))
        length = len(marshalled)

        with self.lock:
            start_position = int.from_bytes(self.update_stream_position_remote, 'little')
            # 6 bytes for the header
            end_position = start_position + length + 6
            # log.debug("Update start from={} len={}", start_position, length)
            if end_position > self.buffer_size:
                self.buffer_full_forced_dump += 1
                # log.debug("Buffer is full")

                # todo: is is necessary? apply_update() is also done inside dump()
                self.apply_update()

                # Nothing goes to the stream on this path, dump() publishes self.data
                # instead, so the change has to be applied before dumping. Snapshot what
                # we are replacing first, taken after apply_update() so it matches what
                # our peers see, and put it back if the dump does not happen: a change
                # kept only in our own copy is one no peer will ever hear about.
                had_key = key in self.data
                old_item = self.data.get(key)
                if delete:
                    self.data.__delitem__(key)
                else:
                    self.data.__setitem__(key, item)

                try:
                    self.dump()
                except Exception:
                    if had_key:
                        self.data.__setitem__(key, old_item)
                    else:
                        self.data.pop(key, None)
                    raise
            else:
                # Applied before publishing: the buffer write below is a bounds-checked
                # slice assignment that cannot fail, while an unhashable key must still
                # fail here rather than emit a frame every peer would choke on
                if delete:
                    self.data.__delitem__(key)
                else:
                    self.data.__setitem__(key, item)

                marshalled = b'\xff' + length.to_bytes(4, 'little') + b'\xff' + marshalled

                # Write body with the real data
                self.buffer.buf[start_position:end_position] = marshalled

                # Inform others about it
                self.update_stream_position = end_position
                self.update_stream_position_remote[:] = end_position.to_bytes(4, 'little')
                # log.debug("Update end to={} buffer_size={} ", end_position, self.buffer_size)

            # Only writes that actually landed are worth measuring
            self.item_size_sum += length
            self.item_size_count += 1
            if self.item_size_min is None or length < self.item_size_min:
                self.item_size_min = length
            if self.item_size_max is None or length > self.item_size_max:
                self.item_size_max = length

    # @profile
    def apply_update(self, max_retry=3, retry=0):
        """Opportunistically apply dict changes from shared memory stream without any locking."""

        if retry > max_retry:
            raise Exceptions.FullDumpsTooFast(
                f"Full dumps too fast, gave up applying updates after {max_retry} retries "
                f"full_dump_counter={self.full_dump_counter} "
                f"full_dump_counter_remote={int.from_bytes(self.full_dump_counter_remote, 'little')}. "
                "Consider increasing buffer_size."
            )

        if self.full_dump_counter < int.from_bytes(self.full_dump_counter_remote, 'little'):
            self.load(force=True)

        if self.update_stream_position < int.from_bytes(self.update_stream_position_remote, 'little'):
            # Remember start position in the update stream
            pos = self.update_stream_position
            # log.debug("Apply update: stream position own={} remote={} full_dump_counter={}", pos, int.from_bytes(self.update_stream_position_remote, 'little'), self.full_dump_counter)

            try:
                # Iterate over all updates until the start of the last update
                while pos < int.from_bytes(self.update_stream_position_remote, 'little'):
                    # Read header
                    # The first byte should be a FF byte to introduce the header
                    if bytes(self.buffer.buf[pos : pos + 1]) != b'\xff':
                        raise Exceptions.CorruptedStream(f"Stream header start marker missing at pos={pos}")
                    pos += 1
                    # Then comes 4 bytes of length
                    length = int.from_bytes(bytes(self.buffer.buf[pos : pos + 4]), 'little')
                    pos += 4
                    # log.debug("Found update, update_stream_position={} length={}", self.update_stream_position, length + 6)
                    if bytes(self.buffer.buf[pos : pos + 1]) != b'\xff':
                        raise Exceptions.CorruptedStream(f"Stream header end marker missing at pos={pos}")
                    pos += 1
                    # Unserialize the update data, we expect a tuple of key and value
                    mode, key, value = self.serializer.loads(bytes(self.buffer.buf[pos : pos + length]))
                    # Update or local dict cache (in our parent)
                    if mode:
                        self.data.__setitem__(key, value)
                    else:
                        self.data.__delitem__(key)
                    pos += length
                    # Remember that we have applied the update
                    self.update_stream_position = pos

                # A dump() may have reset the stream while we were replaying it without
                # a lock; frames we just applied could then be stale. Detect it via the
                # dump counter (always incremented before the stream reset) and reload.
                if self.full_dump_counter < int.from_bytes(self.full_dump_counter_remote, 'little'):
                    return self.apply_update(max_retry=max_retry, retry=retry + 1)
            except (AssertionError, pickle.UnpicklingError) as e:
                # It can happen that a slow process is not fast enough reading the stream and some
                # other process already got around overwriting the current position. It is possible to
                # recover from this situation if and only if a new, fresh full dump exists that can be loaded.
                if self.full_dump_counter < int.from_bytes(self.full_dump_counter_remote, 'little'):
                    log.warning(
                        f"Full dumps too fast full_dump_counter={self.full_dump_counter} full_dump_counter_remote={int.from_bytes(self.full_dump_counter_remote, 'little')}. Consider increasing buffer_size."
                    )
                    self.full_dump_too_fast += 1
                    return self.apply_update(max_retry=max_retry, retry=retry + 1)

                # As a last resort, let's get a lock. This way we are safe but slow.
                with self.lock:
                    if self.full_dump_counter < int.from_bytes(self.full_dump_counter_remote, 'little'):
                        log.warning(
                            f"Full dumps too fast full_dump_counter={self.full_dump_counter} full_dump_counter_remote={int.from_bytes(self.full_dump_counter_remote, 'little')}. Consider increasing buffer_size."
                        )
                        self.full_dump_too_fast += 1
                        return self.apply_update(max_retry=max_retry, retry=retry + 1)

                raise e

    def update(self, other=None, *args, **kwargs):
        # pylint: disable=arguments-differ, keyword-arg-before-vararg

        # The original signature would be `def update(self, other=None, /, **kwargs)` but
        # this is not possible with Cython. *args will just be ignored.

        if other is not None:
            for k, v in other.items() if isinstance(other, collections.abc.Mapping) else other:
                self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def __ior__(self, other):
        # UserDict merges straight into self.data, which publishes nothing, so peers
        # never see it and the next full dump load throws it away
        self.update(other.data if isinstance(other, UltraDict) else other)
        return self

    def __delitem__(self, key):
        # log.debug("__delitem__ {}", key)
        with self.lock:
            self.apply_update()

            # Fail before publishing, or peers replay a delete for a key nobody has
            if key not in self.data:
                raise KeyError(key)

            # append_update() updates our local copy once the change is published
            self.append_update(key, b'', delete=True)

    def __setitem__(self, key, item):
        # log.debug("__setitem__ {}, {}", key, item)
        with self.lock:
            self.apply_update()

            if self.recurse:
                assert type(self.recurse_register) is UltraDict, "recurse_register must be an UltraDict instance"

                if type(item) is dict:
                    # TODO: Use parent's buffer with a namespace prefix?
                    item = UltraDict(
                        item,
                        recurse=True,
                        recurse_register=self.recurse_register,
                        auto_unlink=False,
                        shared_lock=self.shared_lock,
                        buffer_size=self.buffer_size,
                        full_dump_size=self.full_dump_size,
                        lock_timeout=self.lock_timeout,
                    )

                    if item.name not in self.recurse_register.data:
                        self.recurse_register[item.name] = True

            # Append the update to the update stream, which also updates our local copy
            self.append_update(key, item)

    def __getitem__(self, key):
        # log.debug("__getitem__ {}", key)
        self.apply_update()
        return self.data[key]

    # deprecated in Python 3
    def has_key(self, key):
        self.apply_update()
        return key in self.data

    def __eq__(self, other):
        self.apply_update()
        if isinstance(other, UltraDict):
            other.apply_update()
            other = other.data
        return self.data == other

    def __contains__(self, key):
        self.apply_update()
        return key in self.data

    def __len__(self):
        self.apply_update()
        return len(self.data)

    def __iter__(self):
        self.apply_update()
        return iter(self.data)

    def __repr__(self):
        try:
            self.apply_update()
        except Exceptions.AlreadyClosed:
            # If something goes wrong during the update, let's ignore it and still return a representation
            # TODO: Maybe somehow add a stale update warning?
            pass
        return self.data.__repr__()

    def status(self):
        """Internal debug helper to get the control state variables"""
        ret = {attr: getattr(self, attr) for attr in self.__slots__ if hasattr(self, attr) and attr != 'data'}

        ret['update_stream_position_remote'] = int.from_bytes(self.update_stream_position_remote, 'little')
        ret['lock_pid_remote'] = int.from_bytes(self.lock_pid_remote, 'little')
        ret['lock_remote'] = int.from_bytes(self.lock_remote, 'little')
        ret['shared_lock_remote'] = self.shared_lock_remote[0:1] == b'1'
        ret['recurse_remote'] = self.recurse_remote[0:1] == b'1'
        ret['lock'] = self.lock
        ret['full_dump_counter_remote'] = int.from_bytes(self.full_dump_counter_remote, 'little')
        ret['full_dump_memory_name_remote'] = bytes(self.full_dump_memory_name_remote).decode('utf-8').strip('\x00').strip()

        return ret

    def print_status(self, status=None, stderr=False):
        """Internal debug helper to pretty print the control state variables"""
        import pprint

        if not status:
            status = self.status()
        pprint.pprint(status, stream=sys.stderr if stderr else sys.stdout)

    def get_metrics(self):
        """Snapshot of this instance's runtime metrics as a Metrics dataclass.

        Like status(), this takes no lock and does not apply pending updates, so
        item_count reflects our last applied view of the dict.
        """
        buffer_used = int.from_bytes(self.update_stream_position_remote, 'little')

        # macOS is posix but has no /dev/shm, so check the directory, not the platform
        if os.path.isdir('/dev/shm'):
            shm_total, shm_used, shm_free = shutil.disk_usage('/dev/shm')
        else:
            shm_total = shm_used = shm_free = None

        return Metrics(
            item_count=len(self.data),
            item_size_bytes_min=self.item_size_min,
            item_size_bytes_max=self.item_size_max,
            item_size_bytes_sum=self.item_size_sum,
            item_size_observations_total=self.item_size_count,
            buffer_size_bytes=self.buffer_size,
            buffer_used_bytes=buffer_used,
            buffer_used_fraction=buffer_used / self.buffer_size,
            full_dump_size_bytes=self.full_dump_size,
            full_dump_last_bytes=self.full_dump_length,
            full_dump_total=self.full_dump_counter,
            buffer_full_forced_dump_total=self.buffer_full_forced_dump,
            full_dump_memory_full_total=self.full_dump_memory_full,
            full_dump_too_fast_total=self.full_dump_too_fast,
            shm_total_bytes=shm_total,
            shm_used_bytes=shm_used,
            shm_free_bytes=shm_free,
        )

    def cleanup(self):
        # log.debug('Cleanup')

        # for item in self.data.items():
        #    print(type(item))

        if hasattr(self, 'lock') and hasattr(self.lock, 'cleanup'):
            self.lock.cleanup()

        # If we use RLock(), this closes the file handle
        if hasattr(self, 'lock'):
            del self.lock
        if hasattr(self, 'full_dump_memory'):
            del self.full_dump_memory

        data = self.data
        del self.data

        self.del_remotes()

        # self.control.close()
        # self.buffer.close()

        # if self.full_dump_memory:
        #    self.full_dump_memory.close()

        # No further cleanup on Windows, it will break everything
        # if sys.platform == 'win32':
        #    return

        # Only do cleanup once
        # atexit.unregister(self.cleanup)

        self.apply_update = self.raise_already_closed
        self.append_update = self.raise_already_closed

        return data

    def raise_already_closed(self, *args, **kwargs):
        raise Exceptions.AlreadyClosed('UltraDict already closed, you can only access the `UltraDict.data` buffer!')

    def keys(self):
        self.apply_update()
        return self.data.keys()

    def values(self):
        self.apply_update()
        return self.data.values()

    def unlink(self):
        self.close(unlink=True)

    def close(self, unlink=False, from_finalizer=False):
        # log.debug('Close name={} unlink={} auto_unlink={} creator={}', self.name, unlink, self.auto_unlink, hasattr(self.control, 'created_by_ultra'))

        if self.closed:
            # log.debug('Already closed, doing nothing')
            return
        self.closed = True

        if hasattr(self, 'finalizer'):
            self.finalizer.detach()

        full_dump_name = None
        if hasattr(self, 'full_dump_memory_name_remote'):
            full_dump_name = bytes(self.full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')

        data = self.cleanup()

        # If we are the master creator of the shared memory, we'll delete (unlink) it
        # including the full dump memory; for the full dump memory, we delete it even
        # if we are not the creator
        if unlink or (self.auto_unlink and hasattr(self.control, 'created_by_ultra')):
            # log.debug('Unlink', self.name)
            self.control.unlink()
            self.buffer.unlink()
            if full_dump_name:
                self.unlink_by_name(full_dump_name, ignore_errors=True)

            if getattr(self, 'recurse', False):
                self.unlink_recursed()

        if hasattr(self, 'control'):
            self.control.close()
        if hasattr(self, 'buffer'):
            self.buffer.close()

        return data

    def unlink_recursed(self):
        # log.debug("Unlink recursed id={}", hex(id(self)))
        if not self.recurse or (type(self.recurse_register) is not UltraDict):
            raise Exception("Cannot unlink recursed for non-recurse UltraDict")

        ignore_errors = sys.platform == 'win32'
        for name in self.recurse_register.keys():
            # log.debug("Unlink recursed child name={}", name)
            self.unlink_by_name(name=name, ignore_errors=ignore_errors)
            self.unlink_by_name(name=f"{name}_memory", ignore_errors=ignore_errors)

        self.recurse_register.close(unlink=True)

    @staticmethod
    def unlink_by_name(name, ignore_errors=False):
        """
        Can be used to delete left over shared memory blocks after crashes.
        """
        try:
            # log.debug("Unlinking memory '{}'", name)
            memory = UltraDict.get_memory(create=False, name=name)
            memory.unlink()
            memory.close()
            return True
        except Exceptions.CannotAttachSharedMemory as e:
            if not ignore_errors:
                raise e
        return False


# Saved as a reference

# def bytes_to_int(bytes):
#    result = 0
#    for b in bytes:
#        result = result * 256 + int(b)
#    return result
#
# def int_to_bytes(value, length):
#    result = []
#    for i in range(0, length):
#        result.append(value >> (i * 8) & 0xff)
#    result.reverse()
#    return result

# class Mapping(dict):
#
#    def __init__(self, *args, **kwargs):
#        print("__init__", args, kwargs)
#        super().__init__(*args, **kwargs)
#
#    def __setitem__(self, key, item):
#        print("__setitem__", key, item)
#        self.__dict__[key] = item
#
#    def __getitem__(self, key):
#        print("__getitem__", key)
#        return self.__dict__[key]
#
#    def __repr__(self):
#        print("__repr__")
#        return repr(self.__dict__)
#
#    def __len__(self):
#        print("__len__")
#        return len(self.__dict__)
#
#    def __delitem__(self, key):
#        print("__delitem__")
#        del self.__dict__[key]
#
#    def clear(self):
#        print("clear")
#        return self.__dict__.clear()
#
#    def copy(self):
#        print("copy")
#        return self.__dict__.copy()
#
#    def has_key(self, k):
#        print("has_key")
#        return k in self.__dict__
#
#    def update(self, *args, **kwargs):
#        print("update")
#        return self.__dict__.update(*args, **kwargs)
#
#    def keys(self):
#        print("keys")
#        return self.__dict__.keys()
#
#    def values(self):
#        print("values")
#        return self.__dict__.values()
#
#    def items(self):
#        print("items")
#        return self.__dict__.items()
#
#    def pop(self, *args):
#        print("pop")
#        return self.__dict__.pop(*args)
#
#    def __cmp__(self, dict_):
#        print("__cmp__")
#        return self.__cmp__(self.__dict__, dict_)
#
#    def __contains__(self, item):
#        print("__contains__", item)
#        return item in self.__dict__
#
#    def __iter__(self):
#        print("__iter__")
#        return iter(self.__dict__)
#
#    def __unicode__(self):
#        print("__unicode__")
#        return unicode(repr(self.__dict__))
#
