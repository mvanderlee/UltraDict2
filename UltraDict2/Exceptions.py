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

import time


class CannotAttachSharedMemory(Exception):
    pass


class CannotCreateSharedMemory(Exception):
    """Raised when the host cannot back another shared memory segment.

    The original OSError is kept as __cause__; errno is not reliable across
    platforms, so inspect it there rather than branching on the type.
    """

    pass


class CannotAcquireLock(Exception):
    def __init__(self, *args, blocking_pid=0, timestamp=None, **kwargs):
        super().__init__('Cannot acquire lock', *args)
        self.blocking_pid = blocking_pid
        self.timestamp = timestamp or time.monotonic()


class CannotAcquireLockTimeout(CannotAcquireLock):
    def __init__(self, *args, time_passed=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.time_passed = time_passed


class ParameterMismatch(Exception):
    pass


class AlreadyClosed(Exception):
    pass


class AlreadyExists(Exception):
    pass


class FullDumpMemoryFull(Exception):
    pass


class FullDumpsTooFast(Exception):
    """Raised when a reader keeps losing the race against new full dumps.

    Deliberately not an AssertionError: the recovery paths catch AssertionError,
    so a give-up signal that subclassed it would be caught by the very handlers
    raising it and retried forever.
    """

    pass


class CorruptedStream(AssertionError):
    """Raised when the update stream or a full dump contains invalid data.

    Subclasses AssertionError because the recovery paths catch AssertionError;
    unlike a plain `assert`, it still fires when running under `python -O`.
    """

    pass


class MissingDependency(Exception):
    pass
