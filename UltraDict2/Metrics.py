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

# Kept out of UltraDict2.py because setup.py cythonizes that module only,
# so this dataclass stays plain Python.

__all__ = ['Metrics']

import dataclasses


@dataclasses.dataclass
class Metrics:
    """Snapshot of one UltraDict instance, returned by UltraDict.get_metrics().

    Field names are chosen to map directly onto Prometheus metric names.
    """

    # Items in our local view of the dict
    item_count: int

    # Serialized frame size of every write made through this instance. Historical,
    # not a measurement of the current contents. The mean is item_size_bytes_sum
    # divided by item_size_observations_total and is deliberately not stored.
    item_size_bytes_min: int | None
    item_size_bytes_max: int | None
    item_size_bytes_sum: int
    item_size_observations_total: int

    # Update stream buffer
    buffer_size_bytes: int
    buffer_used_bytes: int
    buffer_used_fraction: float

    # Full dumps. full_dump_size_bytes is None unless a static size was configured,
    # full_dump_last_bytes is None until this instance has dumped.
    full_dump_size_bytes: int | None
    full_dump_last_bytes: int | None
    full_dump_total: int

    # Events observed by this instance since it was created
    buffer_full_forced_dump_total: int
    full_dump_memory_full_total: int
    full_dump_too_fast_total: int

    # System shared memory, None where there is no /dev/shm to measure
    shm_total_bytes: int | None
    shm_used_bytes: int | None
    shm_free_bytes: int | None
