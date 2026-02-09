# Copyright 2025 nurion team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deterministic hash utilities for cross-process routing.

Python's built-in hash() is randomized across processes via PYTHONHASHSEED,
which breaks distributed routing in Ray. This module provides a deterministic
hash function for consistent shard routing.
"""

import hashlib


def deterministic_hash(s: str) -> int:
    """Deterministic hash for strings using SHA-256.

    Unlike Python's built-in hash(), this is:
    - Deterministic across processes (not affected by PYTHONHASHSEED)
    - Consistent across Python versions and machines

    This ensures that hash(key) % num_shards produces the same result
    in the client process, shard actor processes, and manager process.

    Returns a 64-bit unsigned integer.
    """
    digest = hashlib.sha256(s.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="little")
