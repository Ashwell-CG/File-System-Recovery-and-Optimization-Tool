"""
disk.py – Virtual Disk Simulator
=================================
Simulates a physical disk as a flat binary file divided into fixed-size blocks.
Provides low-level read/write operations, an LRU write-back cache, and
crash-injection capabilities (power loss, block corruption, partial writes).
"""

import os
import time
import random
import struct
from collections import OrderedDict

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_BLOCK_SIZE = 4096          # 4 KB per block
DEFAULT_TOTAL_BLOCKS = 2560        # 2560 * 4 KB = 10 MB disk
DISK_FILE = "virtual_disk.bin"

# ── Performance Metrics Tracker ──────────────────────────────────────────────

class DiskMetrics:
    """Accumulates read/write latency and cache-hit statistics."""

    def __init__(self):
        self.total_reads = 0
        self.total_writes = 0
        self.total_read_time = 0.0
        self.total_write_time = 0.0
        self.cache_hits = 0
        self.cache_misses = 0

    def record_read(self, elapsed: float):
        self.total_reads += 1
        self.total_read_time += elapsed

    def record_write(self, elapsed: float):
        self.total_writes += 1
        self.total_write_time += elapsed

    def record_cache_hit(self):
        self.cache_hits += 1

    def record_cache_miss(self):
        self.cache_misses += 1

    def summary(self) -> dict:
        return {
            "total_reads": self.total_reads,
            "total_writes": self.total_writes,
            "avg_read_time_ms": (self.total_read_time / self.total_reads * 1000)
                                 if self.total_reads else 0,
            "avg_write_time_ms": (self.total_write_time / self.total_writes * 1000)
                                  if self.total_writes else 0,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": (self.cache_hits / (self.cache_hits + self.cache_misses) * 100)
                               if (self.cache_hits + self.cache_misses) else 0,
        }

    def __repr__(self):
        s = self.summary()
        return (
            f"DiskMetrics(reads={s['total_reads']}, writes={s['total_writes']}, "
            f"avg_rd={s['avg_read_time_ms']:.3f}ms, avg_wr={s['avg_write_time_ms']:.3f}ms, "
            f"cache_hit_rate={s['cache_hit_rate']:.1f}%)"
        )


# ── LRU Block Cache ─────────────────────────────────────────────────────────

class BlockCache:
    """
    Simple LRU write-back cache.
    Dirty entries are flushed when evicted or on explicit flush().
    """

    def __init__(self, capacity: int = 64):
        self.capacity = capacity
        self._cache: OrderedDict[int, bytearray] = OrderedDict()
        self._dirty: set[int] = set()

    def get(self, block_id: int):
        """Return cached block data or None."""
        if block_id in self._cache:
            self._cache.move_to_end(block_id)
            return self._cache[block_id]
        return None

    def put(self, block_id: int, data: bytearray, dirty: bool = False):
        """Insert / update a cache entry, evicting LRU if at capacity."""
        if block_id in self._cache:
            self._cache.move_to_end(block_id)
        self._cache[block_id] = data
        if dirty:
            self._dirty.add(block_id)
        # Evict oldest if over capacity – caller must flush dirty before evict
        while len(self._cache) > self.capacity:
            evicted_id, _ = self._cache.popitem(last=False)
            self._dirty.discard(evicted_id)

    def mark_dirty(self, block_id: int):
        if block_id in self._cache:
            self._dirty.add(block_id)

    def dirty_entries(self):
        """Yield (block_id, data) for all dirty entries."""
        for bid in list(self._dirty):
            if bid in self._cache:
                yield bid, self._cache[bid]

    def clear_dirty(self, block_id: int):
        self._dirty.discard(block_id)

    def invalidate(self, block_id: int):
        self._cache.pop(block_id, None)
        self._dirty.discard(block_id)

    def flush_all_dirty(self):
        """Return list of (block_id, data) that need writing, then clear."""
        entries = [(bid, self._cache[bid]) for bid in self._dirty if bid in self._cache]
        self._dirty.clear()
        return entries

    def clear(self):
        self._cache.clear()
        self._dirty.clear()


# ── Virtual Disk ─────────────────────────────────────────────────────────────

class VirtualDisk:
    """
    Represents a virtual disk stored as a binary file on the host OS.

    Features
    --------
    * Fixed-size block I/O (read_block / write_block).
    * Optional LRU write-back cache to reduce host-file I/O.
    * Crash-injection:
        – ``simulate_power_loss()``: silently drops all pending (dirty-cache)
          writes and flags a crash, so the next mount triggers recovery.
        – ``corrupt_block(block_id)``: overwrites a block with random garbage.
        – ``simulate_partial_write(block_id, data)``: writes only a fraction
          of the data to mimic an interrupted write.
    """

    def __init__(self, filepath: str = DISK_FILE,
                 block_size: int = DEFAULT_BLOCK_SIZE,
                 total_blocks: int = DEFAULT_TOTAL_BLOCKS,
                 cache_enabled: bool = True,
                 cache_capacity: int = 64):
        self.filepath = filepath
        self.block_size = block_size
        self.total_blocks = total_blocks
        self.disk_size = block_size * total_blocks
        self.metrics = DiskMetrics()
        self._crashed = False  # True after a simulated crash

        # Cache
        self.cache_enabled = cache_enabled
        self.cache = BlockCache(cache_capacity) if cache_enabled else None

    # ── Lifecycle ────────────────────────────────────────────────────────

    def format_disk(self):
        """Create / overwrite the virtual disk file with zeros."""
        with open(self.filepath, "wb") as f:
            # Write in chunks to avoid huge memory spike
            chunk = b"\x00" * self.block_size
            for _ in range(self.total_blocks):
                f.write(chunk)
        if self.cache:
            self.cache.clear()
        self._crashed = False

    def exists(self) -> bool:
        return os.path.isfile(self.filepath)

    @property
    def crashed(self) -> bool:
        return self._crashed

    @crashed.setter
    def crashed(self, value: bool):
        self._crashed = value

    # ── Low-level I/O ────────────────────────────────────────────────────

    def _raw_read(self, block_id: int) -> bytearray:
        """Read a single block directly from the host file."""
        with open(self.filepath, "rb") as f:
            f.seek(block_id * self.block_size)
            data = f.read(self.block_size)
        return bytearray(data)

    def _raw_write(self, block_id: int, data: bytes):
        """Write a single block directly to the host file."""
        assert len(data) == self.block_size, (
            f"Block data must be exactly {self.block_size} bytes, got {len(data)}"
        )
        with open(self.filepath, "r+b") as f:
            f.seek(block_id * self.block_size)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

    # ── Public I/O (cached) ──────────────────────────────────────────────

    def read_block(self, block_id: int) -> bytearray:
        """
        Read a block, using cache if available.
        Simulates read latency via a small sleep.
        """
        if block_id < 0 or block_id >= self.total_blocks:
            raise ValueError(f"Block {block_id} out of range [0, {self.total_blocks})")

        start = time.perf_counter()

        # Try cache first
        if self.cache:
            cached = self.cache.get(block_id)
            if cached is not None:
                self.metrics.record_cache_hit()
                elapsed = time.perf_counter() - start
                self.metrics.record_read(elapsed)
                return bytearray(cached)  # return copy
            self.metrics.record_cache_miss()

        # Simulate disk seek latency (very small)
        time.sleep(0.0001)
        data = self._raw_read(block_id)

        if self.cache:
            self.cache.put(block_id, bytearray(data))

        elapsed = time.perf_counter() - start
        self.metrics.record_read(elapsed)
        return data

    def write_block(self, block_id: int, data: bytes):
        """
        Write a block. If cache is enabled, the data goes into the cache
        as a dirty entry and is NOT immediately persisted (write-back).
        Call ``flush()`` to persist.
        """
        if block_id < 0 or block_id >= self.total_blocks:
            raise ValueError(f"Block {block_id} out of range [0, {self.total_blocks})")
        if len(data) > self.block_size:
            raise ValueError("Data exceeds block size")

        # Pad to block size
        padded = bytearray(data) + bytearray(self.block_size - len(data))
        start = time.perf_counter()

        if self.cache:
            self.cache.put(block_id, padded, dirty=True)
        else:
            time.sleep(0.0001)
            self._raw_write(block_id, bytes(padded))

        elapsed = time.perf_counter() - start
        self.metrics.record_write(elapsed)

    def flush(self):
        """Persist all dirty cache entries to the host file."""
        if not self.cache:
            return
        for bid, data in self.cache.flush_all_dirty():
            self._raw_write(bid, bytes(data))

    # ── Crash Injection ──────────────────────────────────────────────────

    def simulate_power_loss(self):
        """
        Simulate sudden power loss.
        All dirty (unflushed) writes in the cache are LOST.
        The crash flag is set so recovery can be triggered on next mount.
        """
        if self.cache:
            dirty_count = len(self.cache._dirty)
            self.cache.clear()  # everything in RAM is gone
            print(f"[CRASH] Power loss! {dirty_count} dirty blocks lost.")
        else:
            print("[CRASH] Power loss! (no cache – all writes already persisted)")
        self._crashed = True

    def corrupt_block(self, block_id: int):
        """Overwrite a block with random garbage to simulate media failure."""
        garbage = bytearray(random.getrandbits(8) for _ in range(self.block_size))
        self._raw_write(block_id, bytes(garbage))
        if self.cache:
            self.cache.invalidate(block_id)
        print(f"[CRASH] Block {block_id} corrupted with random data.")
        self._crashed = True

    def simulate_partial_write(self, block_id: int, data: bytes):
        """
        Write only a random fraction (25-75 %) of the block to simulate
        an interrupted write (e.g., power cut mid-sector).
        """
        padded = bytearray(data) + bytearray(self.block_size - len(data))
        fraction = random.uniform(0.25, 0.75)
        cut = int(self.block_size * fraction)
        partial = bytes(padded[:cut]) + bytes(self.block_size - cut)
        # ^-- rest is zeros (simulating incomplete write)

        with open(self.filepath, "r+b") as f:
            f.seek(block_id * self.block_size)
            f.write(partial)
            f.flush()
        if self.cache:
            self.cache.invalidate(block_id)
        print(f"[CRASH] Partial write on block {block_id} – only {cut}/{self.block_size} bytes written.")
        self._crashed = True
