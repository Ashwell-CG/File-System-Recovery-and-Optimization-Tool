"""
free_space_manager.py – Free Space Management
===============================================
Provides two allocation strategies:

1. **BitmapAllocator** – A bit-vector where each bit represents one disk block.
   0 = free, 1 = occupied.  Fast allocation and O(1) status checks.

2. **LinkedListAllocator** – A linked list of free block IDs.
   Allocation is O(1) from the head; deallocation appends to the head.

Both share a common interface so the file system can swap strategies.
"""

from __future__ import annotations
import math
from typing import List, Optional


class FreeSpaceManager:
    """Abstract base for free-space tracking."""

    def allocate(self, count: int = 1) -> List[int]:
        """Allocate *count* contiguous-or-scattered free blocks. Returns block IDs."""
        raise NotImplementedError

    def free(self, block_ids: List[int]):
        """Mark the given blocks as free."""
        raise NotImplementedError

    def is_free(self, block_id: int) -> bool:
        raise NotImplementedError

    def free_count(self) -> int:
        raise NotImplementedError

    def used_count(self) -> int:
        raise NotImplementedError

    def total_blocks(self) -> int:
        raise NotImplementedError

    def utilization(self) -> float:
        """Return percentage (0-100) of used space."""
        total = self.total_blocks()
        return (self.used_count() / total * 100) if total else 0.0

    # ── Serialization (to / from raw bytes for disk persistence) ─────────
    def serialize(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def deserialize(cls, data: bytes, total: int) -> "FreeSpaceManager":
        raise NotImplementedError


# ─────────────────────────────────────────────────────────────────────────────
# Bitmap Allocator
# ─────────────────────────────────────────────────────────────────────────────

class BitmapAllocator(FreeSpaceManager):
    """
    Bitmap-based free space manager.

    The bitmap is stored as a bytearray; bit *i* of byte *i//8* represents
    block *i*.  ``0`` = free, ``1`` = occupied.
    """

    def __init__(self, total: int, reserved: Optional[List[int]] = None):
        """
        Parameters
        ----------
        total : int
            Total number of blocks on the disk.
        reserved : list[int], optional
            Block IDs to mark as occupied immediately (e.g., superblock,
            bitmap blocks, inode table).
        """
        self._total = total
        self._bitmap = bytearray(math.ceil(total / 8))
        self._used = 0
        if reserved:
            for bid in reserved:
                self._set(bid, True)

    # ── Bit helpers ──────────────────────────────────────────────────────

    def _set(self, block_id: int, occupied: bool):
        byte_idx = block_id // 8
        bit_idx = block_id % 8
        if occupied:
            self._bitmap[byte_idx] |= (1 << bit_idx)
            self._used += 1
        else:
            self._bitmap[byte_idx] &= ~(1 << bit_idx)
            self._used -= 1

    def _get(self, block_id: int) -> bool:
        byte_idx = block_id // 8
        bit_idx = block_id % 8
        return bool(self._bitmap[byte_idx] & (1 << bit_idx))

    # ── Public API ───────────────────────────────────────────────────────

    def allocate(self, count: int = 1) -> List[int]:
        allocated: List[int] = []
        for bid in range(self._total):
            if not self._get(bid):
                self._set(bid, True)
                allocated.append(bid)
                if len(allocated) == count:
                    return allocated
        # Not enough space – rollback
        for bid in allocated:
            self._set(bid, False)
        raise OSError(f"Not enough free blocks: requested {count}, available {self.free_count()}")

    def free(self, block_ids: List[int]):
        for bid in block_ids:
            if self._get(bid):
                self._set(bid, False)

    def is_free(self, block_id: int) -> bool:
        return not self._get(block_id)

    def free_count(self) -> int:
        return self._total - self._used

    def used_count(self) -> int:
        return self._used

    def total_blocks(self) -> int:
        return self._total

    # ── Persistence ──────────────────────────────────────────────────────

    def serialize(self) -> bytes:
        return bytes(self._bitmap)

    @classmethod
    def deserialize(cls, data: bytes, total: int) -> "BitmapAllocator":
        obj = cls(total)
        obj._bitmap = bytearray(data[:math.ceil(total / 8)])
        obj._used = sum(
            bin(byte).count("1") for byte in obj._bitmap
        )
        # Mask out bits beyond total  (last byte may have padding bits)
        return obj

    def __repr__(self):
        return (f"BitmapAllocator(total={self._total}, used={self._used}, "
                f"free={self.free_count()})")


# ─────────────────────────────────────────────────────────────────────────────
# Linked-List Allocator
# ─────────────────────────────────────────────────────────────────────────────

class LinkedListAllocator(FreeSpaceManager):
    """
    Linked-list free space manager.

    Maintains a Python list acting as a stack of free block IDs.
    Allocation pops from the front; deallocation pushes to the front.
    """

    def __init__(self, total: int, reserved: Optional[List[int]] = None):
        self._total = total
        reserved_set = set(reserved) if reserved else set()
        self._free_list: List[int] = [
            bid for bid in range(total) if bid not in reserved_set
        ]

    def allocate(self, count: int = 1) -> List[int]:
        if len(self._free_list) < count:
            raise OSError(f"Not enough free blocks: requested {count}, "
                          f"available {len(self._free_list)}")
        allocated = self._free_list[:count]
        self._free_list = self._free_list[count:]
        return allocated

    def free(self, block_ids: List[int]):
        for bid in block_ids:
            if bid not in self._free_list:
                self._free_list.insert(0, bid)

    def is_free(self, block_id: int) -> bool:
        return block_id in self._free_list

    def free_count(self) -> int:
        return len(self._free_list)

    def used_count(self) -> int:
        return self._total - len(self._free_list)

    def total_blocks(self) -> int:
        return self._total

    # ── Persistence ──────────────────────────────────────────────────────

    def serialize(self) -> bytes:
        import struct
        return struct.pack(f">{len(self._free_list)}I",  *self._free_list)

    @classmethod
    def deserialize(cls, data: bytes, total: int) -> "LinkedListAllocator":
        import struct
        count = len(data) // 4
        free_ids = list(struct.unpack(f">{count}I", data[:count * 4]))
        obj = cls(total)
        obj._free_list = free_ids
        return obj

    def __repr__(self):
        return (f"LinkedListAllocator(total={self._total}, "
                f"free={self.free_count()})")
