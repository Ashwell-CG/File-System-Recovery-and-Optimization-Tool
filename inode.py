"""
inode.py – Inode (Index Node) Management
==========================================
Each file or directory on the virtual disk is represented by an Inode.
The Inode stores:
  * metadata (type, size, timestamps)
  * block pointers for three access methods:
      – Sequential  : an ordered list of block IDs read front-to-back
      – Direct       : fixed-length array of direct block pointers (fast random access)
      – Indexed      : a single pointer to an *index block* that itself holds
                       the list of data-block pointers
"""

from __future__ import annotations
import struct
import time
from enum import IntEnum
from typing import List, Optional


# ── Enumerations ─────────────────────────────────────────────────────────────

class FileType(IntEnum):
    FILE = 0
    DIRECTORY = 1


class AccessMethod(IntEnum):
    SEQUENTIAL = 0
    DIRECT = 1
    INDEXED = 2


# ── Inode ────────────────────────────────────────────────────────────────────

# Maximum direct block pointers stored in the inode itself.
MAX_DIRECT_POINTERS = 12

# Fixed serialized size (we pad to this so the inode table has fixed-size
# entries).  Must be large enough to hold the header + MAX_DIRECT_POINTERS.
INODE_SERIAL_SIZE = 256  # bytes


class Inode:
    """
    Represents a single file-system object (file or directory).

    Attributes
    ----------
    inode_id       : Unique identifier inside the inode table.
    file_type      : FILE or DIRECTORY.
    access_method  : SEQUENTIAL, DIRECT, or INDEXED.
    size           : Logical file size in bytes.
    created_at     : UNIX timestamp of creation.
    modified_at    : UNIX timestamp of last modification.
    block_pointers : List of data-block IDs (for SEQUENTIAL / DIRECT).
    index_block    : Block ID of the index block (for INDEXED method).
    """

    def __init__(self,
                 inode_id: int,
                 file_type: FileType = FileType.FILE,
                 access_method: AccessMethod = AccessMethod.SEQUENTIAL):
        self.inode_id = inode_id
        self.file_type = file_type
        self.access_method = access_method
        self.size = 0
        self.created_at = time.time()
        self.modified_at = self.created_at
        # Block storage
        self.block_pointers: List[int] = []   # used by SEQUENTIAL and DIRECT
        self.index_block: int = -1            # used by INDEXED

    # ── Helpers ──────────────────────────────────────────────────────────

    def add_block(self, block_id: int):
        """Append a data block pointer."""
        self.block_pointers.append(block_id)
        self.modified_at = time.time()

    def remove_blocks(self):
        """Clear all block associations (for deletion)."""
        self.block_pointers.clear()
        self.index_block = -1
        self.size = 0
        self.modified_at = time.time()

    def get_blocks(self) -> List[int]:
        """Return a *copy* of the block pointer list."""
        return list(self.block_pointers)

    # ── Serialization ────────────────────────────────────────────────────

    def serialize(self) -> bytes:
        """
        Pack the inode into a fixed-size byte string.

        Layout (all big-endian):
            4B  inode_id
            1B  file_type
            1B  access_method
            8B  size (uint64)
            8B  created_at (double)
            8B  modified_at (double)
            4B  index_block (int32, -1 if unused)
            4B  num_pointers
            num_pointers * 4B  block_pointers
            padding to INODE_SERIAL_SIZE
        """
        header = struct.pack(
            ">IBBqddi I",
            self.inode_id,
            int(self.file_type),
            int(self.access_method),
            self.size,
            self.created_at,
            self.modified_at,
            self.index_block,
            len(self.block_pointers),
        )
        ptrs = struct.pack(f">{len(self.block_pointers)}I", *self.block_pointers) \
            if self.block_pointers else b""
        raw = header + ptrs
        # Pad
        assert len(raw) <= INODE_SERIAL_SIZE, (
            f"Inode {self.inode_id} serialization too large: {len(raw)} > {INODE_SERIAL_SIZE}")
        return raw.ljust(INODE_SERIAL_SIZE, b"\x00")

    @classmethod
    def deserialize(cls, data: bytes) -> Optional["Inode"]:
        """Unpack an inode from *INODE_SERIAL_SIZE* bytes.  Returns None
        if the data is all zeros (empty slot)."""
        if data == b"\x00" * INODE_SERIAL_SIZE:
            return None
        # Header: I B B q d d i I  → 4+1+1+8+8+8+4+4 = 38 bytes
        hdr_fmt = ">IBBqddiI"
        hdr_size = struct.calcsize(hdr_fmt)
        (inode_id, ftype, amethod, size,
         created, modified, idx_block, num_ptrs) = struct.unpack(
            hdr_fmt, data[:hdr_size]
        )
        ptrs = list(struct.unpack(f">{num_ptrs}I", data[hdr_size:hdr_size + num_ptrs * 4])) \
            if num_ptrs else []

        node = cls(inode_id, FileType(ftype), AccessMethod(amethod))
        node.size = size
        node.created_at = created
        node.modified_at = modified
        node.index_block = idx_block
        node.block_pointers = ptrs
        return node

    def __repr__(self):
        return (f"Inode(id={self.inode_id}, type={self.file_type.name}, "
                f"access={self.access_method.name}, size={self.size}, "
                f"blocks={self.block_pointers})")
