"""
file_system.py – High-Level File System Controller
====================================================
Ties together Disk, FreeSpaceManager, InodeTable, DirectoryTree,
Journal, and CheckpointManager to expose application-level file
operations with crash-safe journaling and optional defragmentation.

Disk Layout
-----------
Block 0           : Superblock  (magic, version, total blocks, etc.)
Blocks 1-3        : Bitmap  (3 blocks → tracks up to 3*4096*8 = 98 304 blocks)
Blocks 4-67       : Inode table (64 blocks → 64 * (4096/256) = 1024 inodes max)
Blocks 68+        : Data blocks
"""

from __future__ import annotations
import json
import math
import struct
import time
from typing import Dict, List, Optional, Tuple

from disk import VirtualDisk, DEFAULT_BLOCK_SIZE, DEFAULT_TOTAL_BLOCKS
from free_space_manager import BitmapAllocator
from inode import Inode, FileType, AccessMethod, INODE_SERIAL_SIZE
from directory import DirectoryTree
from recovery import Journal, CheckpointManager, BackupManager


# ── Layout constants ─────────────────────────────────────────────────────────

SUPERBLOCK_ID = 0
BITMAP_START = 1
BITMAP_BLOCKS = 3                        # 3 × 4096 × 8 = 98 304 bits (enough)
INODE_TABLE_START = BITMAP_START + BITMAP_BLOCKS   # block 4
INODE_TABLE_BLOCKS = 64                  # 64 blocks
INODES_PER_BLOCK = DEFAULT_BLOCK_SIZE // INODE_SERIAL_SIZE  # 16
MAX_INODES = INODE_TABLE_BLOCKS * INODES_PER_BLOCK  # 1024
DATA_START = INODE_TABLE_START + INODE_TABLE_BLOCKS  # block 68

MAGIC = b"FSIM"
VERSION = 1

RESERVED_BLOCKS = list(range(DATA_START))  # 0 .. 67


# ── File System ──────────────────────────────────────────────────────────────

class FileSystem:
    """
    Top-level file-system API.

    Public methods
    --------------
    format()            – Create a fresh file system on the virtual disk.
    mount()             – Load an existing file system (runs recovery if needed).
    create_file(path, access_method)
    write_file(path, data)
    read_file(path)     – Read all content from a file.
    delete_file(path)
    mkdir(path)
    rmdir(path)
    ls(path)
    rename(path, new_name)
    search(name)
    stat(path)
    defragment()        – Compact data blocks to reduce fragmentation.
    checkpoint()        – Save a metadata snapshot.
    simulate_crash(kind, **kw) – Inject a fault.
    recover()           – Run journal replay + checkpoint restore.
    get_metrics()       – Return performance and utilization stats.
    """

    def __init__(self, disk: Optional[VirtualDisk] = None):
        self.disk = disk or VirtualDisk()
        self.allocator: Optional[BitmapAllocator] = None
        self.inodes: Dict[int, Inode] = {}
        self.dir_tree = DirectoryTree()
        self.journal = Journal()
        self.checkpoint_mgr = CheckpointManager()
        self.backup_mgr = BackupManager(self.disk)
        self._next_inode_id = 1  # 0 is root dir
        self._mounted = False
        self._recovery_time = 0.0

    # ──────────────────────────────────────────────────────────────────────
    # FORMAT
    # ──────────────────────────────────────────────────────────────────────

    def format(self):
        """Create a brand-new file system."""
        print("[FS] Formatting disk …")
        self.disk.format_disk()

        # Superblock
        sb = self._make_superblock()
        self.disk.write_block(SUPERBLOCK_ID, sb)

        # Bitmap – mark reserved blocks
        self.allocator = BitmapAllocator(self.disk.total_blocks, reserved=RESERVED_BLOCKS)
        self._persist_bitmap()

        # Root directory inode (inode 0)
        root_inode = Inode(0, FileType.DIRECTORY, AccessMethod.SEQUENTIAL)
        self.inodes = {0: root_inode}
        self._persist_inode(root_inode)
        self._next_inode_id = 1

        # Directory tree (empty root)
        self.dir_tree = DirectoryTree()

        # Flush everything and clear journal
        self.disk.flush()
        self.journal.truncate()
        self.checkpoint()
        self._mounted = True
        print("[FS] Format complete.")

    # ──────────────────────────────────────────────────────────────────────
    # MOUNT
    # ──────────────────────────────────────────────────────────────────────

    def mount(self):
        """
        Load the file system from the virtual disk.
        If the disk is flagged as crashed, run recovery first.
        """
        if not self.disk.exists():
            raise FileNotFoundError("Virtual disk not found. Run format() first.")

        # Check superblock
        sb = self.disk.read_block(SUPERBLOCK_ID)
        if sb[:4] != MAGIC:
            raise ValueError("Invalid superblock – disk not formatted")

        # If crashed, recover before normal mount
        if self.disk.crashed or self.journal.uncommitted_entries():
            print("[FS] Crash detected – running recovery …")
            self.recover()

        # Load bitmap
        self._load_bitmap()

        # Load inodes
        self._load_inodes()

        # Load directory tree from checkpoint
        cp = self.checkpoint_mgr.load()
        if cp and "directory_tree" in cp:
            self.dir_tree = DirectoryTree.deserialize(cp["directory_tree"])
        else:
            self.dir_tree = DirectoryTree()

        self._mounted = True
        print("[FS] File system mounted.")

    # ──────────────────────────────────────────────────────────────────────
    # FILE OPERATIONS
    # ──────────────────────────────────────────────────────────────────────

    def create_file(self, path: str,
                    access_method: AccessMethod = AccessMethod.SEQUENTIAL) -> int:
        """Create an empty file. Returns inode_id."""
        self._check_mounted()
        parent_inode, name = self.dir_tree.resolve_parent(path)
        if parent_inode is None:
            raise FileNotFoundError(f"Parent directory not found for '{path}'")

        inode_id = self._alloc_inode_id()
        node = Inode(inode_id, FileType.FILE, access_method)
        self.inodes[inode_id] = node

        # Journal
        txn = self.journal.log("CREATE_FILE", {
            "path": path, "inode_id": inode_id,
            "parent_inode": parent_inode, "name": name,
            "access_method": int(access_method),
        })

        self.dir_tree.create_entry(parent_inode, name, inode_id, is_dir=False)
        self._persist_inode(node)
        self.disk.flush()
        self.journal.commit(txn)
        print(f"[FS] Created file '{path}' (inode {inode_id}, {access_method.name})")
        return inode_id

    def write_file(self, path: str, data: bytes):
        """Write (overwrite) data to an existing file."""
        self._check_mounted()
        inode_id, is_dir = self.dir_tree.resolve_path(path)
        if inode_id is None:
            raise FileNotFoundError(f"File not found: '{path}'")
        if is_dir:
            raise IsADirectoryError(f"'{path}' is a directory")

        node = self.inodes[inode_id]

        # Free old blocks
        if node.block_pointers:
            self.allocator.free(node.block_pointers)

        # If INDEXED method and had an index block, free that too
        if node.access_method == AccessMethod.INDEXED and node.index_block >= 0:
            self.allocator.free([node.index_block])
        node.remove_blocks()

        # Calculate needed blocks
        num_blocks = max(1, math.ceil(len(data) / self.disk.block_size))
        blocks = self.allocator.allocate(num_blocks)

        # Journal
        txn = self.journal.log("WRITE_FILE", {
            "path": path, "inode_id": inode_id,
            "blocks": blocks, "size": len(data),
        })

        # Write data blocks
        for i, bid in enumerate(blocks):
            start = i * self.disk.block_size
            end = start + self.disk.block_size
            chunk = data[start:end]
            self.disk.write_block(bid, chunk)

        # Update inode
        node.block_pointers = blocks
        node.size = len(data)
        node.modified_at = time.time()

        # Handle INDEXED access method – write index block
        if node.access_method == AccessMethod.INDEXED:
            idx_blocks = self.allocator.allocate(1)
            node.index_block = idx_blocks[0]
            # Index block stores the list of data block IDs
            idx_data = struct.pack(f">{len(blocks)}I", *blocks)
            self.disk.write_block(node.index_block, idx_data)

        self._persist_inode(node)
        self._persist_bitmap()
        self.disk.flush()
        self.journal.commit(txn)
        print(f"[FS] Wrote {len(data)} bytes to '{path}' ({num_blocks} blocks)")

    def read_file(self, path: str) -> bytes:
        """Read all content from a file."""
        self._check_mounted()
        inode_id, is_dir = self.dir_tree.resolve_path(path)
        if inode_id is None:
            raise FileNotFoundError(f"File not found: '{path}'")
        if is_dir:
            raise IsADirectoryError(f"'{path}' is a directory")

        node = self.inodes[inode_id]

        if node.access_method == AccessMethod.INDEXED and node.index_block >= 0:
            return self._read_indexed(node)
        elif node.access_method == AccessMethod.DIRECT:
            return self._read_direct(node)
        else:
            return self._read_sequential(node)

    def _read_sequential(self, node: Inode) -> bytes:
        """Read blocks in order (front to back)."""
        data = bytearray()
        for bid in node.block_pointers:
            data.extend(self.disk.read_block(bid))
        return bytes(data[:node.size])

    def _read_direct(self, node: Inode) -> bytes:
        """Read blocks via direct pointers (same as sequential in our sim,
        but the semantic is random-access capable)."""
        data = bytearray()
        for bid in node.block_pointers:
            data.extend(self.disk.read_block(bid))
        return bytes(data[:node.size])

    def _read_indexed(self, node: Inode) -> bytes:
        """Read the index block first, then fetch data blocks it points to."""
        idx_raw = self.disk.read_block(node.index_block)
        num = len(node.block_pointers)
        block_ids = list(struct.unpack(f">{num}I", idx_raw[:num * 4]))
        data = bytearray()
        for bid in block_ids:
            data.extend(self.disk.read_block(bid))
        return bytes(data[:node.size])

    def delete_file(self, path: str):
        """Delete a file and free its blocks."""
        self._check_mounted()
        parent_inode, name = self.dir_tree.resolve_parent(path)
        if parent_inode is None:
            raise FileNotFoundError(f"Path not found: '{path}'")

        inode_id, is_dir = self.dir_tree.resolve_path(path)
        if inode_id is None:
            raise FileNotFoundError(f"File not found: '{path}'")
        if is_dir:
            raise IsADirectoryError(f"'{path}' is a directory – use rmdir()")

        node = self.inodes[inode_id]

        txn = self.journal.log("DELETE_FILE", {
            "path": path, "inode_id": inode_id,
            "blocks": node.block_pointers,
        })

        # Free data blocks
        if node.block_pointers:
            self.allocator.free(node.block_pointers)
        if node.access_method == AccessMethod.INDEXED and node.index_block >= 0:
            self.allocator.free([node.index_block])

        # Remove from directory + inode table
        self.dir_tree.delete_entry(parent_inode, name)
        node.remove_blocks()
        del self.inodes[inode_id]

        self._persist_bitmap()
        self._persist_inode_table()
        self.disk.flush()
        self.journal.commit(txn)
        print(f"[FS] Deleted file '{path}'")

    # ── Directory operations ─────────────────────────────────────────────

    def mkdir(self, path: str) -> int:
        """Create a directory. Returns inode_id."""
        self._check_mounted()
        parent_inode, name = self.dir_tree.resolve_parent(path)
        if parent_inode is None:
            raise FileNotFoundError(f"Parent directory not found for '{path}'")

        inode_id = self._alloc_inode_id()
        node = Inode(inode_id, FileType.DIRECTORY)
        self.inodes[inode_id] = node

        txn = self.journal.log("MKDIR", {
            "path": path, "inode_id": inode_id,
            "parent_inode": parent_inode, "name": name,
        })

        self.dir_tree.create_entry(parent_inode, name, inode_id, is_dir=True)
        self._persist_inode(node)
        self.disk.flush()
        self.journal.commit(txn)
        print(f"[FS] Created directory '{path}' (inode {inode_id})")
        return inode_id

    def rmdir(self, path: str):
        """Remove an empty directory."""
        self._check_mounted()
        if path == "/":
            raise PermissionError("Cannot remove root directory")
        inode_id, is_dir = self.dir_tree.resolve_path(path)
        if inode_id is None or not is_dir:
            raise FileNotFoundError(f"Directory not found: '{path}'")
        entries = self.dir_tree.list_dir(inode_id)
        if entries:
            raise OSError(f"Directory '{path}' is not empty")

        parent_inode, name = self.dir_tree.resolve_parent(path)

        txn = self.journal.log("RMDIR", {
            "path": path, "inode_id": inode_id,
        })

        self.dir_tree.delete_entry(parent_inode, name)
        del self.inodes[inode_id]
        self._persist_inode_table()
        self.disk.flush()
        self.journal.commit(txn)
        print(f"[FS] Removed directory '{path}'")

    def ls(self, path: str = "/") -> list:
        """List entries in a directory."""
        self._check_mounted()
        inode_id, is_dir = self.dir_tree.resolve_path(path)
        if inode_id is None or not is_dir:
            raise FileNotFoundError(f"Directory not found: '{path}'")
        return self.dir_tree.list_dir(inode_id)

    def rename(self, path: str, new_name: str):
        """Rename a file or directory."""
        self._check_mounted()
        parent_inode, old_name = self.dir_tree.resolve_parent(path)
        if parent_inode is None:
            raise FileNotFoundError(f"Path not found: '{path}'")

        txn = self.journal.log("RENAME", {
            "path": path, "new_name": new_name,
        })

        self.dir_tree.rename_entry(parent_inode, old_name, new_name)
        self.disk.flush()
        self.journal.commit(txn)
        print(f"[FS] Renamed '{old_name}' → '{new_name}'")

    def search(self, name: str) -> list:
        """Search for file/directory by name. Returns list of paths."""
        self._check_mounted()
        return self.dir_tree.search(name)

    def stat(self, path: str) -> dict:
        """Return metadata for a file or directory."""
        self._check_mounted()
        inode_id, is_dir = self.dir_tree.resolve_path(path)
        if inode_id is None:
            raise FileNotFoundError(f"Not found: '{path}'")
        node = self.inodes[inode_id]
        return {
            "inode_id": node.inode_id,
            "type": node.file_type.name,
            "access_method": node.access_method.name,
            "size": node.size,
            "blocks": node.block_pointers,
            "index_block": node.index_block,
            "created": time.ctime(node.created_at),
            "modified": time.ctime(node.modified_at),
        }

    # ──────────────────────────────────────────────────────────────────────
    # DEFRAGMENTATION
    # ──────────────────────────────────────────────────────────────────────

    def defragment(self):
        """
        Compact all file data into contiguous block ranges starting from
        DATA_START.  Reduces fragmentation and improves sequential read
        performance.
        """
        self._check_mounted()
        print("[FS] Starting defragmentation …")
        start = time.perf_counter()

        # Collect all file inodes
        file_inodes = self.dir_tree.all_file_inodes()
        if not file_inodes:
            print("[FS] No files to defragment.")
            return

        # Read all file data into memory
        file_data: List[Tuple[int, bytes]] = []
        for iid in file_inodes:
            node = self.inodes.get(iid)
            if node and node.block_pointers:
                raw = bytearray()
                for bid in node.block_pointers:
                    raw.extend(self.disk.read_block(bid))
                file_data.append((iid, bytes(raw[:node.size])))

        # Free all data blocks
        for iid, _ in file_data:
            node = self.inodes[iid]
            if node.block_pointers:
                self.allocator.free(node.block_pointers)
            if node.access_method == AccessMethod.INDEXED and node.index_block >= 0:
                self.allocator.free([node.index_block])
            node.remove_blocks()

        # Re-write files contiguously
        for iid, data in file_data:
            node = self.inodes[iid]
            num_blocks = max(1, math.ceil(len(data) / self.disk.block_size))
            blocks = self.allocator.allocate(num_blocks)
            for i, bid in enumerate(blocks):
                s = i * self.disk.block_size
                e = s + self.disk.block_size
                self.disk.write_block(bid, data[s:e])
            node.block_pointers = blocks
            node.size = len(data)
            node.modified_at = time.time()
            # Indexed: recreate index block
            if node.access_method == AccessMethod.INDEXED:
                idx_blocks = self.allocator.allocate(1)
                node.index_block = idx_blocks[0]
                idx_data = struct.pack(f">{len(blocks)}I", *blocks)
                self.disk.write_block(node.index_block, idx_data)

        self._persist_inode_table()
        self._persist_bitmap()
        self.disk.flush()
        self.checkpoint()

        elapsed = time.perf_counter() - start
        print(f"[FS] Defragmentation complete ({elapsed * 1000:.1f} ms, "
              f"{len(file_data)} files compacted)")

    # ──────────────────────────────────────────────────────────────────────
    # CRASH SIMULATION
    # ──────────────────────────────────────────────────────────────────────

    def simulate_crash(self, kind: str = "power_loss", **kwargs):
        """
        Inject a simulated fault.

        Parameters
        ----------
        kind : str
            'power_loss' – drop unflushed cache entries.
            'corrupt'    – corrupt a specific block (pass block_id=<int>).
            'partial'    – partial write (pass block_id=<int>, data=<bytes>).
        """
        if kind == "power_loss":
            self.disk.simulate_power_loss()
        elif kind == "corrupt":
            bid = kwargs.get("block_id", DATA_START)
            self.disk.corrupt_block(bid)
        elif kind == "partial":
            bid = kwargs.get("block_id", DATA_START)
            data = kwargs.get("data", b"\xff" * self.disk.block_size)
            self.disk.simulate_partial_write(bid, data)
        else:
            print(f"[FS] Unknown crash kind: {kind}")
            return
        self._mounted = False   # force re-mount
        print("[FS] System is now in CRASHED state. Run recover() or mount().")

    # ──────────────────────────────────────────────────────────────────────
    # RECOVERY
    # ──────────────────────────────────────────────────────────────────────

    def recover(self):
        """
        Attempt to restore consistency:
        1. Replay uncommitted journal entries.
        2. Restore metadata from checkpoint.
        3. Rebuild bitmap from inode table.
        """
        start = time.perf_counter()
        print("[RECOVERY] Starting recovery process …")

        # Step 1: replay journal
        uncommitted = self.journal.uncommitted_entries()
        if uncommitted:
            print(f"[RECOVERY] Replaying {len(uncommitted)} uncommitted journal entries …")
            for entry in uncommitted:
                self._replay_journal_entry(entry)
                self.journal.commit(entry.txn_id)
            print("[RECOVERY] Journal replay complete.")
        else:
            print("[RECOVERY] Journal clean – no uncommitted entries.")

        # Step 2: restore checkpoint metadata
        cp = self.checkpoint_mgr.load()
        if cp:
            print("[RECOVERY] Restoring from checkpoint …")
            # Directory tree
            if "directory_tree" in cp:
                self.dir_tree = DirectoryTree.deserialize(cp["directory_tree"])
            # Inodes
            if "inodes" in cp:
                self.inodes.clear()
                for iid_str, idata in cp["inodes"].items():
                    node = Inode(int(iid_str), FileType(idata["file_type"]),
                                AccessMethod(idata["access_method"]))
                    node.size = idata["size"]
                    node.created_at = idata["created_at"]
                    node.modified_at = idata["modified_at"]
                    node.block_pointers = idata["block_pointers"]
                    node.index_block = idata.get("index_block", -1)
                    self.inodes[int(iid_str)] = node
                self._next_inode_id = max(self.inodes.keys()) + 1 if self.inodes else 1
            print("[RECOVERY] Checkpoint restored.")
        else:
            print("[RECOVERY] No checkpoint found – starting fresh metadata.")
            self.inodes = {0: Inode(0, FileType.DIRECTORY)}
            self.dir_tree = DirectoryTree()

        # Step 3: rebuild bitmap from inodes
        self._rebuild_bitmap()
        self._persist_bitmap()
        self._persist_inode_table()
        self.disk.flush()

        self.disk.crashed = False
        self._mounted = True
        elapsed = time.perf_counter() - start
        self._recovery_time = elapsed * 1000
        print(f"[RECOVERY] Complete ({self._recovery_time:.1f} ms)")

    def _replay_journal_entry(self, entry):
        """Best-effort replay of a journal entry."""
        op = entry.op_type
        p = entry.payload
        print(f"  [REPLAY] {op}: {p}")
        # For our simulation, replay is mostly about ensuring metadata
        # consistency.  The actual data writes may have been lost (which is
        # the point – journaling protects *metadata*, not data in a typical
        # UNIX FS).
        #
        # In a real system we'd redo the block writes here.  For our
        # demonstration we simply acknowledge the entry.

    # ──────────────────────────────────────────────────────────────────────
    # CHECKPOINT
    # ──────────────────────────────────────────────────────────────────────

    def checkpoint(self):
        """Save a full metadata snapshot (inodes + directory tree + bitmap)."""
        self._check_mounted_or_recovering()
        metadata = {
            "inodes": {
                str(iid): {
                    "file_type": int(node.file_type),
                    "access_method": int(node.access_method),
                    "size": node.size,
                    "created_at": node.created_at,
                    "modified_at": node.modified_at,
                    "block_pointers": node.block_pointers,
                    "index_block": node.index_block,
                }
                for iid, node in self.inodes.items()
            },
            "directory_tree": self.dir_tree.serialize(),
            "bitmap": list(self.allocator.serialize()) if self.allocator else [],
            "next_inode_id": self._next_inode_id,
        }
        self.checkpoint_mgr.save(metadata)
        self.journal.truncate()

    # ──────────────────────────────────────────────────────────────────────
    # METRICS
    # ──────────────────────────────────────────────────────────────────────

    def get_metrics(self) -> dict:
        """Aggregate performance and utilization metrics."""
        disk_m = self.disk.metrics.summary()
        space = {}
        if self.allocator:
            space = {
                "total_blocks": self.allocator.total_blocks(),
                "used_blocks": self.allocator.used_count(),
                "free_blocks": self.allocator.free_count(),
                "utilization_%": round(self.allocator.utilization(), 2),
            }
        cp_m = self.checkpoint_mgr.metrics
        return {
            "disk": disk_m,
            "space": space,
            "recovery_time_ms": round(self._recovery_time, 2),
            "checkpoint": cp_m,
            "journal_entries": len(self.journal),
            "total_files": len([n for n in self.inodes.values()
                                if n.file_type == FileType.FILE]),
            "total_dirs": len([n for n in self.inodes.values()
                               if n.file_type == FileType.DIRECTORY]),
        }

    # ──────────────────────────────────────────────────────────────────────
    # INTERNAL HELPERS
    # ──────────────────────────────────────────────────────────────────────

    def _check_mounted(self):
        if not self._mounted:
            raise RuntimeError("File system not mounted. Call mount() or format() first.")

    def _check_mounted_or_recovering(self):
        pass  # allow during recovery

    def _alloc_inode_id(self) -> int:
        iid = self._next_inode_id
        self._next_inode_id += 1
        return iid

    # ── Superblock ───────────────────────────────────────────────────────

    def _make_superblock(self) -> bytes:
        data = bytearray(self.disk.block_size)
        data[0:4] = MAGIC
        struct.pack_into(">I", data, 4, VERSION)
        struct.pack_into(">I", data, 8, self.disk.total_blocks)
        struct.pack_into(">I", data, 12, self.disk.block_size)
        struct.pack_into(">I", data, 16, MAX_INODES)
        struct.pack_into(">I", data, 20, DATA_START)
        return bytes(data)

    # ── Bitmap persistence ───────────────────────────────────────────────

    def _persist_bitmap(self):
        raw = self.allocator.serialize()
        for i in range(BITMAP_BLOCKS):
            s = i * self.disk.block_size
            e = s + self.disk.block_size
            chunk = raw[s:e] if s < len(raw) else b""
            chunk = chunk.ljust(self.disk.block_size, b"\x00")
            self.disk.write_block(BITMAP_START + i, chunk)

    def _load_bitmap(self):
        raw = bytearray()
        for i in range(BITMAP_BLOCKS):
            raw.extend(self.disk.read_block(BITMAP_START + i))
        self.allocator = BitmapAllocator.deserialize(bytes(raw),
                                                     self.disk.total_blocks)

    def _rebuild_bitmap(self):
        """Rebuild the bitmap from the current inode table."""
        self.allocator = BitmapAllocator(self.disk.total_blocks,
                                         reserved=RESERVED_BLOCKS)
        for node in self.inodes.values():
            for bid in node.block_pointers:
                if self.allocator.is_free(bid):
                    # Mark as used
                    self.allocator._set(bid, True)
            if node.index_block >= 0 and self.allocator.is_free(node.index_block):
                self.allocator._set(node.index_block, True)

    # ── Inode persistence ────────────────────────────────────────────────

    def _persist_inode(self, node: Inode):
        """Write a single inode to the inode table on disk."""
        slot = node.inode_id
        block_idx = INODE_TABLE_START + (slot // INODES_PER_BLOCK)
        offset_in_block = (slot % INODES_PER_BLOCK) * INODE_SERIAL_SIZE

        block_data = bytearray(self.disk.read_block(block_idx))
        serialized = node.serialize()
        block_data[offset_in_block:offset_in_block + INODE_SERIAL_SIZE] = serialized
        self.disk.write_block(block_idx, bytes(block_data))

    def _persist_inode_table(self):
        """Rewrite the entire inode table."""
        for i in range(INODE_TABLE_BLOCKS):
            block_data = bytearray(self.disk.block_size)
            for j in range(INODES_PER_BLOCK):
                iid = i * INODES_PER_BLOCK + j
                if iid in self.inodes:
                    offset = j * INODE_SERIAL_SIZE
                    block_data[offset:offset + INODE_SERIAL_SIZE] = \
                        self.inodes[iid].serialize()
            self.disk.write_block(INODE_TABLE_START + i, bytes(block_data))

    def _load_inodes(self):
        """Read the inode table from disk."""
        self.inodes.clear()
        for i in range(INODE_TABLE_BLOCKS):
            block_data = self.disk.read_block(INODE_TABLE_START + i)
            for j in range(INODES_PER_BLOCK):
                offset = j * INODE_SERIAL_SIZE
                raw = block_data[offset:offset + INODE_SERIAL_SIZE]
                node = Inode.deserialize(bytes(raw))
                if node is not None:
                    self.inodes[node.inode_id] = node
        if self.inodes:
            self._next_inode_id = max(self.inodes.keys()) + 1
