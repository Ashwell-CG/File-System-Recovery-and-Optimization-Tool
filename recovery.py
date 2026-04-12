"""
recovery.py – Journaling, Checkpointing, and Backup Restore
=============================================================
Provides a Write-Ahead Log (WAL) journal that records intended disk
mutations *before* they are committed.  On crash recovery the journal
is replayed (redo) to bring the disk back to a consistent state.

Also supports periodic **checkpoints** (full metadata snapshots) and
**backup / restore** of the entire virtual disk image.
"""

from __future__ import annotations
import json
import os
import shutil
import struct
import time
from typing import Any, Dict, List, Optional

from disk import VirtualDisk


# ── Journal Entry ────────────────────────────────────────────────────────────

class JournalEntry:
    """
    A single log record describing one atomic operation.

    Fields
    ------
    txn_id      : monotonically increasing transaction ID
    op_type     : string describing the operation (e.g. "WRITE_BLOCK",
                  "ALLOC_BLOCK", "FREE_BLOCK", "UPDATE_INODE", "UPDATE_DIR")
    payload     : dict of operation-specific data
    committed   : True once the operation has been applied to disk
    timestamp   : wall-clock time of the log record
    """

    def __init__(self, txn_id: int, op_type: str, payload: dict):
        self.txn_id = txn_id
        self.op_type = op_type
        self.payload = payload
        self.committed = False
        self.timestamp = time.time()

    def to_dict(self) -> dict:
        return {
            "txn_id": self.txn_id,
            "op_type": self.op_type,
            "payload": self.payload,
            "committed": self.committed,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "JournalEntry":
        entry = cls(d["txn_id"], d["op_type"], d["payload"])
        entry.committed = d.get("committed", False)
        entry.timestamp = d.get("timestamp", 0)
        return entry

    def __repr__(self):
        status = "✓" if self.committed else "✗"
        return f"[{status}] TXN-{self.txn_id} {self.op_type}"


# ── Journal (Write-Ahead Log) ───────────────────────────────────────────────

JOURNAL_FILE = "fs_journal.log"


class Journal:
    """
    Append-only Write-Ahead Log backed by a JSON-lines file.

    Protocol
    --------
    1. **Begin**: ``log(op, payload)`` → writes an uncommitted record.
    2. **Commit**: ``commit(txn_id)`` → marks the record as committed.
    3. **Recovery**: ``replay_uncommitted()`` returns entries that were
       logged but never committed, so the file-system layer can redo them.
    """

    def __init__(self, journal_path: str = JOURNAL_FILE):
        self.path = journal_path
        self._next_txn: int = 1
        self._entries: List[JournalEntry] = []
        self._load()

    # ── Persistence ──────────────────────────────────────────────────────

    def _load(self):
        """Load existing journal entries from disk."""
        if not os.path.isfile(self.path):
            return
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    entry = JournalEntry.from_dict(json.loads(line))
                    self._entries.append(entry)
                    if entry.txn_id >= self._next_txn:
                        self._next_txn = entry.txn_id + 1

    def _append_to_file(self, entry: JournalEntry):
        with open(self.path, "a") as f:
            f.write(json.dumps(entry.to_dict()) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _rewrite(self):
        """Rewrite the entire journal file (used after commit / truncate)."""
        with open(self.path, "w") as f:
            for e in self._entries:
                f.write(json.dumps(e.to_dict()) + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ── Public API ───────────────────────────────────────────────────────

    def log(self, op_type: str, payload: dict) -> int:
        """Log an operation *before* it is applied. Returns txn_id."""
        txn_id = self._next_txn
        self._next_txn += 1
        entry = JournalEntry(txn_id, op_type, payload)
        self._entries.append(entry)
        self._append_to_file(entry)
        return txn_id

    def commit(self, txn_id: int):
        """Mark a previously logged operation as committed."""
        for e in self._entries:
            if e.txn_id == txn_id:
                e.committed = True
                self._rewrite()
                return
        raise KeyError(f"Transaction {txn_id} not found in journal")

    def uncommitted_entries(self) -> List[JournalEntry]:
        """Return entries that were logged but NOT committed (need redo)."""
        return [e for e in self._entries if not e.committed]

    def committed_entries(self) -> List[JournalEntry]:
        return [e for e in self._entries if e.committed]

    def truncate(self):
        """Clear the journal (called after a successful checkpoint)."""
        self._entries.clear()
        if os.path.isfile(self.path):
            open(self.path, "w").close()

    def __len__(self):
        return len(self._entries)

    def __repr__(self):
        committed = sum(1 for e in self._entries if e.committed)
        return (f"Journal(entries={len(self._entries)}, "
                f"committed={committed}, pending={len(self._entries) - committed})")


# ── Checkpoint ───────────────────────────────────────────────────────────────

CHECKPOINT_FILE = "fs_checkpoint.json"


class CheckpointManager:
    """
    Periodically saves a full metadata snapshot (inode table, directory
    tree, bitmap) so recovery can start from the checkpoint instead of
    replaying the entire journal.
    """

    def __init__(self, checkpoint_path: str = CHECKPOINT_FILE):
        self.path = checkpoint_path
        self.metrics: Dict[str, Any] = {
            "last_checkpoint_time": None,
            "checkpoint_count": 0,
            "last_checkpoint_duration_ms": 0,
        }

    def save(self, metadata: dict):
        """
        Persist a metadata snapshot.

        Parameters
        ----------
        metadata : dict
            Must include keys: 'inodes', 'directory_tree', 'bitmap'.
        """
        start = time.perf_counter()
        metadata["checkpoint_time"] = time.time()
        with open(self.path, "w") as f:
            json.dump(metadata, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        elapsed = time.perf_counter() - start
        self.metrics["last_checkpoint_time"] = metadata["checkpoint_time"]
        self.metrics["checkpoint_count"] += 1
        self.metrics["last_checkpoint_duration_ms"] = elapsed * 1000
        print(f"[CHECKPOINT] Metadata saved ({elapsed * 1000:.1f} ms)")

    def load(self) -> Optional[dict]:
        """Load the most recent checkpoint, or None if absent."""
        if not os.path.isfile(self.path):
            return None
        with open(self.path, "r") as f:
            return json.load(f)

    def exists(self) -> bool:
        return os.path.isfile(self.path)


# ── Backup / Restore ────────────────────────────────────────────────────────

BACKUP_DIR = "backups"


class BackupManager:
    """Full-image backup and restore of the virtual disk file."""

    def __init__(self, disk: VirtualDisk, backup_dir: str = BACKUP_DIR):
        self.disk = disk
        self.backup_dir = backup_dir
        os.makedirs(self.backup_dir, exist_ok=True)

    def create_backup(self, label: str = "") -> str:
        """
        Copy the current virtual disk file to the backup directory.
        Returns the backup filename.
        """
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        suffix = f"_{label}" if label else ""
        backup_name = f"backup_{timestamp}{suffix}.bin"
        dest = os.path.join(self.backup_dir, backup_name)

        # Flush cache before backup
        self.disk.flush()
        shutil.copy2(self.disk.filepath, dest)

        # Also backup metadata files
        for meta_file in [JOURNAL_FILE, CHECKPOINT_FILE]:
            if os.path.isfile(meta_file):
                shutil.copy2(meta_file, os.path.join(self.backup_dir,
                             f"{backup_name}_{meta_file}"))

        print(f"[BACKUP] Created: {backup_name}")
        return backup_name

    def restore_backup(self, backup_name: str):
        """Restore a previously created backup."""
        src = os.path.join(self.backup_dir, backup_name)
        if not os.path.isfile(src):
            raise FileNotFoundError(f"Backup '{backup_name}' not found")
        shutil.copy2(src, self.disk.filepath)

        # Restore metadata if available
        for meta_file in [JOURNAL_FILE, CHECKPOINT_FILE]:
            meta_backup = os.path.join(self.backup_dir, f"{backup_name}_{meta_file}")
            if os.path.isfile(meta_backup):
                shutil.copy2(meta_backup, meta_file)

        if self.disk.cache:
            self.disk.cache.clear()
        self.disk._crashed = False
        print(f"[BACKUP] Restored: {backup_name}")

    def list_backups(self) -> List[str]:
        """Return names of available disk backups."""
        return sorted(
            f for f in os.listdir(self.backup_dir)
            if f.startswith("backup_") and f.endswith(".bin")
        )
