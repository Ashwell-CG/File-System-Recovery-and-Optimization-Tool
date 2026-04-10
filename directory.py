"""
directory.py – Hierarchical Directory Structure
=================================================
Implements a tree-based directory structure where every node is either
a file or a subdirectory.  Each directory entry maps a *name* to an
*inode_id*.  The root directory always has inode 0.

Operations: create, delete, rename, search, list, and path resolution.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Tuple


class DirectoryEntry:
    """A single entry inside a directory: maps name → inode_id."""

    def __init__(self, name: str, inode_id: int, is_dir: bool = False):
        self.name = name
        self.inode_id = inode_id
        self.is_dir = is_dir

    def __repr__(self):
        kind = "DIR" if self.is_dir else "FILE"
        return f"{kind} '{self.name}' -> inode {self.inode_id}"


class DirectoryTree:
    """
    In-memory representation of the full directory hierarchy.

    Internally stored as a dict mapping each inode (directory) to
    its list of DirectoryEntry children.
    """

    ROOT_INODE = 0

    def __init__(self):
        # inode_id → list of DirectoryEntry
        self._dirs: Dict[int, List[DirectoryEntry]] = {self.ROOT_INODE: []}

    # ── Path Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _split_path(path: str) -> List[str]:
        """Split an absolute path like '/foo/bar' into ['foo', 'bar']."""
        parts = [p for p in path.strip("/").split("/") if p]
        return parts

    def resolve_path(self, path: str) -> Tuple[Optional[int], bool]:
        """
        Walk the directory tree to find the inode for *path*.

        Returns (inode_id, is_dir) or (None, False) if not found.
        """
        if path == "/":
            return (self.ROOT_INODE, True)
        parts = self._split_path(path)
        current_inode = self.ROOT_INODE
        for i, part in enumerate(parts):
            children = self._dirs.get(current_inode, [])
            found = False
            for entry in children:
                if entry.name == part:
                    current_inode = entry.inode_id
                    found = True
                    is_dir = entry.is_dir
                    break
            if not found:
                return (None, False)
        return (current_inode, is_dir)

    def resolve_parent(self, path: str) -> Tuple[Optional[int], str]:
        """
        Return (parent_inode, child_name) for a given path.
        E.g. '/foo/bar/baz' → (inode_of_/foo/bar, 'baz')
        """
        parts = self._split_path(path)
        if not parts:
            return (None, "")
        child_name = parts[-1]
        parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
        parent_inode, is_dir = self.resolve_path(parent_path)
        if parent_inode is None or not is_dir:
            return (None, child_name)
        return (parent_inode, child_name)

    # ── Directory Operations ─────────────────────────────────────────────

    def create_entry(self, parent_inode: int, name: str,
                     inode_id: int, is_dir: bool = False):
        """Add a new child entry to an existing directory."""
        if parent_inode not in self._dirs:
            raise FileNotFoundError(f"Parent inode {parent_inode} is not a directory")
        # Check for duplicate
        for e in self._dirs[parent_inode]:
            if e.name == name:
                raise FileExistsError(f"'{name}' already exists in directory (inode {parent_inode})")
        entry = DirectoryEntry(name, inode_id, is_dir)
        self._dirs[parent_inode].append(entry)
        if is_dir:
            self._dirs[inode_id] = []

    def delete_entry(self, parent_inode: int, name: str) -> Optional[DirectoryEntry]:
        """Remove a child entry. Returns the removed entry or None."""
        children = self._dirs.get(parent_inode, [])
        for i, e in enumerate(children):
            if e.name == name:
                removed = children.pop(i)
                # If it was a directory, also remove its children mapping
                if removed.is_dir:
                    self._remove_subtree(removed.inode_id)
                return removed
        return None

    def _remove_subtree(self, inode_id: int):
        """Recursively remove a directory and all its descendants."""
        children = self._dirs.pop(inode_id, [])
        for entry in children:
            if entry.is_dir:
                self._remove_subtree(entry.inode_id)

    def rename_entry(self, parent_inode: int, old_name: str, new_name: str) -> bool:
        """Rename a child entry inside a directory. Returns True on success."""
        children = self._dirs.get(parent_inode, [])
        # Check new name doesn't exist
        for e in children:
            if e.name == new_name:
                raise FileExistsError(f"'{new_name}' already exists")
        for e in children:
            if e.name == old_name:
                e.name = new_name
                return True
        return False

    def list_dir(self, inode_id: int) -> List[DirectoryEntry]:
        """List all entries in a directory."""
        if inode_id not in self._dirs:
            raise FileNotFoundError(f"Inode {inode_id} is not a directory")
        return list(self._dirs[inode_id])

    def search(self, name: str, start_inode: int = 0) -> List[str]:
        """
        Recursively search for entries matching *name* (exact match).
        Returns a list of absolute paths.
        """
        results: List[str] = []
        self._search_recursive(name, start_inode, "/", results)
        return results

    def _search_recursive(self, name: str, inode_id: int,
                          current_path: str, results: List[str]):
        for entry in self._dirs.get(inode_id, []):
            child_path = current_path.rstrip("/") + "/" + entry.name
            if entry.name == name:
                results.append(child_path)
            if entry.is_dir:
                self._search_recursive(name, entry.inode_id, child_path, results)

    # ── Serialization ────────────────────────────────────────────────────

    def serialize(self) -> dict:
        """Convert directory tree to a JSON-serializable dict."""
        data = {}
        for inode_id, entries in self._dirs.items():
            data[str(inode_id)] = [
                {"name": e.name, "inode_id": e.inode_id, "is_dir": e.is_dir}
                for e in entries
            ]
        return data

    @classmethod
    def deserialize(cls, data: dict) -> "DirectoryTree":
        tree = cls()
        tree._dirs.clear()
        for inode_str, entries in data.items():
            inode_id = int(inode_str)
            tree._dirs[inode_id] = [
                DirectoryEntry(e["name"], e["inode_id"], e["is_dir"])
                for e in entries
            ]
        return tree

    # ── Utility ──────────────────────────────────────────────────────────

    def all_file_inodes(self, start_inode: int = 0) -> List[int]:
        """Return inode IDs of every FILE in the tree (recursive)."""
        result: List[int] = []
        for entry in self._dirs.get(start_inode, []):
            if entry.is_dir:
                result.extend(self.all_file_inodes(entry.inode_id))
            else:
                result.append(entry.inode_id)
        return result

    def all_dir_inodes(self) -> List[int]:
        """Return inode IDs of every directory (including root)."""
        return list(self._dirs.keys())

    def print_tree(self, inode_id: int = 0, prefix: str = "", path: str = "/"):
        """Pretty-print the directory hierarchy."""
        entries = self._dirs.get(inode_id, [])
        for i, entry in enumerate(entries):
            connector = "└── " if i == len(entries) - 1 else "├── "
            kind = "📁" if entry.is_dir else "📄"
            print(f"{prefix}{connector}{kind} {entry.name}  (inode {entry.inode_id})")
            if entry.is_dir:
                extension = "    " if i == len(entries) - 1 else "│   "
                self.print_tree(entry.inode_id, prefix + extension,
                                path.rstrip("/") + "/" + entry.name)
