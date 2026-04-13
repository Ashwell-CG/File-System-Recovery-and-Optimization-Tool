"""
cli.py – Interactive Command-Line Interface
=============================================
REPL-style shell for interacting with the File System Simulator.

Commands
--------
  format                     – Create a new file system
  mount                      – Mount an existing file system
  mkdir <path>               – Create a directory
  rmdir <path>               – Remove an empty directory
  create <path> [seq|dir|idx]– Create a file (access method)
  write <path> <text>        – Write text to a file
  read <path>                – Read file contents
  delete <path>              – Delete a file
  ls [path]                  – List directory contents
  tree                       – Print the full directory tree
  rename <path> <new_name>   – Rename a file or directory
  search <name>              – Search for a file/directory by name
  stat <path>                – Show metadata for a file/directory
  crash [power|corrupt|partial] [block_id]
                             – Simulate a disk failure
  recover                    – Run crash recovery
  checkpoint                 – Save a metadata checkpoint
  backup [label]             – Create a full disk backup
  restore <backup_name>      – Restore from a backup
  backups                    – List available backups
  defrag                     – Defragment the file system
  metrics                    – Display performance metrics
  help                       – Show this command list
  exit / quit                – Exit the CLI
"""

import sys
import os
import shlex

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from disk import VirtualDisk
from file_system import FileSystem
from inode import AccessMethod


# ── Helpers ──────────────────────────────────────────────────────────────────

ACCESS_MAP = {
    "seq": AccessMethod.SEQUENTIAL,
    "sequential": AccessMethod.SEQUENTIAL,
    "dir": AccessMethod.DIRECT,
    "direct": AccessMethod.DIRECT,
    "idx": AccessMethod.INDEXED,
    "indexed": AccessMethod.INDEXED,
}

BANNER = r"""
╔══════════════════════════════════════════════════════════════╗
║       File System Recovery & Optimization Simulator         ║
║                     Interactive CLI                         ║
╚══════════════════════════════════════════════════════════════╝
Type 'help' for a list of commands.
"""

HELP_TEXT = """
Available Commands:
───────────────────────────────────────────────────────────────
  format                        Create a new file system
  mount                         Mount existing file system
  mkdir <path>                  Create a directory
  rmdir <path>                  Remove empty directory
  create <path> [seq|dir|idx]   Create a file
  write <path> <text>           Write text to file
  read <path>                   Read file content
  delete <path>                 Delete a file
  ls [path]                     List directory contents
  tree                          Print directory tree
  rename <path> <new_name>      Rename file/directory
  search <name>                 Search by name
  stat <path>                   Show file/dir metadata
  crash [power|corrupt|partial] [block_id]
                                Simulate a disk failure
  recover                       Run crash recovery
  checkpoint                    Save metadata checkpoint
  backup [label]                Create disk backup
  restore <backup_name>         Restore from backup
  backups                       List available backups
  defrag                        Defragment the disk
  metrics                       Show performance metrics
  help                          Show this help
  exit / quit                   Exit
───────────────────────────────────────────────────────────────
"""


def print_metrics(m: dict):
    """Pretty-print the metrics dictionary."""
    print("\n╔══════════════ Performance Metrics ══════════════╗")

    d = m.get("disk", {})
    print(f"  Disk Reads       : {d.get('total_reads', 0)}")
    print(f"  Disk Writes      : {d.get('total_writes', 0)}")
    print(f"  Avg Read Time    : {d.get('avg_read_time_ms', 0):.3f} ms")
    print(f"  Avg Write Time   : {d.get('avg_write_time_ms', 0):.3f} ms")
    print(f"  Cache Hit Rate   : {d.get('cache_hit_rate', 0):.1f}%")

    s = m.get("space", {})
    print(f"\n  Total Blocks     : {s.get('total_blocks', '?')}")
    print(f"  Used Blocks      : {s.get('used_blocks', '?')}")
    print(f"  Free Blocks      : {s.get('free_blocks', '?')}")
    print(f"  Utilization      : {s.get('utilization_%', '?')}%")

    print(f"\n  Recovery Time    : {m.get('recovery_time_ms', 0):.2f} ms")
    print(f"  Journal Entries  : {m.get('journal_entries', 0)}")
    print(f"  Total Files      : {m.get('total_files', 0)}")
    print(f"  Total Dirs       : {m.get('total_dirs', 0)}")

    cp = m.get("checkpoint", {})
    print(f"  Checkpoints Made : {cp.get('checkpoint_count', 0)}")
    print(f"  Last CP Duration : {cp.get('last_checkpoint_duration_ms', 0):.1f} ms")

    print("╚═════════════════════════════════════════════════╝\n")


# ── Main REPL ────────────────────────────────────────────────────────────────

def main():
    print(BANNER)
    disk = VirtualDisk()
    fs = FileSystem(disk)

    while True:
        try:
            raw = input("fs> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not raw:
            continue

        try:
            parts = shlex.split(raw)
        except ValueError:
            parts = raw.split()

        cmd = parts[0].lower()
        args = parts[1:]

        try:
            # ── Lifecycle ────────────────────────────────────────────────
            if cmd == "format":
                fs.format()

            elif cmd == "mount":
                fs.mount()

            # ── Directory ops ────────────────────────────────────────────
            elif cmd == "mkdir":
                if not args:
                    print("Usage: mkdir <path>")
                else:
                    fs.mkdir(args[0])

            elif cmd == "rmdir":
                if not args:
                    print("Usage: rmdir <path>")
                else:
                    fs.rmdir(args[0])

            elif cmd == "ls":
                path = args[0] if args else "/"
                entries = fs.ls(path)
                if not entries:
                    print("  (empty)")
                for e in entries:
                    kind = "DIR " if e.is_dir else "FILE"
                    print(f"  [{kind}] {e.name}  (inode {e.inode_id})")

            elif cmd == "tree":
                print("/")
                fs.dir_tree.print_tree()

            # ── File ops ─────────────────────────────────────────────────
            elif cmd == "create":
                if not args:
                    print("Usage: create <path> [seq|dir|idx]")
                else:
                    path = args[0]
                    method = AccessMethod.SEQUENTIAL
                    if len(args) > 1:
                        method = ACCESS_MAP.get(args[1].lower(), AccessMethod.SEQUENTIAL)
                    fs.create_file(path, method)

            elif cmd == "write":
                if len(args) < 2:
                    print("Usage: write <path> <text...>")
                else:
                    path = args[0]
                    text = " ".join(args[1:])
                    fs.write_file(path, text.encode("utf-8"))

            elif cmd == "read":
                if not args:
                    print("Usage: read <path>")
                else:
                    data = fs.read_file(args[0])
                    print(f"  Content ({len(data)} bytes):")
                    print(f"  {data.decode('utf-8', errors='replace')}")

            elif cmd == "delete":
                if not args:
                    print("Usage: delete <path>")
                else:
                    fs.delete_file(args[0])

            elif cmd == "rename":
                if len(args) < 2:
                    print("Usage: rename <path> <new_name>")
                else:
                    fs.rename(args[0], args[1])

            elif cmd == "search":
                if not args:
                    print("Usage: search <name>")
                else:
                    results = fs.search(args[0])
                    if results:
                        for r in results:
                            print(f"  Found: {r}")
                    else:
                        print("  No matches found.")

            elif cmd == "stat":
                if not args:
                    print("Usage: stat <path>")
                else:
                    info = fs.stat(args[0])
                    for k, v in info.items():
                        print(f"  {k:15s}: {v}")

            # ── Crash & Recovery ─────────────────────────────────────────
            elif cmd == "crash":
                kind = args[0] if args else "power"
                kind_map = {"power": "power_loss", "corrupt": "corrupt",
                            "partial": "partial"}
                crash_kind = kind_map.get(kind, "power_loss")
                kwargs = {}
                if len(args) > 1:
                    kwargs["block_id"] = int(args[1])
                fs.simulate_crash(crash_kind, **kwargs)

            elif cmd == "recover":
                fs.recover()

            elif cmd == "checkpoint":
                fs.checkpoint()

            # ── Backup ───────────────────────────────────────────────────
            elif cmd == "backup":
                label = args[0] if args else ""
                fs.backup_mgr.create_backup(label)

            elif cmd == "restore":
                if not args:
                    print("Usage: restore <backup_name>")
                else:
                    fs.backup_mgr.restore_backup(args[0])
                    fs.mount()

            elif cmd == "backups":
                bk = fs.backup_mgr.list_backups()
                if bk:
                    for b in bk:
                        print(f"  {b}")
                else:
                    print("  No backups found.")

            # ── Optimization ─────────────────────────────────────────────
            elif cmd == "defrag":
                fs.defragment()

            # ── Metrics ──────────────────────────────────────────────────
            elif cmd == "metrics":
                print_metrics(fs.get_metrics())

            # ── Help / Exit ──────────────────────────────────────────────
            elif cmd == "help":
                print(HELP_TEXT)

            elif cmd in ("exit", "quit"):
                # Flush before exit
                try:
                    fs.disk.flush()
                    fs.checkpoint()
                except Exception:
                    pass
                print("Goodbye!")
                break

            else:
                print(f"Unknown command: '{cmd}'. Type 'help' for options.")

        except Exception as e:
            print(f"  Error: {e}")


if __name__ == "__main__":
    main()
