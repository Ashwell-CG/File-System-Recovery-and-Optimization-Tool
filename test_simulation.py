"""
test_simulation.py – Automated Crash & Recovery Demonstration
===============================================================
Runs a scripted sequence that exercises every major subsystem:

  1. Format a fresh file system
  2. Create directories and files (all three access methods)
  3. Write data, read it back, verify correctness
  4. Measure I/O performance
  5. Defragment and compare performance
  6. Simulate a crash (power loss mid-write)
  7. Run recovery and verify data consistency
  8. Simulate block corruption → recover from checkpoint
  9. Create and restore a backup
 10. Print final metrics summary

Usage
-----
    python test_simulation.py
"""

import os
import sys
import time

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from disk import VirtualDisk
from file_system import FileSystem
from inode import AccessMethod

DIVIDER = "=" * 65


def banner(title: str):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def assert_eq(label, actual, expected):
    if actual == expected:
        print(f"  ✓ {label}")
    else:
        print(f"  ✗ {label}")
        print(f"    Expected: {expected!r}")
        print(f"    Got:      {actual!r}")


def run():
    # ── Cleanup previous artifacts ───────────────────────────────────────
    for f in ["virtual_disk.bin", "fs_journal.log", "fs_checkpoint.json"]:
        if os.path.isfile(f):
            os.remove(f)

    disk = VirtualDisk()
    fs = FileSystem(disk)

    # ─────────────────────────────────────────────────────────────────────
    banner("1. FORMAT FILE SYSTEM")
    # ─────────────────────────────────────────────────────────────────────
    fs.format()
    print("  File system formatted successfully.")

    # ─────────────────────────────────────────────────────────────────────
    banner("2. CREATE DIRECTORIES")
    # ─────────────────────────────────────────────────────────────────────
    fs.mkdir("/documents")
    fs.mkdir("/documents/reports")
    fs.mkdir("/images")
    fs.mkdir("/logs")
    print("  Created: /documents, /documents/reports, /images, /logs")

    # ─────────────────────────────────────────────────────────────────────
    banner("3. CREATE FILES (three access methods)")
    # ─────────────────────────────────────────────────────────────────────
    fs.create_file("/documents/readme.txt", AccessMethod.SEQUENTIAL)
    fs.create_file("/documents/reports/q1.csv", AccessMethod.DIRECT)
    fs.create_file("/images/logo.dat", AccessMethod.INDEXED)
    fs.create_file("/logs/system.log", AccessMethod.SEQUENTIAL)
    print("  4 files created with SEQUENTIAL, DIRECT, and INDEXED access.")

    # ─────────────────────────────────────────────────────────────────────
    banner("4. WRITE DATA & VERIFY")
    # ─────────────────────────────────────────────────────────────────────
    test_data = {
        "/documents/readme.txt": b"Hello, this is the README for the project.\n" * 50,
        "/documents/reports/q1.csv": b"id,name,value\n" + b"1,alpha,100\n" * 200,
        "/images/logo.dat": bytes(range(256)) * 40,   # ~10 KB binary
        "/logs/system.log": b"[INFO] System started.\n" * 100,
    }

    for path, data in test_data.items():
        fs.write_file(path, data)

    # Read back and verify
    all_ok = True
    for path, expected in test_data.items():
        actual = fs.read_file(path)
        if actual == expected:
            print(f"  ✓ {path} – {len(expected)} bytes verified")
        else:
            print(f"  ✗ {path} – MISMATCH!")
            all_ok = False
    if all_ok:
        print("  All writes verified successfully.")

    # ─────────────────────────────────────────────────────────────────────
    banner("5. DIRECTORY TREE")
    # ─────────────────────────────────────────────────────────────────────
    print("/")
    fs.dir_tree.print_tree()

    # ─────────────────────────────────────────────────────────────────────
    banner("6. PERFORMANCE BEFORE DEFRAG")
    # ─────────────────────────────────────────────────────────────────────
    m_before = fs.get_metrics()
    print(f"  Reads:  {m_before['disk']['total_reads']}")
    print(f"  Writes: {m_before['disk']['total_writes']}")
    print(f"  Avg Read:  {m_before['disk']['avg_read_time_ms']:.3f} ms")
    print(f"  Avg Write: {m_before['disk']['avg_write_time_ms']:.3f} ms")
    print(f"  Space used: {m_before['space']['utilization_%']}%")

    # ─────────────────────────────────────────────────────────────────────
    banner("7. DEFRAGMENTATION")
    # ─────────────────────────────────────────────────────────────────────
    # First delete a file to create fragmentation
    fs.delete_file("/logs/system.log")
    # Write a new large file that will scatter into freed gaps
    fs.create_file("/logs/access.log", AccessMethod.SEQUENTIAL)
    fs.write_file("/logs/access.log", b"GET /index.html 200\n" * 500)
    print("  Created fragmentation by deleting and rewriting.")

    # Now defragment
    fs.defragment()

    # Read back to confirm integrity after defrag
    for path in ["/documents/readme.txt", "/documents/reports/q1.csv",
                 "/images/logo.dat", "/logs/access.log"]:
        data = fs.read_file(path)
        print(f"  ✓ {path} readable after defrag ({len(data)} bytes)")

    # ─────────────────────────────────────────────────────────────────────
    banner("8. SIMULATE CRASH – Power Loss")
    # ─────────────────────────────────────────────────────────────────────
    print("  Writing new data (will be lost in crash) …")
    # Start a large write, then crash before flush completes
    fs.create_file("/documents/important.txt", AccessMethod.SEQUENTIAL)
    # Write some data that WILL be committed (flushed)
    fs.write_file("/documents/important.txt", b"This data is safe.\n" * 100)
    print("  Committed write of 'important.txt'.")

    # Now checkpoint so that important.txt survives recovery
    fs.checkpoint()

    # Start another write that will NOT survive
    print("  Starting a write that will be LOST …")
    # We manually push data to cache but crash before flush
    fs.create_file("/documents/doomed.txt", AccessMethod.SEQUENTIAL)
    fs.write_file("/documents/doomed.txt", b"This will be lost!\n" * 50)

    # CRASH!
    fs.simulate_crash("power_loss")
    print("  System CRASHED.")

    # ─────────────────────────────────────────────────────────────────────
    banner("9. RECOVERY")
    # ─────────────────────────────────────────────────────────────────────
    fs.recover()

    # After recovery, 'important.txt' (checkpointed) should survive
    # 'doomed.txt' may or may not survive depending on journal state
    try:
        data = fs.read_file("/documents/important.txt")
        print(f"  ✓ important.txt recovered ({len(data)} bytes)")
    except Exception as e:
        print(f"  ✗ important.txt NOT recovered: {e}")

    # ─────────────────────────────────────────────────────────────────────
    banner("10. SIMULATE CRASH – Block Corruption")
    # ─────────────────────────────────────────────────────────────────────
    # Get blocks of readme.txt and corrupt one
    info = fs.stat("/documents/readme.txt")
    blocks = info["blocks"]
    if blocks:
        target_block = blocks[0]
        fs.simulate_crash("corrupt", block_id=target_block)
        print(f"  Corrupted block {target_block} (first block of readme.txt)")

        # Try to read – data will be garbage
        try:
            data = fs.read_file("/documents/readme.txt")
            print(f"  Read returned {len(data)} bytes (may contain corruption)")
        except Exception as e:
            print(f"  Read failed: {e}")

        # Recover from checkpoint
        fs.recover()
        print("  Recovery from checkpoint completed.")

    # ─────────────────────────────────────────────────────────────────────
    banner("11. BACKUP & RESTORE")
    # ─────────────────────────────────────────────────────────────────────
    backup_name = fs.backup_mgr.create_backup("pre_test")
    print(f"  Backup created: {backup_name}")

    # Delete a file
    try:
        fs.delete_file("/images/logo.dat")
        print("  Deleted /images/logo.dat")
    except Exception:
        pass

    # Restore backup
    fs.backup_mgr.restore_backup(backup_name)
    fs.mount()
    try:
        data = fs.read_file("/images/logo.dat")
        print(f"  ✓ /images/logo.dat restored ({len(data)} bytes)")
    except FileNotFoundError:
        print("  ✗ /images/logo.dat not found after restore")

    # ─────────────────────────────────────────────────────────────────────
    banner("12. SEARCH & RENAME")
    # ─────────────────────────────────────────────────────────────────────
    results = fs.search("readme.txt")
    print(f"  Search 'readme.txt': {results}")

    try:
        fs.rename("/documents/readme.txt", "README.md")
        print("  Renamed readme.txt → README.md")
    except Exception as e:
        print(f"  Rename error: {e}")

    results = fs.search("README.md")
    print(f"  Search 'README.md': {results}")

    # ─────────────────────────────────────────────────────────────────────
    banner("13. FINAL METRICS")
    # ─────────────────────────────────────────────────────────────────────
    m = fs.get_metrics()
    print(f"  Total Reads       : {m['disk']['total_reads']}")
    print(f"  Total Writes      : {m['disk']['total_writes']}")
    print(f"  Avg Read Time     : {m['disk']['avg_read_time_ms']:.3f} ms")
    print(f"  Avg Write Time    : {m['disk']['avg_write_time_ms']:.3f} ms")
    print(f"  Cache Hit Rate    : {m['disk']['cache_hit_rate']:.1f}%")
    print(f"  Space Utilization : {m['space']['utilization_%']}%")
    print(f"  Recovery Time     : {m['recovery_time_ms']:.2f} ms")
    print(f"  Files             : {m['total_files']}")
    print(f"  Directories       : {m['total_dirs']}")

    print(f"\n{DIVIDER}")
    print("  SIMULATION COMPLETE – All tests executed.")
    print(DIVIDER)


if __name__ == "__main__":
    run()
