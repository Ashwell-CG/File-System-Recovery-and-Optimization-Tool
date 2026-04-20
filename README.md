# File System Recovery & Optimization Simulator

> A **C-language operating system project** that simulates a real-world file system with crash recovery, journaling, defragmentation, and disk optimization — written from scratch at the systems level.

---

## Overview

This project demonstrates how modern file systems work internally by implementing a complete virtual disk and file system in **C (C11 standard)**. Unlike interpreted implementations, this runs as native machine code — the same level at which real operating systems (Linux, Windows NT) operate.

The simulator creates a **10 MB virtual hard disk** as a binary file on your computer and lets you create folders, write files, simulate power failures, and recover data — all through an interactive terminal.

---

## Key Features

### 🖴 Virtual Disk Simulation
- Simulates a physical disk using a **10 MB binary file** (`c_virtual_disk.bin`)
- Fixed-size block architecture: **2,560 blocks × 4 KB each**
- **64-block LRU write-back cache** for high-speed access
- Performance metrics: read/write counts, latency, cache hit rate

### 📁 File & Directory Management
- Create, delete, rename, and search files and folders
- **Hierarchical directory tree** (folders inside folders)
- Inode-based metadata storage (exactly **256 bytes per inode**)
- Supports up to **1,024 files/directories** simultaneously

### 📂 Three File Access Methods

| Method | How It Works | Best For |
|--------|-------------|----------|
| **Sequential** | Read blocks in order | Log files, audio streams |
| **Direct** | Inode points directly to each block | Small/medium files |
| **Indexed** | Inode → Index Block → Data Blocks | Large files (UNIX-style) |

### 🗺️ Free Space Management
- **Bitmap allocator**: 320 bytes for 2,560 blocks (1 bit per block)
- Fast O(n) block search and allocation

### 🛡️ Three-Layer Crash Recovery

| Layer | File | Description |
|-------|------|-------------|
| **Journal** | `fs_journal.bin` | Write-Ahead Log (WAL) — records operations before executing |
| **Checkpoint** | `fs_checkpoint.bin` | Full binary snapshot of all metadata (~2.5 MB) |
| **Backup** | `backups/` folder | Complete copy of the entire virtual disk |

### ⚡ Performance Optimization
- Disk caching (LRU eviction with 64 slots)
- Full **disk defragmentation** — packs fragmented files contiguously
- Real-time metrics: reads, writes, latency, cache hit rate, utilization

---

## Project Structure

```
📦 File-System-Recovery-and-Optimization-Tool/
 ┣ 📜 disk.h / disk.c           → Virtual disk + LRU block cache
 ┣ 📜 bitmap.h / bitmap.c       → Free space manager (bitmap)
 ┣ 📜 inode.h / inode.c         → File metadata (256 bytes/inode)
 ┣ 📜 directory.h / directory.c → Hierarchical folder tree
 ┣ 📜 recovery.h / recovery.c   → Journal, checkpoint, backup/restore
 ┣ 📜 fs.h / fs.c               → Central file system controller
 ┣ 📜 main.c                    → Interactive CLI (fs> prompt)
 ┣ 📜 test.c                    → Automated 13-stage test suite
 ┣ 📜 Makefile                  → GCC build instructions
 ┗ 📜 C_Complete_Explanation.md → Full beginner-friendly documentation
```

**Generated at runtime (not committed):**
```
 ┣ 💾 c_virtual_disk.bin        → The 10 MB virtual hard disk
 ┣ 📝 fs_journal.bin            → Write-ahead recovery log
 ┣ 📸 fs_checkpoint.bin         → Metadata snapshot (~2.5 MB)
 ┗ 📁 backups/                  → Full disk backup copies
```

---

## Building & Running

### Prerequisites
- **GCC** (MinGW on Windows via [MSYS2](https://www.msys2.org/))
- **make** (optional, included with MSYS2)

### Step 1: Clone the Repository

```bash
git clone https://github.com/Ashwell-CG/File-System-Recovery-and-Optimization-Tool.git
cd File-System-Recovery-and-Optimization-Tool
```

### Step 2: Compile

**Using Make:**
```bash
make
```

**Or manually with GCC:**
```bash
gcc -O2 -std=c11 disk.c bitmap.c inode.c directory.c recovery.c fs.c main.c -o fs_sim.exe
gcc -O2 -std=c11 disk.c bitmap.c inode.c directory.c recovery.c fs.c test.c -o fs_test.exe
```

> **Windows (PowerShell):** Add GCC to PATH first:
> ```powershell
> $env:PATH = "C:\msys64\mingw64\bin;" + $env:PATH
> ```

### Step 3: Run the Interactive Simulator

```bash
./fs_sim.exe        # Linux/macOS
.\fs_sim.exe        # Windows PowerShell
```

### Step 4: Run the Automated 13-Stage Test

```bash
./fs_test.exe
```

---

## CLI Commands

Once inside the `fs>` prompt:

```
fs> format                          # Create fresh file system
fs> mkdir docs                      # Create a folder (/ auto-added)
fs> mkdir docs/reports              # Create a subfolder
fs> create docs/notes.txt           # Create a file (seq/dir/idx optional)
fs> write docs/notes.txt Hello!     # Write text to a file
fs> read docs/notes.txt             # Read file content
fs> tree                            # Print full directory tree
fs> stat docs/notes.txt             # Show file metadata (inode info)
fs> search notes.txt                # Search for a file by name
fs> rename docs/notes.txt new.txt   # Rename a file

fs> checkpoint                      # Manually save metadata snapshot
fs> crash power                     # Simulate power failure!
fs> recover                         # Recover from crash

fs> backup v1                       # Create full disk backup
fs> restore backup_..._v1.bin       # Restore from backup
fs> defrag                          # Defragment the disk
fs> metrics                         # Show performance dashboard
fs> exit                            # Save and quit
```

> **No need for leading `/`** — paths like `mkdir docs` and `mkdir /docs` both work.

---

## Disk Layout

The 10 MB virtual disk is organized as:

```
| Block 0    | Blocks 1-3   | Blocks 4-67       | Blocks 68-2559  |
| SUPERBLOCK |   BITMAP     |   INODE TABLE     |   DATA BLOCKS   |
|   (4 KB)   |  (12 KB)     |    (256 KB)       |   (~9.98 MB)    |
```

---

## How Crash Recovery Works

```
You type: crash power
  └─> Cache is wiped (unflushed data lost)
  └─> Disk marked as CRASHED

You type: recover
  └─> 1. Load fs_journal.bin → check for uncommitted operations
  └─> 2. Load fs_checkpoint.bin → restore full folder tree + all inodes
  └─> 3. Rebuild bitmap from inodes → write back to disk
  └─> System is mounted again ✓
```

Auto-checkpointing ensures the folder tree is saved after **every** `mkdir`, `create`, and `write` — so data survives even sudden power cuts.

---

## Performance Metrics

After running operations, `metrics` displays:

```
+============== Performance Metrics ==============+
  Disk Reads       : 14
  Disk Writes      : 164
  Cache Hit Rate   : 78.6%
  Total Blocks     : 2560
  Used Blocks      : 71
  Utilization      : 2.77%
  Recovery Time    : 48.00 ms
  Total Files      : 2
  Total Dirs       : 4
  Checkpoints Made : 3
+================================================+
```

---

## Automated Test Results

Running `./fs_test.exe` executes all 13 verification stages:

```
--- Stage 1:  Format                ✓ PASS
--- Stage 2:  Create Directories    ✓ PASS
--- Stage 3:  Create Files          ✓ PASS
--- Stage 4:  Write Files           ✓ PASS
--- Stage 5:  Read Back Files       ✓ PASS
--- Stage 6:  Directory Listing     ✓ PASS
--- Stage 7:  Search                ✓ PASS
--- Stage 8:  Checkpoint            ✓ PASS
--- Stage 9:  Simulate Power Loss   ✓ PASS
--- Stage 10: Recovery              ✓ PASS
--- Stage 11: Read After Recovery   ✓ PASS
--- Stage 12: Backup & Restore      ✓ PASS
--- Stage 13: Defragmentation       ✓ PASS

+===================================================+
|  ALL 13 STAGES PASSED - C File System Simulator   |
+===================================================+
```

---

## Learning Outcomes

- File system architecture (superblock, inode table, data blocks)
- Disk storage concepts (blocks, bitmap, fragmentation)
- Crash recovery techniques (WAL journaling, checkpointing)
- Data consistency mechanisms (commit/rollback)
- LRU cache implementation in C
- Systems programming with structs, pointers, and binary file I/O

---

## Contributors

- **Ashwell Cherian Giji**
- **Ashish Arman Toppo**
- **Thanda Sai Moukthika**

---

## License

This project is for **educational purposes** as part of an Operating Systems course.
