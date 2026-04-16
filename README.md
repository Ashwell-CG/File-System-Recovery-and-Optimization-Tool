# File System Recovery & Optimization Simulator

A **Python-based operating system project** that simulates a real-world file system with advanced features like **crash recovery, journaling, defragmentation, and disk optimization**.

---

## Overview

This project demonstrates how modern file systems work internally by implementing:

* Virtual disk simulation
* File and directory management
* Free-space allocation strategies
* Crash recovery using journaling
* Performance monitoring and optimization

It is designed for **learning OS concepts** and simulating real-world disk behaviors.

---

## Key Features

### Virtual Disk Simulation

* Simulates a physical disk using a binary file
* Fixed-size block architecture (4KB blocks)
* Includes caching and performance tracking

### File & Directory Management

* Create, delete, rename, and search files
* Hierarchical directory tree structure
* Supports metadata and inode-based storage

### Access Methods

* Sequential Access
* Direct Access
* Indexed Access

### Free Space Management

* Bitmap allocation (efficient tracking)
* Linked-list allocation (alternative strategy)

### Crash Recovery System

* Write-Ahead Logging (Journal)
* Checkpointing
* Backup & Restore system

### Performance Optimization

* Disk caching (LRU)
* Defragmentation support
* Metrics tracking (reads, writes, latency)

---

## Interfaces

### 🧾 CLI (Command Line Interface)

Supports commands like:

```
format, mount, mkdir, create, write, read, delete
crash, recover, backup, restore, defrag
```

### GUI Dashboard

* Built with Tkinter
* Visual disk block representation
* Real-time system metrics
* File explorer interface

---

## Project Structure

```
📦 File-System-Simulator
 ┣ 📜 cli.py
 ┣ 📜 gui.py
 ┣ 📜 file_system.py
 ┣ 📜 disk.py
 ┣ 📜 inode.py
 ┣ 📜 directory.py
 ┣ 📜 free_space_manager.py
 ┣ 📜 recovery.py
 ┣ 📜 test_simulation.py
 ┗ 📜 export_pdf.py
```

---

## Installation

```bash
git clone https://github.com/your-username/File-System-Recovery-and-Optimization-Tool.git
cd File-System-Recovery-and-Optimization-Tool
```

---

## Usage

### Run CLI

```bash
python cli.py
```

### Run GUI

```bash
python gui.py
```

### Run Full Simulation Test

```bash
python test_simulation.py
```

---

## Simulation Capabilities

* Disk crash simulation (power loss, corruption)
* Recovery using journal replay
* Backup & restore functionality
* Defragmentation impact analysis

---

## Metrics Tracked

* Disk reads/writes
* Average latency
* Cache hit rate
* Space utilization
* Recovery time

---

## Learning Outcomes

* File system architecture
* Disk storage concepts
* Crash recovery techniques
* Data consistency mechanisms
* OS-level resource management

---

## Contributors

* Ashwell Cherian Giji
* 

---

## License

This project is for **educational purposes**.
