"""
c_gui.py - Tkinter GUI for the C File System Recovery & Optimization Tool
==========================================================================
This restores the Python/Tkinter GUI style while controlling the existing C
backend directly through ctypes. The C filesystem logic remains in fs.c,
disk.c, bitmap.c, inode.c, directory.c, and recovery.c.
"""

from __future__ import annotations

import ctypes
import os
import subprocess
import sys
import time

import tkinter as tk
from tkinter import messagebox, scrolledtext, simpledialog, ttk


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

BLOCK_SIZE = 4096
TOTAL_BLOCKS = 2560
DATA_START = 68
MAX_INODES = 1024
MAX_BLOCK_PTR = 48
MAX_DIRS = 256
MAX_ENTRIES_PER_DIR = 64
MAX_NAME_LEN = 128
MAX_PATH_LEN = 512
BITMAP_BYTES = (TOTAL_BLOCKS + 7) // 8
CACHE_SIZE = 64
MAX_JOURNAL_ENTRIES = 256
DISK_PATH = os.path.join(PROJECT_ROOT, "c_virtual_disk.bin")

ACCESS_SEQUENTIAL = 0
ACCESS_DIRECT = 1
ACCESS_INDEXED = 2
FTYPE_FILE = 0
FTYPE_DIR = 1


def shared_library_name() -> str:
    if sys.platform == "darwin":
        return "libfs.dylib"
    if os.name == "nt":
        return "fs.dll"
    return "libfs.so"


LIB_PATH = os.path.join(PROJECT_ROOT, shared_library_name())


class CacheEntry(ctypes.Structure):
    _fields_ = [
        ("block_id", ctypes.c_int),
        ("data", ctypes.c_uint8 * BLOCK_SIZE),
        ("dirty", ctypes.c_int),
        ("access_time", ctypes.c_longlong),
        ("valid", ctypes.c_int),
    ]


class DiskMetrics(ctypes.Structure):
    _fields_ = [
        ("reads", ctypes.c_long),
        ("writes", ctypes.c_long),
        ("cache_hits", ctypes.c_long),
        ("cache_misses", ctypes.c_long),
        ("total_read_ms", ctypes.c_double),
        ("total_write_ms", ctypes.c_double),
    ]


class VirtualDisk(ctypes.Structure):
    _fields_ = [
        ("path", ctypes.c_char * 256),
        ("fp", ctypes.c_void_p),
        ("total_blocks", ctypes.c_int),
        ("block_size", ctypes.c_int),
        ("crashed", ctypes.c_int),
        ("cache", CacheEntry * CACHE_SIZE),
        ("timer", ctypes.c_longlong),
        ("metrics", DiskMetrics),
    ]


class Bitmap(ctypes.Structure):
    _fields_ = [
        ("bits", ctypes.c_uint8 * BITMAP_BYTES),
        ("total", ctypes.c_int),
        ("used", ctypes.c_int),
    ]


class Inode(ctypes.Structure):
    _fields_ = [
        ("inode_id", ctypes.c_int32),
        ("file_type", ctypes.c_int32),
        ("access_method", ctypes.c_int32),
        ("size", ctypes.c_int32),
        ("created_at", ctypes.c_double),
        ("modified_at", ctypes.c_double),
        ("block_count", ctypes.c_int32),
        ("index_block", ctypes.c_int32),
        ("is_valid", ctypes.c_int32),
        ("block_pointers", ctypes.c_int32 * MAX_BLOCK_PTR),
        ("_pad", ctypes.c_uint8 * 20),
    ]


class DirEntry(ctypes.Structure):
    _fields_ = [
        ("name", ctypes.c_char * MAX_NAME_LEN),
        ("inode_id", ctypes.c_int),
        ("is_dir", ctypes.c_int),
        ("is_valid", ctypes.c_int),
    ]


class DirNode(ctypes.Structure):
    _fields_ = [
        ("dir_inode", ctypes.c_int),
        ("entries", DirEntry * MAX_ENTRIES_PER_DIR),
        ("entry_count", ctypes.c_int),
        ("is_valid", ctypes.c_int),
    ]


class DirTree(ctypes.Structure):
    _fields_ = [
        ("nodes", DirNode * MAX_DIRS),
        ("node_count", ctypes.c_int),
    ]


class JournalEntry(ctypes.Structure):
    _fields_ = [
        ("txn_id", ctypes.c_int32),
        ("op_type", ctypes.c_char * 32),
        ("path", ctypes.c_char * MAX_PATH_LEN),
        ("inode_id", ctypes.c_int32),
        ("parent_inode", ctypes.c_int32),
        ("blocks", ctypes.c_int32 * MAX_BLOCK_PTR),
        ("block_count", ctypes.c_int32),
        ("size", ctypes.c_int32),
        ("committed", ctypes.c_int32),
        ("is_valid", ctypes.c_int32),
    ]


class Journal(ctypes.Structure):
    _fields_ = [
        ("entries", JournalEntry * MAX_JOURNAL_ENTRIES),
        ("count", ctypes.c_int),
        ("next_txn_id", ctypes.c_int),
    ]


class Checkpoint(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_int),
        ("next_inode_id", ctypes.c_int),
        ("inode_count", ctypes.c_int),
        ("inodes", Inode * MAX_INODES),
        ("dir_tree", DirTree),
    ]


class FileSystem(ctypes.Structure):
    _fields_ = [
        ("disk", VirtualDisk),
        ("bitmap", Bitmap),
        ("inodes", Inode * MAX_INODES),
        ("dir_tree", DirTree),
        ("journal", Journal),
        ("checkpoint", Checkpoint),
        ("next_inode_id", ctypes.c_int),
        ("mounted", ctypes.c_int),
        ("recovery_time_ms", ctypes.c_double),
        ("checkpoints_made", ctypes.c_int),
        ("last_cp_ms", ctypes.c_double),
    ]


def build_shared_library():
    if os.path.exists(LIB_PATH):
        return

    compiler = os.environ.get("CC", "gcc")
    sources = ["disk.c", "bitmap.c", "inode.c", "directory.c", "recovery.c", "fs.c"]
    if os.name == "nt":
        cmd = [compiler, "-shared", "-O2", "-std=c11", *sources, "-o", LIB_PATH, "-lws2_32"]
    else:
        cmd = [compiler, "-shared", "-fPIC", "-O2", "-std=c11", *sources, "-o", LIB_PATH]

    try:
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        details = exc.stderr.strip() or exc.stdout.strip() or "Unknown build error."
        raise RuntimeError(f"Failed to build the C shared library.\n{details}") from exc
    except FileNotFoundError as exc:
        raise RuntimeError("C compiler not found. Install gcc/clang and try again.") from exc


class CFileSystemBridge:
    """Small ctypes bridge around the existing C FileSystem API."""

    def __init__(self):
        build_shared_library()
        self.lib = ctypes.CDLL(LIB_PATH)
        self.fs = FileSystem()
        self._configure_signatures()
        if self.lib.fs_init(ctypes.byref(self.fs), DISK_PATH.encode("utf-8")) < 0:
            raise RuntimeError("Unable to initialize the C file system backend.")

    def _configure_signatures(self):
        self.lib.fs_init.argtypes = [ctypes.POINTER(FileSystem), ctypes.c_char_p]
        self.lib.fs_init.restype = ctypes.c_int
        self.lib.fs_close.argtypes = [ctypes.POINTER(FileSystem)]
        self.lib.fs_close.restype = None
        self.lib.fs_format.argtypes = [ctypes.POINTER(FileSystem)]
        self.lib.fs_format.restype = ctypes.c_int
        self.lib.fs_mount.argtypes = [ctypes.POINTER(FileSystem)]
        self.lib.fs_mount.restype = ctypes.c_int
        self.lib.fs_create_file.argtypes = [ctypes.POINTER(FileSystem), ctypes.c_char_p, ctypes.c_int]
        self.lib.fs_create_file.restype = ctypes.c_int
        self.lib.fs_write_file.argtypes = [ctypes.POINTER(FileSystem), ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_int]
        self.lib.fs_write_file.restype = ctypes.c_int
        self.lib.fs_read_file.argtypes = [ctypes.POINTER(FileSystem), ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_int]
        self.lib.fs_read_file.restype = ctypes.c_int
        self.lib.fs_delete_file.argtypes = [ctypes.POINTER(FileSystem), ctypes.c_char_p]
        self.lib.fs_delete_file.restype = ctypes.c_int
        self.lib.fs_mkdir.argtypes = [ctypes.POINTER(FileSystem), ctypes.c_char_p]
        self.lib.fs_mkdir.restype = ctypes.c_int
        self.lib.fs_rmdir.argtypes = [ctypes.POINTER(FileSystem), ctypes.c_char_p]
        self.lib.fs_rmdir.restype = ctypes.c_int
        self.lib.fs_simulate_crash.argtypes = [ctypes.POINTER(FileSystem), ctypes.c_char_p, ctypes.c_int]
        self.lib.fs_simulate_crash.restype = None
        self.lib.fs_recover.argtypes = [ctypes.POINTER(FileSystem)]
        self.lib.fs_recover.restype = ctypes.c_int
        self.lib.fs_checkpoint.argtypes = [ctypes.POINTER(FileSystem)]
        self.lib.fs_checkpoint.restype = ctypes.c_int
        self.lib.fs_defragment.argtypes = [ctypes.POINTER(FileSystem)]
        self.lib.fs_defragment.restype = ctypes.c_int

    def close(self):
        self.lib.fs_close(ctypes.byref(self.fs))

    def _as_path(self, path: str) -> bytes:
        path = path.strip()
        if not path.startswith("/"):
            path = f"/{path}"
        return path.encode("utf-8")

    def format(self) -> int:
        return self.lib.fs_format(ctypes.byref(self.fs))

    def mount(self) -> int:
        return self.lib.fs_mount(ctypes.byref(self.fs))

    def create_file(self, path: str, method: int) -> int:
        return self.lib.fs_create_file(ctypes.byref(self.fs), self._as_path(path), method)

    def write_file(self, path: str, data: str) -> int:
        encoded = data.encode("utf-8")
        buf = (ctypes.c_uint8 * len(encoded))(*encoded) if encoded else (ctypes.c_uint8 * 1)()
        return self.lib.fs_write_file(ctypes.byref(self.fs), self._as_path(path), buf, len(encoded))

    def read_file(self, path: str) -> str:
        buf = (ctypes.c_uint8 * (BLOCK_SIZE * MAX_BLOCK_PTR))()
        count = self.lib.fs_read_file(ctypes.byref(self.fs), self._as_path(path), buf, len(buf) - 1)
        if count < 0:
            raise RuntimeError("Read failed. Check that the file exists and the disk is mounted.")
        return bytes(buf[:count]).decode("utf-8", errors="replace")

    def delete_file(self, path: str) -> int:
        return self.lib.fs_delete_file(ctypes.byref(self.fs), self._as_path(path))

    def mkdir(self, path: str) -> int:
        return self.lib.fs_mkdir(ctypes.byref(self.fs), self._as_path(path))

    def rmdir(self, path: str) -> int:
        return self.lib.fs_rmdir(ctypes.byref(self.fs), self._as_path(path))

    def simulate_crash(self, kind: str, block_id: int):
        self.lib.fs_simulate_crash(ctypes.byref(self.fs), kind.encode("utf-8"), block_id)

    def recover(self) -> int:
        return self.lib.fs_recover(ctypes.byref(self.fs))

    def defragment(self) -> int:
        return self.lib.fs_defragment(ctypes.byref(self.fs))


class FileSystemGUI:
    """Tkinter GUI matching the earlier Python version, backed by C."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("C File System Recovery & Optimization Tool")
        self.root.geometry("1420x860")
        self.root.minsize(1180, 720)

        self.backend = CFileSystemBridge()
        self.status_var = tk.StringVar(value="Ready. Format or mount the C disk to begin.")
        self.summary_var = tk.StringVar(value="Disk summary unavailable.")
        self._viz_used_blocks: set[int] = set()
        self._viz_fragmented_blocks: set[int] = set()

        self.colors = {
            "bg": "#f3f6fb",
            "panel": "#ffffff",
            "panel_alt": "#eef3f8",
            "accent": "#1f6feb",
            "accent_dark": "#164ea6",
            "text": "#17324d",
            "muted": "#5b728b",
            "free": "#3fbf7f",
            "used": "#d84d57",
            "fragmented": "#f2b94b",
            "reserved": "#95a5b5",
            "grid": "#d6dee8",
        }

        self._configure_style()
        self._build_layout()
        self._bind_events()
        self._initialize_on_startup()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _configure_style(self):
        self.root.configure(bg=self.colors["bg"])
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("App.TFrame", background=self.colors["bg"])
        style.configure("Panel.TFrame", background=self.colors["panel"])
        style.configure("AltPanel.TFrame", background=self.colors["panel_alt"])
        style.configure("Title.TLabel", background=self.colors["bg"], foreground=self.colors["text"], font=("Helvetica", 20, "bold"))
        style.configure("Subtitle.TLabel", background=self.colors["bg"], foreground=self.colors["muted"], font=("Helvetica", 10))
        style.configure("PanelTitle.TLabel", background=self.colors["panel"], foreground=self.colors["text"], font=("Helvetica", 12, "bold"))
        style.configure("AltPanelTitle.TLabel", background=self.colors["panel_alt"], foreground=self.colors["text"], font=("Helvetica", 12, "bold"))
        style.configure("Status.TLabel", background=self.colors["panel_alt"], foreground=self.colors["text"], font=("Helvetica", 10))
        style.configure("App.TButton", font=("Helvetica", 10, "bold"), padding=(10, 8), background=self.colors["accent"], foreground="white", borderwidth=0)
        style.map("App.TButton", background=[("active", self.colors["accent_dark"])])

    def _build_layout(self):
        container = ttk.Frame(self.root, style="App.TFrame", padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=0)
        container.columnconfigure(1, weight=1)
        container.columnconfigure(2, weight=1)
        container.rowconfigure(1, weight=1)

        header = ttk.Frame(container, style="App.TFrame")
        header.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 12))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="File System Recovery & Optimization Tool", style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(header, text="Tkinter GUI controlling the C backend via ctypes", style="Subtitle.TLabel").grid(row=1, column=0, sticky="w", pady=(2, 0))

        self.controls_panel = ttk.Frame(container, style="Panel.TFrame", padding=14)
        self.controls_panel.grid(row=1, column=0, sticky="nsew", padx=(0, 12))
        self.controls_panel.columnconfigure(0, weight=1)

        self.console_panel = ttk.Frame(container, style="Panel.TFrame", padding=14)
        self.console_panel.grid(row=1, column=1, sticky="nsew", padx=(0, 12))
        self.console_panel.columnconfigure(0, weight=1)
        self.console_panel.rowconfigure(1, weight=1)

        self.visual_panel = ttk.Frame(container, style="AltPanel.TFrame", padding=14)
        self.visual_panel.grid(row=1, column=2, sticky="nsew")
        self.visual_panel.columnconfigure(0, weight=1)
        self.visual_panel.rowconfigure(3, weight=1)

        status_bar = ttk.Frame(container, style="AltPanel.TFrame", padding=(12, 10))
        status_bar.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(12, 0))
        status_bar.columnconfigure(0, weight=1)
        ttk.Label(status_bar, textvariable=self.status_var, style="Status.TLabel").grid(row=0, column=0, sticky="w")

        self._build_controls()
        self._build_console()
        self._build_visualization()

    def _build_controls(self):
        ttk.Label(self.controls_panel, text="Controls", style="PanelTitle.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 10))
        buttons = [
            ("Format Disk", self.format_disk),
            ("Mount Disk", self.mount_disk),
            ("Create File", self.create_file),
            ("Create Directory", self.create_directory),
            ("Delete Directory", self.delete_directory),
            ("Write File", self.write_file),
            ("Read File", self.read_file),
            ("Delete File", self.delete_file),
            ("Crash Simulation", self.simulate_crash),
            ("Recover System", self.recover_system),
            ("Defragment", self.defragment_disk),
            ("Show Metrics", self.show_metrics),
        ]
        for row, (label, handler) in enumerate(buttons, start=1):
            ttk.Button(self.controls_panel, text=label, command=handler, style="App.TButton").grid(row=row, column=0, sticky="ew", pady=5)

        helper = "Tips:\n• Use paths like /docs/report.txt\n• Format Disk first on a new disk\n• Create parent directories before nested files"
        tk.Label(self.controls_panel, text=helper, justify="left", anchor="nw", bg=self.colors["panel"], fg=self.colors["muted"], font=("Helvetica", 10), wraplength=230).grid(row=len(buttons) + 1, column=0, sticky="ew", pady=(16, 0))

    def _build_console(self):
        ttk.Label(self.console_panel, text="Output Console", style="PanelTitle.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 10))
        self.console = scrolledtext.ScrolledText(
            self.console_panel,
            wrap="word",
            font=("Courier New", 10),
            bg="#0f1720",
            fg="#d7e3f0",
            insertbackground="#d7e3f0",
            relief="flat",
            padx=10,
            pady=10,
        )
        self.console.grid(row=1, column=0, sticky="nsew")
        self.console.configure(state="disabled")

    def _build_visualization(self):
        ttk.Label(self.visual_panel, text="Disk Visualization", style="AltPanelTitle.TLabel").grid(row=0, column=0, sticky="w")
        tk.Label(self.visual_panel, textvariable=self.summary_var, justify="left", anchor="w", bg=self.colors["panel_alt"], fg=self.colors["muted"], font=("Helvetica", 10), wraplength=420).grid(row=1, column=0, sticky="ew", pady=(8, 10))

        legend = ttk.Frame(self.visual_panel, style="AltPanel.TFrame")
        legend.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        legend_items = [("Free", self.colors["free"]), ("Used", self.colors["used"]), ("Fragmented", self.colors["fragmented"]), ("Reserved", self.colors["reserved"])]
        for idx, (label, color) in enumerate(legend_items):
            item = ttk.Frame(legend, style="AltPanel.TFrame")
            item.grid(row=0, column=idx, sticky="w", padx=(0, 10))
            swatch = tk.Canvas(item, width=14, height=14, bg=self.colors["panel_alt"], highlightthickness=0)
            swatch.create_rectangle(1, 1, 13, 13, fill=color, outline=color)
            swatch.pack(side="left")
            tk.Label(item, text=label, bg=self.colors["panel_alt"], fg=self.colors["text"], font=("Helvetica", 9)).pack(side="left", padx=(5, 0))

        viz_frame = ttk.Frame(self.visual_panel, style="AltPanel.TFrame")
        viz_frame.grid(row=3, column=0, sticky="nsew")
        viz_frame.columnconfigure(0, weight=1)
        viz_frame.rowconfigure(0, weight=1)
        self.block_canvas = tk.Canvas(viz_frame, bg="#fbfdff", highlightthickness=1, highlightbackground=self.colors["grid"])
        self.block_canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(viz_frame, orient="vertical", command=self.block_canvas.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.block_canvas.configure(yscrollcommand=scrollbar.set)

    def _bind_events(self):
        self.block_canvas.bind("<Configure>", lambda _event: self.refresh_visualization())

    def _initialize_on_startup(self):
        self.log("GUI initialized.")
        if os.path.exists(DISK_PATH) and self.backend.mount() == 0:
            self.log("Mounted existing C virtual disk.")
            self.status_var.set("Mounted existing disk.")
        else:
            self.log("No mounted C disk yet. Use Format Disk to begin.")
        self.refresh_visualization()

    def log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        self.console.configure(state="normal")
        self.console.insert("end", f"[{timestamp}] {message}\n")
        self.console.see("end")
        self.console.configure(state="disabled")

    def _prompt_path(self, title: str, prompt: str, initial: str = "/notes.txt") -> str | None:
        value = simpledialog.askstring(title, prompt, parent=self.root, initialvalue=initial)
        if value is None:
            return None
        value = value.strip()
        if not value:
            messagebox.showerror("Invalid Path", "Path cannot be empty.")
            return None
        return value

    def _require_success(self, result: int, action_name: str):
        if result < 0:
            raise RuntimeError(f"{action_name} failed. Format/mount first and check the path.")

    def _run_operation(self, action_name: str, callback, success_message: str | None = None):
        started = time.perf_counter()
        try:
            result = callback()
            elapsed = (time.perf_counter() - started) * 1000
            message = success_message or f"{action_name} completed in {elapsed:.1f} ms."
            self.status_var.set(message)
            self.log(message)
            self.refresh_visualization()
            if success_message:
                messagebox.showinfo("Success", success_message)
            return result
        except Exception as exc:
            message = f"{action_name} failed: {exc}"
            self.status_var.set(message)
            self.log(message)
            self.refresh_visualization()
            messagebox.showerror("Operation Failed", str(exc))
            return None

    def _used_blocks(self) -> set[int]:
        used = set()
        total = self.backend.fs.bitmap.total or TOTAL_BLOCKS
        bits = self.backend.fs.bitmap.bits
        for block_id in range(total):
            if bits[block_id // 8] & (1 << (block_id % 8)):
                used.add(block_id)
        return used

    def _fragmented_blocks(self) -> set[int]:
        fragmented = set()
        for inode in self.backend.fs.inodes:
            if not inode.is_valid or inode.file_type != FTYPE_FILE or inode.block_count < 2:
                continue
            blocks = [inode.block_pointers[i] for i in range(inode.block_count)]
            contiguous = all(blocks[idx] == blocks[idx - 1] + 1 for idx in range(1, len(blocks)))
            if not contiguous:
                fragmented.update(blocks)
                if inode.index_block >= 0:
                    fragmented.add(inode.index_block)
        return fragmented

    def metrics_dict(self) -> dict:
        fs = self.backend.fs
        reads = fs.disk.metrics.reads
        writes = fs.disk.metrics.writes
        hit_total = fs.disk.metrics.cache_hits + fs.disk.metrics.cache_misses
        total_files = sum(1 for inode in fs.inodes if inode.is_valid and inode.file_type == FTYPE_FILE)
        total_dirs = sum(1 for inode in fs.inodes if inode.is_valid and inode.file_type == FTYPE_DIR)
        total_blocks = fs.bitmap.total or TOTAL_BLOCKS
        used_blocks = fs.bitmap.used
        free_blocks = max(total_blocks - used_blocks, 0)
        utilization = (used_blocks / total_blocks * 100) if total_blocks else 0.0
        return {
            "reads": reads,
            "writes": writes,
            "avg_read_ms": (fs.disk.metrics.total_read_ms / reads) if reads else 0.0,
            "avg_write_ms": (fs.disk.metrics.total_write_ms / writes) if writes else 0.0,
            "cache_hit_rate": (fs.disk.metrics.cache_hits / hit_total * 100) if hit_total else 0.0,
            "used_blocks": used_blocks,
            "free_blocks": free_blocks,
            "utilization": utilization,
            "recovery_time_ms": fs.recovery_time_ms,
            "journal_entries": fs.journal.count,
            "total_files": total_files,
            "total_dirs": total_dirs,
            "checkpoints_made": fs.checkpoints_made,
            "last_cp_ms": fs.last_cp_ms,
        }

    def refresh_visualization(self):
        self.block_canvas.delete("all")
        width = max(self.block_canvas.winfo_width(), 360)
        cols = 32
        gap = 4
        margin = 10
        cell = max(8, min(18, (width - margin * 2 - gap * (cols - 1)) // cols))
        if cell <= 0:
            return

        used_blocks = self._used_blocks()
        fragmented_blocks = self._fragmented_blocks()
        self._viz_used_blocks = used_blocks
        self._viz_fragmented_blocks = fragmented_blocks

        rows = (TOTAL_BLOCKS + cols - 1) // cols
        for block_id in range(TOTAL_BLOCKS):
            row = block_id // cols
            col = block_id % cols
            x1 = margin + col * (cell + gap)
            y1 = margin + row * (cell + gap)
            x2 = x1 + cell
            y2 = y1 + cell
            if block_id < DATA_START:
                fill = self.colors["reserved"]
            elif block_id in fragmented_blocks:
                fill = self.colors["fragmented"]
            elif block_id in used_blocks:
                fill = self.colors["used"]
            else:
                fill = self.colors["free"]
            self.block_canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline="#ffffff", tags=("block", f"block-{block_id}"))

        self.block_canvas.tag_bind("block", "<Motion>", self._on_block_hover)
        self.block_canvas.tag_bind("block", "<Leave>", lambda _event: self.status_var.set("Ready."))
        canvas_height = margin * 2 + rows * cell + max(rows - 1, 0) * gap
        self.block_canvas.configure(scrollregion=(0, 0, width, canvas_height))

        metrics = self.metrics_dict()
        self.summary_var.set(
            f"Mounted: {'Yes' if self.backend.fs.mounted else 'No'} | Crashed: {'Yes' if self.backend.fs.disk.crashed else 'No'}\n"
            f"Used: {metrics['used_blocks']} | Free: {metrics['free_blocks']}\n"
            f"Utilization: {metrics['utilization']:.2f}%\n"
            f"Fragmented blocks: {len(fragmented_blocks)}\n"
            f"Files: {metrics['total_files']} | Directories: {metrics['total_dirs']}"
        )

    def _on_block_hover(self, _event):
        item = self.block_canvas.find_withtag("current")
        if not item:
            return
        tags = self.block_canvas.gettags(item[0])
        block_tag = next((tag for tag in tags if tag.startswith("block-")), None)
        if not block_tag:
            return
        block_id = int(block_tag.split("-", 1)[1])
        state = "reserved" if block_id < DATA_START else "free"
        if block_id in self._viz_fragmented_blocks:
            state = "fragmented"
        elif block_id in self._viz_used_blocks and block_id >= DATA_START:
            state = "used"
        self.status_var.set(f"Block {block_id}: {state}")

    def show_metrics(self):
        metrics = self.metrics_dict()
        self.log("Metrics requested.")
        messagebox.showinfo(
            "Performance Metrics",
            "Disk Metrics\n"
            f"• Total reads: {metrics['reads']}\n"
            f"• Total writes: {metrics['writes']}\n"
            f"• Avg read time: {metrics['avg_read_ms']:.3f} ms\n"
            f"• Avg write time: {metrics['avg_write_ms']:.3f} ms\n"
            f"• Cache hit rate: {metrics['cache_hit_rate']:.1f}%\n\n"
            "Space Metrics\n"
            f"• Total blocks: {self.backend.fs.bitmap.total or TOTAL_BLOCKS}\n"
            f"• Used blocks: {metrics['used_blocks']}\n"
            f"• Free blocks: {metrics['free_blocks']}\n"
            f"• Utilization: {metrics['utilization']:.2f}%\n\n"
            "System Metrics\n"
            f"• Recovery time: {metrics['recovery_time_ms']:.2f} ms\n"
            f"• Journal entries: {metrics['journal_entries']}\n"
            f"• Total files: {metrics['total_files']}\n"
            f"• Total directories: {metrics['total_dirs']}\n"
            f"• Checkpoints made: {metrics['checkpoints_made']}\n"
            f"• Last checkpoint duration: {metrics['last_cp_ms']:.2f} ms",
        )

    def format_disk(self):
        if not messagebox.askyesno("Format Disk", "Formatting will recreate the C file system. Continue?"):
            return
        self._run_operation("Format disk", lambda: self._require_success(self.backend.format(), "Format disk"), "Disk formatted successfully.")

    def mount_disk(self):
        self._run_operation("Mount disk", lambda: self._require_success(self.backend.mount(), "Mount disk"), "Disk mounted successfully.")

    def create_file(self):
        path = self._prompt_path("Create File", "Enter file path:")
        if not path:
            return
        method_input = simpledialog.askstring("Access Method", "Enter access method: sequential, direct, or indexed", parent=self.root, initialvalue="sequential")
        if method_input is None:
            return
        method_map = {"sequential": ACCESS_SEQUENTIAL, "direct": ACCESS_DIRECT, "indexed": ACCESS_INDEXED}
        method = method_map.get(method_input.strip().lower())
        if method is None:
            messagebox.showerror("Invalid Access Method", "Use sequential, direct, or indexed.")
            return
        self._run_operation("Create file", lambda: self._require_success(self.backend.create_file(path, method), "Create file"), f"File created: {path}")

    def create_directory(self):
        path = self._prompt_path("Create Directory", "Enter directory path:", "/docs")
        if not path:
            return
        self._run_operation("Create directory", lambda: self._require_success(self.backend.mkdir(path), "Create directory"), f"Directory created: {path}")

    def delete_directory(self):
        path = self._prompt_path("Delete Directory", "Enter directory path:", "/docs")
        if not path:
            return
        self._run_operation("Delete directory", lambda: self._require_success(self.backend.rmdir(path), "Delete directory"), f"Directory deleted: {path}")

    def write_file(self):
        path = self._prompt_path("Write File", "Enter file path:")
        if not path:
            return
        data = simpledialog.askstring("Write File", "Enter file content:", parent=self.root)
        if data is None:
            return
        self._run_operation("Write file", lambda: self._require_success(self.backend.write_file(path, data), "Write file"), f"Data written to {path}")

    def read_file(self):
        path = self._prompt_path("Read File", "Enter file path:")
        if not path:
            return
        content = self._run_operation("Read file", lambda: self.backend.read_file(path))
        if content is not None:
            self.log(f"Read from {path}:")
            self.log(content if content else "(empty file)")
            messagebox.showinfo("File Content", content if content else "(empty file)")

    def delete_file(self):
        path = self._prompt_path("Delete File", "Enter file path:")
        if not path:
            return
        if not messagebox.askyesno("Delete File", f"Delete file at {path}?"):
            return
        self._run_operation("Delete file", lambda: self._require_success(self.backend.delete_file(path), "Delete file"), f"File deleted: {path}")

    def simulate_crash(self):
        kind_input = simpledialog.askstring("Crash Simulation", "Enter crash type: power or corrupt", parent=self.root, initialvalue="power")
        if kind_input is None:
            return
        kind = kind_input.strip().lower()
        if kind not in {"power", "corrupt"}:
            messagebox.showerror("Invalid Crash Type", "Use power or corrupt.")
            return
        block_id = DATA_START
        if kind == "corrupt":
            block_text = simpledialog.askstring("Block ID", "Enter block ID to corrupt:", parent=self.root, initialvalue=str(DATA_START))
            if block_text is None:
                return
            try:
                block_id = int(block_text.strip())
            except ValueError:
                messagebox.showerror("Invalid Block ID", "Block ID must be an integer.")
                return
        self._run_operation("Crash simulation", lambda: self.backend.simulate_crash(kind, block_id), f"Crash simulation executed: {kind}")

    def recover_system(self):
        self._run_operation("Recover system", lambda: self._require_success(self.backend.recover(), "Recover system"), "Recovery completed successfully.")

    def defragment_disk(self):
        before = len(self._fragmented_blocks())

        def action():
            self._require_success(self.backend.defragment(), "Defragment disk")
            after = len(self._fragmented_blocks())
            self.log(f"Fragmented blocks before: {before}")
            self.log(f"Fragmented blocks after: {after}")

        self._run_operation("Defragment disk", action, "Defragmentation completed.")

    def on_close(self):
        try:
            if self.backend.fs.mounted:
                self.backend.lib.fs_checkpoint(ctypes.byref(self.backend.fs))
            self.backend.close()
        finally:
            self.root.destroy()


def main():
    root = tk.Tk()
    FileSystemGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
