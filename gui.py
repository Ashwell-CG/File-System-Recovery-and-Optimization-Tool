"""
gui.py - Tkinter GUI for the File System Recovery & Optimization Tool
=====================================================================
Provides a modern desktop interface on top of the existing CLI backend.
This module does not reimplement file-system logic; it delegates every
operation to the existing FileSystem and VirtualDisk classes.
"""

from __future__ import annotations

import io
import os
import sys
import time
from contextlib import redirect_stdout
from typing import Callable

import tkinter as tk
from tkinter import messagebox, scrolledtext, simpledialog, ttk


# Ensure local module imports work when launching the file directly.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from disk import VirtualDisk
from file_system import DATA_START, FileSystem
from inode import AccessMethod, FileType


ACCESS_METHOD_OPTIONS = {
    "sequential": AccessMethod.SEQUENTIAL,
    "direct": AccessMethod.DIRECT,
    "indexed": AccessMethod.INDEXED,
}

CRASH_KIND_OPTIONS = {
    "power": "power_loss",
    "power_loss": "power_loss",
    "corrupt": "corrupt",
    "partial": "partial",
}


class FileSystemGUI:
    """Desktop GUI wrapper around the existing FileSystem API."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("File System Recovery & Optimization Tool")
        self.root.geometry("1420x860")
        self.root.minsize(1180, 720)

        self.disk = VirtualDisk()
        self.fs = FileSystem(self.disk)
        self.status_var = tk.StringVar(value="Ready. Format or mount the disk to begin.")
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

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _configure_style(self):
        self.root.configure(bg=self.colors["bg"])
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("App.TFrame", background=self.colors["bg"])
        style.configure("Panel.TFrame", background=self.colors["panel"])
        style.configure("AltPanel.TFrame", background=self.colors["panel_alt"])
        style.configure(
            "Title.TLabel",
            background=self.colors["bg"],
            foreground=self.colors["text"],
            font=("Helvetica", 20, "bold"),
        )
        style.configure(
            "Subtitle.TLabel",
            background=self.colors["bg"],
            foreground=self.colors["muted"],
            font=("Helvetica", 10),
        )
        style.configure(
            "PanelTitle.TLabel",
            background=self.colors["panel"],
            foreground=self.colors["text"],
            font=("Helvetica", 12, "bold"),
        )
        style.configure(
            "AltPanelTitle.TLabel",
            background=self.colors["panel_alt"],
            foreground=self.colors["text"],
            font=("Helvetica", 12, "bold"),
        )
        style.configure(
            "Info.TLabel",
            background=self.colors["panel"],
            foreground=self.colors["muted"],
            font=("Helvetica", 10),
        )
        style.configure(
            "Status.TLabel",
            background=self.colors["panel_alt"],
            foreground=self.colors["text"],
            font=("Helvetica", 10),
        )
        style.configure(
            "App.TButton",
            font=("Helvetica", 10, "bold"),
            padding=(10, 8),
            background=self.colors["accent"],
            foreground="white",
            borderwidth=0,
        )
        style.map(
            "App.TButton",
            background=[("active", self.colors["accent_dark"])],
            foreground=[("disabled", "#d8e0e8")],
        )

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

        ttk.Label(header, text="File System Recovery & Optimization Tool", style="Title.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            header,
            text="Tkinter GUI powered by the existing FileSystem backend",
            style="Subtitle.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))

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
        ttk.Label(status_bar, textvariable=self.status_var, style="Status.TLabel").grid(
            row=0, column=0, sticky="w"
        )

        self._build_controls()
        self._build_console()
        self._build_visualization()

    def _build_controls(self):
        ttk.Label(self.controls_panel, text="Controls", style="PanelTitle.TLabel").grid(
            row=0, column=0, sticky="w", pady=(0, 10)
        )

        buttons = [
            ("Format Disk", self.format_disk),
            ("Mount Disk", self.mount_disk),
            ("Create File", self.create_file),
            ("Create Directory", self.create_directory),
            ("Write File", self.write_file),
            ("Read File", self.read_file),
            ("Delete File", self.delete_file),
            ("Crash Simulation", self.simulate_crash),
            ("Recover System", self.recover_system),
            ("Defragment", self.defragment_disk),
            ("Show Metrics", self.show_metrics),
        ]

        for row, (label, handler) in enumerate(buttons, start=1):
            ttk.Button(
                self.controls_panel,
                text=label,
                command=handler,
                style="App.TButton",
            ).grid(row=row, column=0, sticky="ew", pady=5)

        helper = (
            "Tips:\n"
            "• Use absolute paths like /docs/report.txt\n"
            "• Create directories before files inside them\n"
            "• Crash recovery is available after simulated faults"
        )
        help_label = tk.Label(
            self.controls_panel,
            text=helper,
            justify="left",
            anchor="nw",
            bg=self.colors["panel"],
            fg=self.colors["muted"],
            font=("Helvetica", 10),
            wraplength=230,
        )
        help_label.grid(row=len(buttons) + 1, column=0, sticky="ew", pady=(16, 0))

    def _build_console(self):
        ttk.Label(self.console_panel, text="Output Console", style="PanelTitle.TLabel").grid(
            row=0, column=0, sticky="w", pady=(0, 10)
        )

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
        ttk.Label(self.visual_panel, text="Disk Visualization", style="AltPanelTitle.TLabel").grid(
            row=0, column=0, sticky="w"
        )

        summary = tk.Label(
            self.visual_panel,
            textvariable=self.summary_var,
            justify="left",
            anchor="w",
            bg=self.colors["panel_alt"],
            fg=self.colors["muted"],
            font=("Helvetica", 10),
            wraplength=420,
        )
        summary.grid(row=1, column=0, sticky="ew", pady=(8, 10))

        legend = ttk.Frame(self.visual_panel, style="AltPanel.TFrame")
        legend.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        legend.columnconfigure((0, 1, 2, 3), weight=1)

        legend_items = [
            ("Free", self.colors["free"]),
            ("Used", self.colors["used"]),
            ("Fragmented", self.colors["fragmented"]),
            ("Reserved", self.colors["reserved"]),
        ]
        for idx, (label, color) in enumerate(legend_items):
            item = ttk.Frame(legend, style="AltPanel.TFrame")
            item.grid(row=0, column=idx, sticky="w", padx=(0, 10))
            swatch = tk.Canvas(item, width=14, height=14, bg=self.colors["panel_alt"], highlightthickness=0)
            swatch.create_rectangle(1, 1, 13, 13, fill=color, outline=color)
            swatch.pack(side="left")
            tk.Label(
                item,
                text=label,
                bg=self.colors["panel_alt"],
                fg=self.colors["text"],
                font=("Helvetica", 9),
            ).pack(side="left", padx=(5, 0))

        viz_frame = ttk.Frame(self.visual_panel, style="AltPanel.TFrame")
        viz_frame.grid(row=3, column=0, sticky="nsew")
        viz_frame.columnconfigure(0, weight=1)
        viz_frame.rowconfigure(0, weight=1)

        self.block_canvas = tk.Canvas(
            viz_frame,
            bg="#fbfdff",
            highlightthickness=1,
            highlightbackground=self.colors["grid"],
        )
        self.block_canvas.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(viz_frame, orient="vertical", command=self.block_canvas.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.block_canvas.configure(yscrollcommand=scrollbar.set)

    def _bind_events(self):
        self.block_canvas.bind("<Configure>", lambda _event: self.refresh_visualization())

    # ------------------------------------------------------------------
    # Startup and helpers
    # ------------------------------------------------------------------

    def _initialize_on_startup(self):
        self.log("GUI initialized.")
        if self.disk.exists():
            try:
                self._run_operation(
                    "Auto-mount disk",
                    self.fs.mount,
                    success_message=None,
                    show_popup=False,
                    show_error_popup=False,
                )
            except Exception:
                pass
        self.refresh_visualization()

    def log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        self.console.configure(state="normal")
        self.console.insert("end", f"[{timestamp}] {message}\n")
        self.console.see("end")
        self.console.configure(state="disabled")

    def set_status(self, message: str):
        self.status_var.set(message)
        self.log(message)

    def _prompt_path(self, title: str, prompt: str) -> str | None:
        value = simpledialog.askstring(title, prompt, parent=self.root)
        if value is None:
            return None
        value = value.strip()
        if not value:
            messagebox.showerror("Invalid Path", "Path cannot be empty.")
            return None
        return value

    def _run_operation(
        self,
        action_name: str,
        callback: Callable[[], object],
        *,
        success_message: str | None = None,
        show_popup: bool = True,
        show_error_popup: bool = True,
    ):
        buffer = io.StringIO()
        started = time.perf_counter()
        try:
            with redirect_stdout(buffer):
                result = callback()
            output = buffer.getvalue().strip()
            if output:
                for line in output.splitlines():
                    self.log(line)
            elapsed = (time.perf_counter() - started) * 1000
            final_message = success_message or f"{action_name} completed in {elapsed:.1f} ms."
            self.status_var.set(final_message)
            self.log(final_message)
            self.refresh_visualization()
            if show_popup and success_message:
                messagebox.showinfo("Success", success_message)
            return result
        except Exception as exc:
            output = buffer.getvalue().strip()
            if output:
                for line in output.splitlines():
                    self.log(line)
            error_message = f"{action_name} failed: {exc}"
            self.status_var.set(error_message)
            self.log(error_message)
            self.refresh_visualization()
            if show_error_popup:
                messagebox.showerror("Operation Failed", str(exc))
            return None

    def _log_directory_snapshot(self):
        if not self.fs.inodes:
            return
        try:
            entries = self.fs.ls("/")
        except Exception:
            return
        if not entries:
            self.log("Root directory is empty.")
            return
        summary = ", ".join(
            f"{'[DIR]' if entry.is_dir else '[FILE]'} {entry.name}"
            for entry in sorted(entries, key=lambda item: (not item.is_dir, item.name.lower()))
        )
        self.log(f"Root entries: {summary}")

    def _fragmented_blocks(self) -> set[int]:
        fragmented: set[int] = set()
        for node in self.fs.inodes.values():
            if node.file_type != FileType.FILE or len(node.block_pointers) < 2:
                continue
            contiguous = all(
                node.block_pointers[idx] == node.block_pointers[idx - 1] + 1
                for idx in range(1, len(node.block_pointers))
            )
            if not contiguous:
                fragmented.update(node.block_pointers)
        return fragmented

    def _used_blocks(self) -> set[int]:
        used = set()
        if self.fs.allocator:
            for block_id in range(self.disk.total_blocks):
                if not self.fs.allocator.is_free(block_id):
                    used.add(block_id)
        return used

    # ------------------------------------------------------------------
    # Visualization and metrics
    # ------------------------------------------------------------------

    def refresh_visualization(self):
        self.block_canvas.delete("all")
        width = max(self.block_canvas.winfo_width(), 360)
        cols = 32
        gap = 4
        margin = 10
        usable_width = width - (margin * 2)
        cell = max(8, min(18, (usable_width - gap * (cols - 1)) // cols))
        if cell <= 0:
            return

        used_blocks = self._used_blocks()
        fragmented_blocks = self._fragmented_blocks()
        self._viz_used_blocks = used_blocks
        self._viz_fragmented_blocks = fragmented_blocks
        total_blocks = self.disk.total_blocks
        rows = (total_blocks + cols - 1) // cols

        for block_id in range(total_blocks):
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

            self.block_canvas.create_rectangle(
                x1,
                y1,
                x2,
                y2,
                fill=fill,
                outline="#ffffff",
                tags=("block", f"block-{block_id}"),
            )

        self.block_canvas.tag_bind("block", "<Motion>", self._on_block_hover)
        self.block_canvas.tag_bind("block", "<Leave>", lambda _event: self.status_var.set("Ready."))

        canvas_height = margin * 2 + rows * cell + max(rows - 1, 0) * gap
        self.block_canvas.configure(scrollregion=(0, 0, width, canvas_height))

        metrics = self.fs.get_metrics()
        space = metrics.get("space", {})
        summary_lines = [
            f"Total blocks: {space.get('total_blocks', self.disk.total_blocks)}",
            f"Used: {space.get('used_blocks', 0)} | Free: {space.get('free_blocks', self.disk.total_blocks)}",
            f"Utilization: {space.get('utilization_%', 0)}%",
            f"Fragmented blocks: {len(fragmented_blocks)}",
            f"Files: {metrics.get('total_files', 0)} | Directories: {metrics.get('total_dirs', 0)}",
        ]
        self.summary_var.set("\n".join(summary_lines))

    def _on_block_hover(self, event):
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
        metrics = self.fs.get_metrics()
        disk_metrics = metrics.get("disk", {})
        space_metrics = metrics.get("space", {})
        cp_metrics = metrics.get("checkpoint", {})

        message = (
            "Disk Metrics\n"
            f"• Total reads: {disk_metrics.get('total_reads', 0)}\n"
            f"• Total writes: {disk_metrics.get('total_writes', 0)}\n"
            f"• Avg read time: {disk_metrics.get('avg_read_time_ms', 0):.3f} ms\n"
            f"• Avg write time: {disk_metrics.get('avg_write_time_ms', 0):.3f} ms\n"
            f"• Cache hit rate: {disk_metrics.get('cache_hit_rate', 0):.1f}%\n\n"
            "Space Metrics\n"
            f"• Total blocks: {space_metrics.get('total_blocks', self.disk.total_blocks)}\n"
            f"• Used blocks: {space_metrics.get('used_blocks', 0)}\n"
            f"• Free blocks: {space_metrics.get('free_blocks', self.disk.total_blocks)}\n"
            f"• Utilization: {space_metrics.get('utilization_%', 0)}%\n\n"
            "System Metrics\n"
            f"• Recovery time: {metrics.get('recovery_time_ms', 0):.2f} ms\n"
            f"• Journal entries: {metrics.get('journal_entries', 0)}\n"
            f"• Total files: {metrics.get('total_files', 0)}\n"
            f"• Total directories: {metrics.get('total_dirs', 0)}\n"
            f"• Checkpoints made: {cp_metrics.get('checkpoint_count', 0)}\n"
            f"• Last checkpoint duration: {cp_metrics.get('last_checkpoint_duration_ms', 0):.2f} ms"
        )
        self.log("Metrics requested.")
        messagebox.showinfo("Performance Metrics", message)

    # ------------------------------------------------------------------
    # Operation handlers
    # ------------------------------------------------------------------

    def format_disk(self):
        if not messagebox.askyesno(
            "Format Disk",
            "Formatting will recreate the file system on the virtual disk. Continue?",
        ):
            return
        self._run_operation(
            "Format disk",
            self.fs.format,
            success_message="Disk formatted successfully.",
        )
        self._log_directory_snapshot()

    def mount_disk(self):
        self._run_operation(
            "Mount disk",
            self.fs.mount,
            success_message="Disk mounted successfully.",
        )
        self._log_directory_snapshot()

    def create_file(self):
        path = self._prompt_path("Create File", "Enter file path:")
        if not path:
            return

        method_input = simpledialog.askstring(
            "Access Method",
            "Enter access method: sequential, direct, or indexed",
            parent=self.root,
            initialvalue="sequential",
        )
        if method_input is None:
            return
        method = ACCESS_METHOD_OPTIONS.get(method_input.strip().lower())
        if method is None:
            messagebox.showerror("Invalid Access Method", "Use sequential, direct, or indexed.")
            return

        def action():
            inode_id = self.fs.create_file(path, method)
            self.log(f"Created file at {path} with inode {inode_id}.")

        self._run_operation(
            "Create file",
            action,
            success_message=f"File created: {path}",
        )
        self._log_directory_snapshot()

    def create_directory(self):
        path = self._prompt_path("Create Directory", "Enter directory path:")
        if not path:
            return

        self._run_operation(
            "Create directory",
            lambda: self.fs.mkdir(path),
            success_message=f"Directory created: {path}",
        )
        self._log_directory_snapshot()

    def write_file(self):
        path = self._prompt_path("Write File", "Enter file path:")
        if not path:
            return

        data = simpledialog.askstring("Write File", "Enter file content:", parent=self.root)
        if data is None:
            return

        self._run_operation(
            "Write file",
            lambda: self.fs.write_file(path, data.encode("utf-8")),
            success_message=f"Data written to {path}",
        )

    def read_file(self):
        path = self._prompt_path("Read File", "Enter file path:")
        if not path:
            return

        def action():
            content = self.fs.read_file(path).decode("utf-8", errors="replace")
            self.log(f"Read from {path}:")
            self.log(content if content else "(empty file)")
            return content

        content = self._run_operation(
            "Read file",
            action,
            success_message=None,
            show_popup=False,
        )
        if content is not None:
            messagebox.showinfo("File Content", content if content else "(empty file)")

    def delete_file(self):
        path = self._prompt_path("Delete File", "Enter file path:")
        if not path:
            return

        if not messagebox.askyesno("Delete File", f"Delete file at {path}?"):
            return

        self._run_operation(
            "Delete file",
            lambda: self.fs.delete_file(path),
            success_message=f"File deleted: {path}",
        )
        self._log_directory_snapshot()

    def simulate_crash(self):
        kind_input = simpledialog.askstring(
            "Crash Simulation",
            "Enter crash type: power, corrupt, or partial",
            parent=self.root,
            initialvalue="power",
        )
        if kind_input is None:
            return

        kind = CRASH_KIND_OPTIONS.get(kind_input.strip().lower())
        if kind is None:
            messagebox.showerror("Invalid Crash Type", "Use power, corrupt, or partial.")
            return

        kwargs = {}
        if kind in {"corrupt", "partial"}:
            block_text = simpledialog.askstring(
                "Block ID",
                f"Enter block ID to {kind_input.strip().lower()}:",
                parent=self.root,
                initialvalue=str(DATA_START),
            )
            if block_text is None:
                return
            try:
                kwargs["block_id"] = int(block_text.strip())
            except ValueError:
                messagebox.showerror("Invalid Block ID", "Block ID must be an integer.")
                return

        if kind == "partial":
            partial_data = simpledialog.askstring(
                "Partial Data",
                "Enter data payload for the partial write simulation:",
                parent=self.root,
                initialvalue="INTERRUPTED_WRITE",
            )
            if partial_data is None:
                return
            kwargs["data"] = partial_data.encode("utf-8")

        self._run_operation(
            "Crash simulation",
            lambda: self.fs.simulate_crash(kind, **kwargs),
            success_message=f"Crash simulation executed: {kind}",
        )

    def recover_system(self):
        self._run_operation(
            "Recover system",
            self.fs.recover,
            success_message="Recovery completed successfully.",
        )
        self._log_directory_snapshot()

    def defragment_disk(self):
        before = len(self._fragmented_blocks())

        def action():
            self.fs.defragment()
            after = len(self._fragmented_blocks())
            self.log(f"Fragmented blocks before: {before}")
            self.log(f"Fragmented blocks after: {after}")

        self._run_operation(
            "Defragment disk",
            action,
            success_message="Defragmentation completed.",
        )


def main():
    root = tk.Tk()
    app = FileSystemGUI(root)
    root.mainloop()
    return app


if __name__ == "__main__":
    main()
