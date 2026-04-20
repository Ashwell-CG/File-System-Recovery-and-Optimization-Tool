/*
 * fs.h - File System Controller
 * ================================
 * Ties together Disk, Bitmap, Inodes, Directory, Journal, and Checkpoint.
 */

#ifndef FS_H
#define FS_H

#include "disk.h"
#include "bitmap.h"
#include "inode.h"
#include "directory.h"
#include "recovery.h"

typedef struct {
    VirtualDisk  disk;
    Bitmap       bitmap;
    Inode        inodes[MAX_INODES];
    DirTree      dir_tree;
    Journal      journal;
    Checkpoint   checkpoint;
    int          next_inode_id;
    int          mounted;
    double       recovery_time_ms;
    int          checkpoints_made;
    double       last_cp_ms;
} FileSystem;

/* ── Lifecycle ─────────────────────────────────────────────── */
int  fs_init(FileSystem *fs, const char *disk_path);
void fs_close(FileSystem *fs);
int  fs_format(FileSystem *fs);
int  fs_mount(FileSystem *fs);

/* ── File operations ───────────────────────────────────────── */
int  fs_create_file(FileSystem *fs, const char *path, int access_method);
int  fs_write_file(FileSystem *fs, const char *path, const uint8_t *data, int len);
int  fs_read_file(FileSystem *fs, const char *path, uint8_t *buf, int buf_len);
int  fs_delete_file(FileSystem *fs, const char *path);

/* ── Directory operations ──────────────────────────────────── */
int  fs_mkdir(FileSystem *fs, const char *path);
int  fs_rmdir(FileSystem *fs, const char *path);
int  fs_ls(FileSystem *fs, const char *path, DirEntry *out, int max);
int  fs_rename(FileSystem *fs, const char *path, const char *new_name);
int  fs_search(FileSystem *fs, const char *name, char results[][MAX_PATH_LEN], int max);
int  fs_stat(FileSystem *fs, const char *path, Inode *out);
void fs_tree(FileSystem *fs);

/* ── Crash + Recovery ──────────────────────────────────────── */
void fs_simulate_crash(FileSystem *fs, const char *kind, int block_id);
int  fs_recover(FileSystem *fs);
int  fs_checkpoint(FileSystem *fs);
int  fs_backup(FileSystem *fs, const char *tag);
int  fs_restore(FileSystem *fs, const char *backup_name);

/* ── Optimization ──────────────────────────────────────────── */
int  fs_defragment(FileSystem *fs);

/* ── Metrics ───────────────────────────────────────────────── */
void fs_print_metrics(FileSystem *fs);

#endif /* FS_H */
