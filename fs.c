/*
 * fs.c - File System Controller implementation
 */

#include "fs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>

/* ── Helpers ──────────────────────────────────────────────── */

static int check_mounted(FileSystem *fs) {
    if (!fs->mounted) {
        fprintf(stderr, "[FS] File system not mounted! Run format or recover first.\n");
        return -1;
    }
    return 0;
}

static int alloc_inode_id(FileSystem *fs) {
    return fs->next_inode_id++;
}

/* Fill RESERVED_BLOCKS array (blocks 0..DATA_START-1) */
static void make_reserved(int *arr, int *count) {
    *count = DATA_START; /* 0..67 */
    for (int i = 0; i < DATA_START; i++) arr[i] = i;
}

/* ── Disk persistence helpers ─────────────────────────────── */

static void persist_bitmap(FileSystem *fs) {
    uint8_t raw[BITMAP_BYTES];
    bitmap_serialize(&fs->bitmap, raw);
    uint8_t block[BLOCK_SIZE];
    for (int i = 0; i < BITMAP_BLOCKS; i++) {
        int s = i * BLOCK_SIZE;
        int e = s + BLOCK_SIZE;
        memset(block, 0, BLOCK_SIZE);
        for (int j = s; j < e && j < (int)BITMAP_BYTES; j++)
            block[j - s] = raw[j];
        disk_write_block(&fs->disk, BITMAP_START + i, block);
    }
}

static void load_bitmap(FileSystem *fs) {
    uint8_t raw[BITMAP_BLOCKS * BLOCK_SIZE];
    for (int i = 0; i < BITMAP_BLOCKS; i++)
        disk_read_block(&fs->disk, BITMAP_START + i, raw + i * BLOCK_SIZE);
    bitmap_deserialize(&fs->bitmap, raw, TOTAL_BLOCKS);
}

static void persist_inode(FileSystem *fs, Inode *node) {
    int slot     = node->inode_id;
    int blk      = INODE_TABLE_START + (slot / INODES_PER_BLOCK);
    int offset   = (slot % INODES_PER_BLOCK) * INODE_SERIAL_SIZE;
    uint8_t block[BLOCK_SIZE];
    disk_read_block(&fs->disk, blk, block);
    inode_serialize(node, block + offset);
    disk_write_block(&fs->disk, blk, block);
}

static void persist_inode_table(FileSystem *fs) {
    for (int i = 0; i < INODE_TABLE_BLOCKS; i++) {
        uint8_t block[BLOCK_SIZE];
        memset(block, 0, BLOCK_SIZE);
        for (int j = 0; j < INODES_PER_BLOCK; j++) {
            int iid = i * INODES_PER_BLOCK + j;
            if (iid < MAX_INODES && fs->inodes[iid].is_valid)
                inode_serialize(&fs->inodes[iid], block + j * INODE_SERIAL_SIZE);
        }
        disk_write_block(&fs->disk, INODE_TABLE_START + i, block);
    }
}

static void load_inodes(FileSystem *fs) {
    memset(fs->inodes, 0, sizeof(fs->inodes));
    fs->next_inode_id = 1;
    for (int i = 0; i < INODE_TABLE_BLOCKS; i++) {
        uint8_t block[BLOCK_SIZE];
        disk_read_block(&fs->disk, INODE_TABLE_START + i, block);
        for (int j = 0; j < INODES_PER_BLOCK; j++) {
            int iid = i * INODES_PER_BLOCK + j;
            if (iid < MAX_INODES) {
                Inode tmp;
                if (inode_deserialize(&tmp, block + j * INODE_SERIAL_SIZE)) {
                    fs->inodes[iid] = tmp;
                    if (iid >= fs->next_inode_id)
                        fs->next_inode_id = iid + 1;
                }
            }
        }
    }
}

static void write_superblock(FileSystem *fs) {
    uint8_t block[BLOCK_SIZE];
    memset(block, 0, BLOCK_SIZE);
    memcpy(block, MAGIC, 4);
    int v = FS_VERSION;
    memcpy(block + 4, &v, 4);
    memcpy(block + 8,  &fs->disk.total_blocks, 4);
    memcpy(block + 12, &fs->disk.block_size,   4);
    disk_write_block(&fs->disk, SUPERBLOCK_ID, block);
}

static int check_superblock(FileSystem *fs) {
    uint8_t block[BLOCK_SIZE];
    disk_read_block(&fs->disk, SUPERBLOCK_ID, block);
    return memcmp(block, MAGIC, 4) == 0;
}

/* ── Checkpoint helpers ─────────────────────────────────────── */

static void build_checkpoint(FileSystem *fs) {
    fs->checkpoint.magic         = 0xC5EC;
    fs->checkpoint.next_inode_id = fs->next_inode_id;
    fs->checkpoint.inode_count   = 0;
    memcpy(fs->checkpoint.inodes, fs->inodes, sizeof(fs->inodes));
    for (int i = 0; i < MAX_INODES; i++)
        if (fs->inodes[i].is_valid) fs->checkpoint.inode_count++;
    fs->checkpoint.dir_tree = fs->dir_tree;
}

static void rebuild_bitmap_from_inodes(FileSystem *fs) {
    int reserved[DATA_START];
    int reserved_count;
    make_reserved(reserved, &reserved_count);
    bitmap_init(&fs->bitmap, TOTAL_BLOCKS, reserved, reserved_count);
    for (int i = 0; i < MAX_INODES; i++) {
        if (!fs->inodes[i].is_valid) continue;
        for (int j = 0; j < fs->inodes[i].block_count; j++) {
            int bid = fs->inodes[i].block_pointers[j];
            if (bid >= 0) {
                /* Force-mark as used */
                if (bitmap_is_free(&fs->bitmap, bid)) {
                    int b[1] = {bid};
                    bitmap_allocate(&fs->bitmap, b, 1);
                }
            }
        }
        if (fs->inodes[i].index_block >= 0 &&
            bitmap_is_free(&fs->bitmap, fs->inodes[i].index_block)) {
            int b[1] = {fs->inodes[i].index_block};
            bitmap_allocate(&fs->bitmap, b, 1);
        }
    }
}

/* ── Lifecycle ─────────────────────────────────────────────── */

int fs_init(FileSystem *fs, const char *disk_path) {
    memset(fs, 0, sizeof(FileSystem));
    fs->mounted           = 0;
    fs->next_inode_id     = 1;
    fs->recovery_time_ms  = 0.0;
    fs->checkpoints_made  = 0;
    return disk_open(&fs->disk, disk_path);
}

void fs_close(FileSystem *fs) {
    disk_flush(&fs->disk);
    disk_close(&fs->disk);
}

int fs_format(FileSystem *fs) {
    printf("[FS] Formatting disk ...\n");
    if (disk_format(&fs->disk) < 0) return -1;

    write_superblock(fs);

    int reserved[DATA_START];
    int reserved_count;
    make_reserved(reserved, &reserved_count);
    bitmap_init(&fs->bitmap, TOTAL_BLOCKS, reserved, reserved_count);
    persist_bitmap(fs);

    /* Root inode (inode 0) */
    memset(fs->inodes, 0, sizeof(fs->inodes));
    inode_init(&fs->inodes[0], 0, FTYPE_DIR, ACCESS_SEQUENTIAL);
    persist_inode(fs, &fs->inodes[0]);
    fs->next_inode_id = 1;

    dir_init(&fs->dir_tree);
    disk_flush(&fs->disk);
    journal_truncate(&fs->journal, JOURNAL_PATH);
    fs->mounted = 1;
    fs_checkpoint(fs);
    printf("[FS] Format complete.\n");
    return 0;
}

int fs_mount(FileSystem *fs) {
    if (!disk_exists(fs->disk.path)) return -1;
    if (!check_superblock(fs)) return -1;
    if (fs->disk.crashed || journal_uncommitted_count(&fs->journal) > 0) {
        printf("[FS] Crash detected – running recovery ...\n");
        fs->mounted = 1;
        return fs_recover(fs);
    }
    load_bitmap(fs);
    load_inodes(fs);
    /* Load dir tree from checkpoint */
    if (checkpoint_load(&fs->checkpoint, CHECKPOINT_PATH) == 0)
        fs->dir_tree = fs->checkpoint.dir_tree;
    else
        dir_init(&fs->dir_tree);
    fs->mounted = 1;
    printf("[FS] File system mounted.\n");
    return 0;
}

/* ── File operations ───────────────────────────────────────── */

int fs_create_file(FileSystem *fs, const char *path, int access_method) {
    if (check_mounted(fs) < 0) return -1;
    char child_name[MAX_NAME_LEN];
    int parent = dir_resolve_parent(&fs->dir_tree, path, child_name);
    if (parent < 0) { fprintf(stderr, "[FS] Parent not found: %s\n", path); return -1; }

    int iid = alloc_inode_id(fs);
    inode_init(&fs->inodes[iid], iid, FTYPE_FILE, access_method);

    int txn = journal_log(&fs->journal, "CREATE_FILE", path, iid, parent, NULL, 0, 0);
    dir_create_file(&fs->dir_tree, parent, child_name, iid);
    persist_inode(fs, &fs->inodes[iid]);
    disk_flush(&fs->disk);
    journal_commit(&fs->journal, txn);
    journal_save(&fs->journal, JOURNAL_PATH);

    const char *methods[] = {"SEQUENTIAL", "DIRECT", "INDEXED"};
    printf("[FS] Created file '%s' (inode %d, %s)\n", path, iid, methods[access_method]);
    /* Auto-checkpoint so structure survives a crash */
    fs_checkpoint(fs);
    return iid;
}

int fs_write_file(FileSystem *fs, const char *path, const uint8_t *data, int len) {
    if (check_mounted(fs) < 0) return -1;
    int is_dir = 0;
    int iid = dir_resolve_path(&fs->dir_tree, path, &is_dir);
    if (iid < 0) { fprintf(stderr, "[FS] File not found: %s\n", path); return -1; }
    if (is_dir)  { fprintf(stderr, "[FS] %s is a directory\n", path); return -1; }

    Inode *node = &fs->inodes[iid];

    /* Free old blocks */
    if (node->block_count > 0)
        bitmap_free(&fs->bitmap, node->block_pointers, node->block_count);
    if (node->index_block >= 0) {
        int ib[1] = {node->index_block};
        bitmap_free(&fs->bitmap, ib, 1);
    }
    inode_remove_blocks(node);

    /* Allocate new blocks */
    int num_blocks = (len + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (num_blocks < 1) num_blocks = 1;
    int blocks[MAX_BLOCK_PTR];
    if (bitmap_allocate(&fs->bitmap, blocks, num_blocks) < 0) return -1;

    int txn = journal_log(&fs->journal, "WRITE_FILE", path, iid, -1, blocks, num_blocks, len);

    /* Write data blocks */
    uint8_t block[BLOCK_SIZE];
    for (int i = 0; i < num_blocks; i++) {
        memset(block, 0, BLOCK_SIZE);
        int s = i * BLOCK_SIZE;
        int chunk = (len - s) > BLOCK_SIZE ? BLOCK_SIZE : (len - s);
        if (chunk > 0) memcpy(block, data + s, chunk);
        disk_write_block(&fs->disk, blocks[i], block);
        node->block_pointers[i] = blocks[i];
    }
    node->block_count = num_blocks;
    node->size        = len;
    node->modified_at = (double)time(NULL);

    /* INDEXED: write index block */
    if (node->access_method == ACCESS_INDEXED) {
        int idx_blk[1];
        if (bitmap_allocate(&fs->bitmap, idx_blk, 1) == 0) {
            node->index_block = idx_blk[0];
            memset(block, 0, BLOCK_SIZE);
            memcpy(block, blocks, num_blocks * sizeof(int));
            disk_write_block(&fs->disk, node->index_block, block);
        }
    }

    persist_inode(fs, node);
    persist_bitmap(fs);
    disk_flush(&fs->disk);
    journal_commit(&fs->journal, txn);
    journal_save(&fs->journal, JOURNAL_PATH);
    printf("[FS] Wrote %d bytes to '%s' (%d blocks)\n", len, path, num_blocks);
    /* Auto-checkpoint so data survives a crash */
    fs_checkpoint(fs);
    return 0;
}

int fs_read_file(FileSystem *fs, const char *path, uint8_t *buf, int buf_len) {
    if (check_mounted(fs) < 0) return -1;
    int is_dir = 0;
    int iid = dir_resolve_path(&fs->dir_tree, path, &is_dir);
    if (iid < 0) { fprintf(stderr, "[FS] File not found: %s\n", path); return -1; }
    if (is_dir)  { fprintf(stderr, "[FS] %s is a directory\n", path); return -1; }

    Inode *node = &fs->inodes[iid];
    int read_len = node->size < buf_len ? node->size : buf_len;
    uint8_t block[BLOCK_SIZE];
    int pos = 0;

    int *block_ids = node->block_pointers;
    /* For INDEXED, read block IDs from the index block */
    int idx_blocks[MAX_BLOCK_PTR];
    if (node->access_method == ACCESS_INDEXED && node->index_block >= 0) {
        disk_read_block(&fs->disk, node->index_block, block);
        memcpy(idx_blocks, block, node->block_count * sizeof(int));
        block_ids = idx_blocks;
    }

    for (int i = 0; i < node->block_count && pos < read_len; i++) {
        disk_read_block(&fs->disk, block_ids[i], block);
        int chunk = (read_len - pos) > BLOCK_SIZE ? BLOCK_SIZE : (read_len - pos);
        memcpy(buf + pos, block, chunk);
        pos += chunk;
    }
    return read_len;
}

int fs_delete_file(FileSystem *fs, const char *path) {
    if (check_mounted(fs) < 0) return -1;
    char child_name[MAX_NAME_LEN];
    int parent = dir_resolve_parent(&fs->dir_tree, path, child_name);
    if (parent < 0) return -1;
    int is_dir = 0;
    int iid = dir_resolve_path(&fs->dir_tree, path, &is_dir);
    if (iid < 0) return -1;
    if (is_dir) { fprintf(stderr, "[FS] Use rmdir to remove a directory.\n"); return -1; }

    Inode *node = &fs->inodes[iid];
    int txn = journal_log(&fs->journal, "DELETE_FILE", path, iid, parent,
                          node->block_pointers, node->block_count, 0);
    if (node->block_count > 0) bitmap_free(&fs->bitmap, node->block_pointers, node->block_count);
    if (node->index_block >= 0) { int ib[1]={node->index_block}; bitmap_free(&fs->bitmap,ib,1); }
    dir_delete_entry(&fs->dir_tree, parent, child_name);
    memset(node, 0, sizeof(Inode));
    persist_inode_table(fs);
    persist_bitmap(fs);
    disk_flush(&fs->disk);
    journal_commit(&fs->journal, txn);
    journal_save(&fs->journal, JOURNAL_PATH);
    printf("[FS] Deleted file '%s'\n", path);
    return 0;
}

/* ── Directory operations ──────────────────────────────────── */

int fs_mkdir(FileSystem *fs, const char *path) {
    if (check_mounted(fs) < 0) return -1;
    char child_name[MAX_NAME_LEN];
    int parent = dir_resolve_parent(&fs->dir_tree, path, child_name);
    if (parent < 0) { fprintf(stderr, "[FS] Parent not found: %s\n", path); return -1; }

    int iid = alloc_inode_id(fs);
    inode_init(&fs->inodes[iid], iid, FTYPE_DIR, ACCESS_SEQUENTIAL);

    int txn = journal_log(&fs->journal, "MKDIR", path, iid, parent, NULL, 0, 0);
    dir_mkdir(&fs->dir_tree, parent, child_name, iid);
    persist_inode(fs, &fs->inodes[iid]);
    disk_flush(&fs->disk);
    journal_commit(&fs->journal, txn);
    journal_save(&fs->journal, JOURNAL_PATH);
    printf("[FS] Created directory '%s' (inode %d)\n", path, iid);
    /* Auto-checkpoint so directory tree survives a crash */
    fs_checkpoint(fs);
    return iid;
}

int fs_rmdir(FileSystem *fs, const char *path) {
    if (check_mounted(fs) < 0) return -1;
    char child_name[MAX_NAME_LEN];
    int parent = dir_resolve_parent(&fs->dir_tree, path, child_name);
    if (parent < 0) return -1;
    int is_dir = 0;
    int iid = dir_resolve_path(&fs->dir_tree, path, &is_dir);
    if (iid < 0 || !is_dir) return -1;

    /* Check empty */
    DirEntry entries[MAX_ENTRIES_PER_DIR];
    int n = dir_list(&fs->dir_tree, iid, entries, MAX_ENTRIES_PER_DIR);
    if (n > 0) { fprintf(stderr, "[FS] Directory not empty: %s\n", path); return -1; }

    int txn = journal_log(&fs->journal, "RMDIR", path, iid, parent, NULL, 0, 0);
    dir_delete_entry(&fs->dir_tree, parent, child_name);
    memset(&fs->inodes[iid], 0, sizeof(Inode));
    persist_inode_table(fs);
    disk_flush(&fs->disk);
    journal_commit(&fs->journal, txn);
    journal_save(&fs->journal, JOURNAL_PATH);
    printf("[FS] Removed directory '%s'\n", path);
    return 0;
}

int fs_ls(FileSystem *fs, const char *path, DirEntry *out, int max) {
    if (check_mounted(fs) < 0) return -1;
    int is_dir = 0;
    int iid = dir_resolve_path(&fs->dir_tree, path, &is_dir);
    if (iid < 0 || !is_dir) { fprintf(stderr, "[FS] Not a directory: %s\n", path); return -1; }
    int n = dir_list(&fs->dir_tree, iid, out, max);
    for (int i = 0; i < n; i++) {
        printf("  [%s] %s  (inode %d)\n",
               out[i].is_dir ? "DIR " : "FILE",
               out[i].name, out[i].inode_id);
    }
    return n;
}

int fs_rename(FileSystem *fs, const char *path, const char *new_name) {
    if (check_mounted(fs) < 0) return -1;
    char child_name[MAX_NAME_LEN];
    int parent = dir_resolve_parent(&fs->dir_tree, path, child_name);
    if (parent < 0) return -1;
    int txn = journal_log(&fs->journal, "RENAME", path, -1, parent, NULL, 0, 0);
    dir_rename(&fs->dir_tree, parent, child_name, new_name);
    disk_flush(&fs->disk);
    journal_commit(&fs->journal, txn);
    journal_save(&fs->journal, JOURNAL_PATH);
    printf("[FS] Renamed '%s' -> '%s'\n", child_name, new_name);
    return 0;
}

int fs_search(FileSystem *fs, const char *name,
              char results[][MAX_PATH_LEN], int max) {
    if (check_mounted(fs) < 0) return -1;
    return dir_search(&fs->dir_tree, name, results, max);
}

int fs_stat(FileSystem *fs, const char *path, Inode *out) {
    if (check_mounted(fs) < 0) return -1;
    int is_dir = 0;
    int iid = dir_resolve_path(&fs->dir_tree, path, &is_dir);
    if (iid < 0) { fprintf(stderr, "[FS] Not found: %s\n", path); return -1; }
    *out = fs->inodes[iid];
    inode_print(out);
    return 0;
}

void fs_tree(FileSystem *fs) {
    if (!fs->mounted) return;
    printf("/\n");
    dir_print_tree(&fs->dir_tree, 0, "", 1);
}

/* ── Crash & Recovery ──────────────────────────────────────── */

void fs_simulate_crash(FileSystem *fs, const char *kind, int block_id) {
    if (strcmp(kind, "power") == 0)
        disk_simulate_power_loss(&fs->disk);
    else if (strcmp(kind, "corrupt") == 0)
        disk_corrupt_block(&fs->disk, block_id > 0 ? block_id : DATA_START);
    else
        printf("[FS] Unknown crash kind: %s\n", kind);
    fs->mounted = 0;
}

int fs_recover(FileSystem *fs) {
    clock_t start = clock();
    printf("[RECOVERY] Starting recovery process ...\n");

    /* Clear crash flag FIRST so disk reads/writes work during recovery */
    fs->disk.crashed = 0;

    /* Step 1: journal replay */
    journal_load(&fs->journal, JOURNAL_PATH);
    int uncommitted = journal_uncommitted_count(&fs->journal);
    if (uncommitted > 0) {
        printf("[RECOVERY] Replaying %d uncommitted entries ...\n", uncommitted);
        for (int i = 0; i < fs->journal.count; i++) {
            if (fs->journal.entries[i].is_valid && !fs->journal.entries[i].committed)
                journal_commit(&fs->journal, fs->journal.entries[i].txn_id);
        }
        printf("[RECOVERY] Journal replay complete.\n");
    } else {
        printf("[RECOVERY] Journal clean - no uncommitted entries.\n");
    }

    /* Step 2: restore checkpoint */
    if (checkpoint_load(&fs->checkpoint, CHECKPOINT_PATH) == 0) {
        printf("[RECOVERY] Restoring from checkpoint ...\n");
        memcpy(fs->inodes, fs->checkpoint.inodes, sizeof(fs->inodes));
        fs->dir_tree     = fs->checkpoint.dir_tree;
        fs->next_inode_id = fs->checkpoint.next_inode_id;
        printf("[RECOVERY] Checkpoint restored.\n");
    } else {
        printf("[RECOVERY] No checkpoint found - starting fresh.\n");
        memset(fs->inodes, 0, sizeof(fs->inodes));
        inode_init(&fs->inodes[0], 0, FTYPE_DIR, ACCESS_SEQUENTIAL);
        dir_init(&fs->dir_tree);
        fs->next_inode_id = 1;
    }

    /* Step 3: rebuild bitmap from inodes */
    rebuild_bitmap_from_inodes(fs);
    persist_bitmap(fs);
    persist_inode_table(fs);
    disk_flush(&fs->disk);
    fs->mounted = 1;

    double elapsed = (double)(clock() - start) / CLOCKS_PER_SEC * 1000.0;
    fs->recovery_time_ms = elapsed;
    printf("[RECOVERY] Complete (%.1f ms)\n", elapsed);
    return 0;
}

int fs_checkpoint(FileSystem *fs) {
    build_checkpoint(fs);
    clock_t t0 = clock();
    int r = checkpoint_save(&fs->checkpoint, CHECKPOINT_PATH);
    fs->last_cp_ms = (double)(clock() - t0) / CLOCKS_PER_SEC * 1000.0;
    fs->checkpoints_made++;
    journal_truncate(&fs->journal, JOURNAL_PATH);
    return r;
}

int fs_backup(FileSystem *fs, const char *tag) {
    build_checkpoint(fs);
    return backup_create(&fs->disk, &fs->journal, &fs->checkpoint, tag);
}

int fs_restore(FileSystem *fs, const char *backup_name) {
    int r = backup_restore(&fs->disk, &fs->journal, &fs->checkpoint, backup_name);
    if (r < 0) return r;
    /* Reload after restore */
    memcpy(fs->inodes, fs->checkpoint.inodes, sizeof(fs->inodes));
    fs->dir_tree     = fs->checkpoint.dir_tree;
    fs->next_inode_id = fs->checkpoint.next_inode_id;
    rebuild_bitmap_from_inodes(fs);
    fs->mounted = 1;
    return 0;
}

/* ── Defragmentation ───────────────────────────────────────── */

int fs_defragment(FileSystem *fs) {
    if (check_mounted(fs) < 0) return -1;
    printf("[FS] Starting defragmentation ...\n");
    clock_t t0 = clock();

    int file_inodes[MAX_INODES];
    int count = dir_all_file_inodes(&fs->dir_tree, 0, file_inodes, MAX_INODES);
    if (count == 0) { printf("[FS] No files to defragment.\n"); return 0; }

    /* Read all file data */
    uint8_t *bufs[MAX_INODES];
    int      sizes[MAX_INODES];
    for (int i = 0; i < count; i++) {
        Inode *n = &fs->inodes[file_inodes[i]];
        sizes[i] = n->size;
        bufs[i]  = (uint8_t *)malloc(n->size > 0 ? n->size : 1);
        fs_read_file(fs, "", bufs[i], n->size); /* We'll read manually below */
        /* Manual read since we don't have path here */
        uint8_t blk[BLOCK_SIZE];
        int pos = 0;
        for (int j = 0; j < n->block_count && pos < n->size; j++) {
            disk_read_block(&fs->disk, n->block_pointers[j], blk);
            int chunk = (n->size - pos) > BLOCK_SIZE ? BLOCK_SIZE : (n->size - pos);
            memcpy(bufs[i] + pos, blk, chunk);
            pos += chunk;
        }
    }

    /* Free all data blocks */
    for (int i = 0; i < count; i++) {
        Inode *n = &fs->inodes[file_inodes[i]];
        if (n->block_count > 0)
            bitmap_free(&fs->bitmap, n->block_pointers, n->block_count);
        if (n->index_block >= 0) {
            int ib[1] = {n->index_block};
            bitmap_free(&fs->bitmap, ib, 1);
        }
        inode_remove_blocks(n);
    }

    /* Re-write files contiguously */
    for (int i = 0; i < count; i++) {
        Inode *n       = &fs->inodes[file_inodes[i]];
        int num_blocks = (sizes[i] + BLOCK_SIZE - 1) / BLOCK_SIZE;
        if (num_blocks < 1) num_blocks = 1;
        int blocks[MAX_BLOCK_PTR];
        bitmap_allocate(&fs->bitmap, blocks, num_blocks);
        uint8_t blk[BLOCK_SIZE];
        for (int j = 0; j < num_blocks; j++) {
            memset(blk, 0, BLOCK_SIZE);
            int s = j * BLOCK_SIZE;
            int chunk = (sizes[i] - s) > BLOCK_SIZE ? BLOCK_SIZE : (sizes[i] - s);
            if (chunk > 0) memcpy(blk, bufs[i] + s, chunk);
            disk_write_block(&fs->disk, blocks[j], blk);
            n->block_pointers[j] = blocks[j];
        }
        n->block_count = num_blocks;
        n->size        = sizes[i];
        n->modified_at = (double)time(NULL);
        if (n->access_method == ACCESS_INDEXED) {
            int idx_blk[1];
            bitmap_allocate(&fs->bitmap, idx_blk, 1);
            n->index_block = idx_blk[0];
            memset(blk, 0, BLOCK_SIZE);
            memcpy(blk, blocks, num_blocks * sizeof(int));
            disk_write_block(&fs->disk, n->index_block, blk);
        }
        free(bufs[i]);
    }

    persist_inode_table(fs);
    persist_bitmap(fs);
    disk_flush(&fs->disk);
    fs_checkpoint(fs);

    double elapsed = (double)(clock() - t0) / CLOCKS_PER_SEC * 1000.0;
    printf("[FS] Defragmentation complete (%.1f ms, %d files compacted)\n", elapsed, count);
    return 0;
}

/* ── Metrics ───────────────────────────────────────────────── */

void fs_print_metrics(FileSystem *fs) {
    printf("\n");
    printf("+============== Performance Metrics ==============+\n");
    disk_print_metrics(&fs->disk);
    printf("\n");
    if (fs->mounted && fs->bitmap.total > 0) {
        printf("  Total Blocks     : %d\n",  fs->bitmap.total);
        printf("  Used Blocks      : %d\n",  bitmap_used_count(&fs->bitmap));
        printf("  Free Blocks      : %d\n",  bitmap_free_count(&fs->bitmap));
        printf("  Utilization      : %.2f%%\n", bitmap_utilization(&fs->bitmap));
    }
    printf("\n");
    int files = 0, dirs = 0;
    for (int i = 0; i < MAX_INODES; i++) {
        if (fs->inodes[i].is_valid) {
            if (fs->inodes[i].file_type == FTYPE_FILE) files++;
            else dirs++;
        }
    }
    printf("  Recovery Time    : %.2f ms\n", fs->recovery_time_ms);
    printf("  Journal Entries  : %d\n",       fs->journal.count);
    printf("  Total Files      : %d\n",       files);
    printf("  Total Dirs       : %d\n",       dirs);
    printf("  Checkpoints Made : %d\n",       fs->checkpoints_made);
    printf("  Last CP Duration : %.1f ms\n",  fs->last_cp_ms);
    printf("+================================================+\n\n");
}
