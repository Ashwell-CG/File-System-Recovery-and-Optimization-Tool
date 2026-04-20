/*
 * recovery.h - Journal, Checkpoint, Backup
 * ==========================================
 */

#ifndef RECOVERY_H
#define RECOVERY_H

#include "disk.h"
#include "directory.h"
#include "inode.h"

#define MAX_JOURNAL_ENTRIES 256
#define JOURNAL_PATH    "fs_journal.bin"
#define CHECKPOINT_PATH "fs_checkpoint.bin"
#define BACKUP_DIR      "backups"

/* ── Journal Entry ── */
typedef struct {
    int32_t txn_id;
    char    op_type[32];
    char    path[MAX_PATH_LEN];
    int32_t inode_id;
    int32_t parent_inode;
    int32_t blocks[MAX_BLOCK_PTR];
    int32_t block_count;
    int32_t size;
    int32_t committed;  /* 0 = pending, 1 = committed */
    int32_t is_valid;
} JournalEntry;

typedef struct {
    JournalEntry entries[MAX_JOURNAL_ENTRIES];
    int          count;
    int          next_txn_id;
} Journal;

/* ── Checkpoint (binary snapshot) ── */
typedef struct {
    int    magic;           /* 0xC5EC */
    int    next_inode_id;
    int    inode_count;
    Inode  inodes[MAX_INODES];
    DirTree dir_tree;
} Checkpoint;

/* ── Journal API ── */
void journal_init(Journal *j);
int  journal_log(Journal *j, const char *op, const char *path,
                 int inode_id, int parent_inode,
                 int *blocks, int block_count, int size);
void journal_commit(Journal *j, int txn_id);
int  journal_uncommitted_count(Journal *j);
int  journal_save(Journal *j, const char *path);
int  journal_load(Journal *j, const char *path);
void journal_truncate(Journal *j, const char *path);

/* ── Checkpoint API ── */
int checkpoint_save(Checkpoint *cp, const char *path);
int checkpoint_load(Checkpoint *cp, const char *path);

/* ── Backup API ── */
int backup_create(VirtualDisk *d, Journal *j, Checkpoint *cp, const char *tag);
int backup_restore(VirtualDisk *d, Journal *j, Checkpoint *cp, const char *backup_name);

#endif /* RECOVERY_H */
