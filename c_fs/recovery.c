/*
 * recovery.c - Journal, Checkpoint, and Backup implementation
 */

#include "recovery.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#ifdef _WIN32
#include <direct.h>
#define mkdir(p,m) _mkdir(p)
#else
#include <sys/stat.h>
#endif

/* ── Journal ──────────────────────────────────────────────── */

void journal_init(Journal *j) {
    memset(j, 0, sizeof(Journal));
    j->next_txn_id = 1;
}

int journal_log(Journal *j, const char *op, const char *path,
                int inode_id, int parent_inode,
                int *blocks, int block_count, int size) {
    if (j->count >= MAX_JOURNAL_ENTRIES) {
        fprintf(stderr, "[JOURNAL] Journal full!\n");
        return -1;
    }
    JournalEntry *e = &j->entries[j->count++];
    memset(e, 0, sizeof(*e));
    e->txn_id      = j->next_txn_id++;
    strncpy(e->op_type, op,   31);
    strncpy(e->path,    path, MAX_PATH_LEN - 1);
    e->inode_id    = inode_id;
    e->parent_inode = parent_inode;
    e->size        = size;
    e->committed   = 0;
    e->is_valid    = 1;
    e->block_count = block_count;
    if (blocks && block_count > 0) {
        int n = block_count < MAX_BLOCK_PTR ? block_count : MAX_BLOCK_PTR;
        memcpy(e->blocks, blocks, n * sizeof(int));
    }
    return e->txn_id;
}

void journal_commit(Journal *j, int txn_id) {
    for (int i = 0; i < j->count; i++) {
        if (j->entries[i].is_valid && j->entries[i].txn_id == txn_id)
            j->entries[i].committed = 1;
    }
}

int journal_uncommitted_count(Journal *j) {
    int n = 0;
    for (int i = 0; i < j->count; i++) {
        if (j->entries[i].is_valid && !j->entries[i].committed) n++;
    }
    return n;
}

int journal_save(Journal *j, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    fwrite(j, sizeof(Journal), 1, f);
    fclose(f);
    return 0;
}

int journal_load(Journal *j, const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) { journal_init(j); return -1; }
    fread(j, sizeof(Journal), 1, f);
    fclose(f);
    return 0;
}

void journal_truncate(Journal *j, const char *path) {
    memset(j->entries, 0, sizeof(j->entries));
    j->count = 0;
    if (path) {
        FILE *f = fopen(path, "wb");
        if (f) { fwrite(j, sizeof(Journal), 1, f); fclose(f); }
    }
    printf("[JOURNAL] Truncated.\n");
}

/* ── Checkpoint ────────────────────────────────────────────── */

int checkpoint_save(Checkpoint *cp, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    cp->magic = 0xC5EC;
    fwrite(cp, sizeof(Checkpoint), 1, f);
    fclose(f);
    printf("[CHECKPOINT] Metadata saved.\n");
    return 0;
}

int checkpoint_load(Checkpoint *cp, const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fread(cp, sizeof(Checkpoint), 1, f);
    fclose(f);
    if (cp->magic != 0xC5EC) return -1;
    return 0;
}

/* ── Backup ────────────────────────────────────────────────── */

static void copy_file(const char *src, const char *dst) {
    FILE *fs = fopen(src, "rb");
    FILE *fd = fopen(dst, "wb");
    if (!fs || !fd) { if (fs) fclose(fs); if (fd) fclose(fd); return; }
    char buf[65536];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fs)) > 0)
        fwrite(buf, 1, n, fd);
    fclose(fs); fclose(fd);
}

int backup_create(VirtualDisk *d, Journal *j, Checkpoint *cp, const char *tag) {
    /* Flush disk first */
    disk_flush(d);

    /* Ensure backups directory exists */
    mkdir(BACKUP_DIR, 0755);

    /* Get timestamp */
    time_t t = time(NULL);
    struct tm *tm_info = localtime(&t);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y%m%d_%H%M%S", tm_info);

    /* Build filenames */
    char disk_bk[512], jrnl_bk[512], cp_bk[512];
    snprintf(disk_bk, 511, "%s/backup_%s_%s.bin",   BACKUP_DIR, ts, tag);
    snprintf(jrnl_bk, 511, "%s/backup_%s_%s.bin_journal.bin", BACKUP_DIR, ts, tag);
    snprintf(cp_bk,   511, "%s/backup_%s_%s.bin_checkpoint.bin", BACKUP_DIR, ts, tag);

    copy_file(d->path, disk_bk);
    journal_save(j,  jrnl_bk);
    checkpoint_save(cp, cp_bk);

    printf("[BACKUP] Created: %s\n", disk_bk);
    return 0;
}

int backup_restore(VirtualDisk *d, Journal *j, Checkpoint *cp, const char *backup_name) {
    /* Build paths */
    char disk_bk[512], jrnl_bk[512], cp_bk[512];
    snprintf(disk_bk, 511, "%s/%s",                  BACKUP_DIR, backup_name);
    snprintf(jrnl_bk, 511, "%s/%s_journal.bin",      BACKUP_DIR, backup_name);
    snprintf(cp_bk,   511, "%s/%s_checkpoint.bin",   BACKUP_DIR, backup_name);

    disk_flush(d);
    copy_file(disk_bk, d->path);
    journal_load(j, jrnl_bk);
    checkpoint_load(cp, cp_bk);
    d->crashed = 0;

    printf("[BACKUP] Restored from: %s\n", backup_name);
    return 0;
}
