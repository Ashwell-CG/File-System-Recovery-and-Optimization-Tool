/*
 * disk.h - Virtual Disk Simulator
 * =================================
 * Provides raw block-level I/O over a host file (virtual_disk.bin).
 * Includes an LRU write-back block cache and crash injection.
 */

#ifndef DISK_H
#define DISK_H

#include <stdint.h>
#include <stdio.h>

/* ── Layout constants ──────────────────────────────────────── */
#define BLOCK_SIZE        4096
#define TOTAL_BLOCKS      2560
#define DISK_SIZE         (BLOCK_SIZE * TOTAL_BLOCKS)   /* 10 MB */
#define SUPERBLOCK_ID     0
#define BITMAP_START      1
#define BITMAP_BLOCKS     3
#define INODE_TABLE_START 4
#define INODE_TABLE_BLOCKS 64
#define INODES_PER_BLOCK  16
#define MAX_INODES        (INODE_TABLE_BLOCKS * INODES_PER_BLOCK)
#define DATA_START        68
#define INODE_SERIAL_SIZE 256
#define MAX_BLOCK_PTR     48
#define MAGIC             "FSIM"
#define FS_VERSION        1

/* ── LRU Cache ─────────────────────────────────────────────── */
#define CACHE_SIZE        64

typedef struct {
    int      block_id;
    uint8_t  data[BLOCK_SIZE];
    int      dirty;
    long long access_time;
    int      valid;
} CacheEntry;

/* ── Performance metrics ───────────────────────────────────── */
typedef struct {
    long reads;
    long writes;
    long cache_hits;
    long cache_misses;
    double total_read_ms;
    double total_write_ms;
} DiskMetrics;

/* ── Virtual Disk ──────────────────────────────────────────── */
typedef struct {
    char         path[256];
    FILE        *fp;
    int          total_blocks;
    int          block_size;
    int          crashed;
    CacheEntry   cache[CACHE_SIZE];
    long long    timer;
    DiskMetrics  metrics;
} VirtualDisk;

/* ── Function prototypes ───────────────────────────────────── */
int  disk_open(VirtualDisk *d, const char *path);
void disk_close(VirtualDisk *d);
int  disk_format(VirtualDisk *d);
int  disk_read_block(VirtualDisk *d, int block_id, uint8_t *buf);
int  disk_write_block(VirtualDisk *d, int block_id, const uint8_t *buf);
int  disk_flush(VirtualDisk *d);
void disk_simulate_power_loss(VirtualDisk *d);
void disk_corrupt_block(VirtualDisk *d, int block_id);
void disk_print_metrics(VirtualDisk *d);
int  disk_exists(const char *path);

#endif /* DISK_H */
