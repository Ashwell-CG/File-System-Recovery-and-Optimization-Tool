/*
 * disk.c - Virtual Disk implementation
 */

#include "disk.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <io.h>
#define F_OK 0
#define access _access
#else
#include <unistd.h>
#endif

/* ── Internal helpers ─────────────────────────────────────── */

static double now_ms(void) {
    return (double)clock() / CLOCKS_PER_SEC * 1000.0;
}

/* Find the least-recently-used cache slot */
static int cache_find_lru(VirtualDisk *d) {
    int lru = 0;
    for (int i = 1; i < CACHE_SIZE; i++) {
        if (!d->cache[i].valid) return i;
        if (d->cache[i].access_time < d->cache[lru].access_time) lru = i;
    }
    return lru;
}

/* Find a block in the cache; returns index or -1 */
static int cache_find(VirtualDisk *d, int block_id) {
    for (int i = 0; i < CACHE_SIZE; i++) {
        if (d->cache[i].valid && d->cache[i].block_id == block_id)
            return i;
    }
    return -1;
}

/* Write a dirty cache slot to the actual file */
static int cache_flush_slot(VirtualDisk *d, int idx) {
    if (!d->cache[idx].valid || !d->cache[idx].dirty) return 0;
    long offset = (long)d->cache[idx].block_id * BLOCK_SIZE;
    if (fseek(d->fp, offset, SEEK_SET) != 0) return -1;
    if (fwrite(d->cache[idx].data, 1, BLOCK_SIZE, d->fp) != BLOCK_SIZE) return -1;
    d->cache[idx].dirty = 0;
    return 0;
}

/* ── Public API ────────────────────────────────────────────── */

int disk_exists(const char *path) {
    return access(path, F_OK) == 0;
}

int disk_open(VirtualDisk *d, const char *path) {
    strncpy(d->path, path, 255);
    d->total_blocks = TOTAL_BLOCKS;
    d->block_size   = BLOCK_SIZE;
    d->crashed      = 0;
    d->timer        = 0;
    memset(d->cache, 0, sizeof(d->cache));
    memset(&d->metrics, 0, sizeof(d->metrics));

    /* Open existing or create new */
    d->fp = fopen(path, "r+b");
    if (!d->fp) {
        d->fp = fopen(path, "w+b");
        if (!d->fp) {
            perror("[DISK] Cannot open disk file");
            return -1;
        }
    }
    return 0;
}

void disk_close(VirtualDisk *d) {
    disk_flush(d);
    if (d->fp) { fclose(d->fp); d->fp = NULL; }
}

int disk_format(VirtualDisk *d) {
    if (!d->fp) return -1;
    rewind(d->fp);
    uint8_t zero[BLOCK_SIZE];
    memset(zero, 0, BLOCK_SIZE);
    for (int i = 0; i < TOTAL_BLOCKS; i++) {
        if (fwrite(zero, 1, BLOCK_SIZE, d->fp) != BLOCK_SIZE) return -1;
    }
    fflush(d->fp);
    /* Clear cache */
    memset(d->cache, 0, sizeof(d->cache));
    d->crashed = 0;
    return 0;
}

int disk_read_block(VirtualDisk *d, int block_id, uint8_t *buf) {
    if (d->crashed) { fprintf(stderr, "[DISK] Disk crashed!\n"); return -1; }
    if (block_id < 0 || block_id >= TOTAL_BLOCKS) return -1;

    double t0 = now_ms();

    int idx = cache_find(d, block_id);
    if (idx >= 0) {
        /* Cache hit */
        memcpy(buf, d->cache[idx].data, BLOCK_SIZE);
        d->cache[idx].access_time = ++d->timer;
        d->metrics.cache_hits++;
        d->metrics.total_read_ms += now_ms() - t0;
        d->metrics.reads++;
        return 0;
    }

    /* Cache miss - read from file */
    d->metrics.cache_misses++;
    long offset = (long)block_id * BLOCK_SIZE;
    if (fseek(d->fp, offset, SEEK_SET) != 0) return -1;
    if (fread(buf, 1, BLOCK_SIZE, d->fp) != BLOCK_SIZE) {
        memset(buf, 0, BLOCK_SIZE); /* treat short read as zeros */
    }

    /* Store into cache */
    int slot = cache_find_lru(d);
    if (d->cache[slot].valid && d->cache[slot].dirty)
        cache_flush_slot(d, slot);
    d->cache[slot].valid       = 1;
    d->cache[slot].block_id    = block_id;
    d->cache[slot].dirty       = 0;
    d->cache[slot].access_time = ++d->timer;
    memcpy(d->cache[slot].data, buf, BLOCK_SIZE);

    d->metrics.reads++;
    d->metrics.total_read_ms += now_ms() - t0;
    return 0;
}

int disk_write_block(VirtualDisk *d, int block_id, const uint8_t *buf) {
    if (d->crashed) { fprintf(stderr, "[DISK] Disk crashed!\n"); return -1; }
    if (block_id < 0 || block_id >= TOTAL_BLOCKS) return -1;

    double t0 = now_ms();

    int idx = cache_find(d, block_id);
    if (idx < 0) {
        idx = cache_find_lru(d);
        if (d->cache[idx].valid && d->cache[idx].dirty)
            cache_flush_slot(d, idx);
        d->cache[idx].valid    = 1;
        d->cache[idx].block_id = block_id;
    }
    memcpy(d->cache[idx].data, buf, BLOCK_SIZE);
    d->cache[idx].dirty       = 1;
    d->cache[idx].access_time = ++d->timer;

    d->metrics.writes++;
    d->metrics.total_write_ms += now_ms() - t0;
    return 0;
}

int disk_flush(VirtualDisk *d) {
    if (!d->fp) return -1;
    for (int i = 0; i < CACHE_SIZE; i++) {
        if (d->cache[i].valid && d->cache[i].dirty)
            cache_flush_slot(d, i);
    }
    fflush(d->fp);
    return 0;
}

void disk_simulate_power_loss(VirtualDisk *d) {
    int lost = 0;
    for (int i = 0; i < CACHE_SIZE; i++) {
        if (d->cache[i].valid && d->cache[i].dirty) {
            d->cache[i].valid = 0;
            lost++;
        }
    }
    d->crashed = 1;
    printf("[CRASH] Power loss! %d dirty blocks lost.\n", lost);
    printf("[FS] System is now in CRASHED state. Run recover.\n");
}

void disk_corrupt_block(VirtualDisk *d, int block_id) {
    uint8_t buf[BLOCK_SIZE];
    memset(buf, 0xAB, BLOCK_SIZE); /* fill with garbage */
    long offset = (long)block_id * BLOCK_SIZE;
    fseek(d->fp, offset, SEEK_SET);
    fwrite(buf, 1, BLOCK_SIZE, d->fp);
    d->crashed = 1;
    printf("[CRASH] Block %d corrupted!\n", block_id);
}

void disk_print_metrics(VirtualDisk *d) {
    long total_reads = d->metrics.reads;
    long hits        = d->metrics.cache_hits;
    double hit_rate  = (total_reads > 0) ? (hits * 100.0 / total_reads) : 0.0;
    double avg_r     = (total_reads  > 0) ? d->metrics.total_read_ms  / total_reads  : 0;
    double avg_w     = (d->metrics.writes > 0) ? d->metrics.total_write_ms / d->metrics.writes : 0;

    printf("  Disk Reads       : %ld\n", d->metrics.reads);
    printf("  Disk Writes      : %ld\n", d->metrics.writes);
    printf("  Avg Read Time    : %.3f ms\n", avg_r);
    printf("  Avg Write Time   : %.3f ms\n", avg_w);
    printf("  Cache Hit Rate   : %.1f%%\n", hit_rate);
}
