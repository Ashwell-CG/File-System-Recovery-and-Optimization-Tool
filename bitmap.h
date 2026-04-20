/*
 * bitmap.h - Bitmap Free Space Manager
 * ======================================
 * Tracks which disk blocks are free (0) or occupied (1).
 */

#ifndef BITMAP_H
#define BITMAP_H

#include "disk.h"
#include <stdint.h>

#define BITMAP_BYTES ((TOTAL_BLOCKS + 7) / 8)

typedef struct {
    uint8_t bits[BITMAP_BYTES];
    int     total;
    int     used;
} Bitmap;

void bitmap_init(Bitmap *bm, int total, int *reserved, int reserved_count);
int  bitmap_allocate(Bitmap *bm, int *out_blocks, int count);
void bitmap_free(Bitmap *bm, int *blocks, int count);
int  bitmap_is_free(Bitmap *bm, int block_id);
int  bitmap_free_count(Bitmap *bm);
int  bitmap_used_count(Bitmap *bm);
double bitmap_utilization(Bitmap *bm);
void bitmap_serialize(Bitmap *bm, uint8_t *buf);
void bitmap_deserialize(Bitmap *bm, const uint8_t *buf, int total);

#endif /* BITMAP_H */
