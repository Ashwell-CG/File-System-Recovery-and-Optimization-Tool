/*
 * bitmap.c - Bitmap Free Space Manager implementation
 */

#include "bitmap.h"
#include <stdio.h>
#include <string.h>

static void bm_set(Bitmap *bm, int block_id, int occupied) {
    int byte_idx = block_id / 8;
    int bit_idx  = block_id % 8;
    if (occupied) {
        bm->bits[byte_idx] |= (1 << bit_idx);
        bm->used++;
    } else {
        bm->bits[byte_idx] &= ~(1 << bit_idx);
        bm->used--;
    }
}

static int bm_get(Bitmap *bm, int block_id) {
    int byte_idx = block_id / 8;
    int bit_idx  = block_id % 8;
    return (bm->bits[byte_idx] >> bit_idx) & 1;
}

void bitmap_init(Bitmap *bm, int total, int *reserved, int reserved_count) {
    memset(bm->bits, 0, sizeof(bm->bits));
    bm->total = total;
    bm->used  = 0;
    for (int i = 0; i < reserved_count; i++)
        bm_set(bm, reserved[i], 1);
}

int bitmap_allocate(Bitmap *bm, int *out_blocks, int count) {
    int found = 0;
    for (int i = 0; i < bm->total && found < count; i++) {
        if (!bm_get(bm, i)) {
            out_blocks[found++] = i;
        }
    }
    if (found < count) {
        fprintf(stderr, "[BITMAP] Not enough free blocks: need %d, have %d\n",
                count, bitmap_free_count(bm));
        return -1;
    }
    for (int i = 0; i < count; i++)
        bm_set(bm, out_blocks[i], 1);
    return 0;
}

void bitmap_free(Bitmap *bm, int *blocks, int count) {
    for (int i = 0; i < count; i++) {
        if (blocks[i] >= 0 && bm_get(bm, blocks[i]))
            bm_set(bm, blocks[i], 0);
    }
}

int bitmap_is_free(Bitmap *bm, int block_id) {
    return !bm_get(bm, block_id);
}

int bitmap_free_count(Bitmap *bm) {
    return bm->total - bm->used;
}

int bitmap_used_count(Bitmap *bm) {
    return bm->used;
}

double bitmap_utilization(Bitmap *bm) {
    return bm->total ? (bm->used * 100.0 / bm->total) : 0.0;
}

void bitmap_serialize(Bitmap *bm, uint8_t *buf) {
    memcpy(buf, bm->bits, BITMAP_BYTES);
}

void bitmap_deserialize(Bitmap *bm, const uint8_t *buf, int total) {
    bm->total = total;
    memcpy(bm->bits, buf, BITMAP_BYTES);
    /* Recount used bits */
    bm->used = 0;
    for (int i = 0; i < total; i++) {
        if (bm_get(bm, i)) bm->used++;
    }
}
