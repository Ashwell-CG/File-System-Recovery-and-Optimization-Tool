/*
 * inode.h - Inode Metadata
 * ==========================
 * Stores file/directory metadata.  Each inode is exactly 256 bytes
 * so the inode table fits neatly into disk blocks.
 */

#ifndef INODE_H
#define INODE_H

#include <stdint.h>
#include "disk.h"

/* ── File types ─────────────────────────────────────────────── */
#define FTYPE_FILE 0
#define FTYPE_DIR  1

/* ── Access methods ─────────────────────────────────────────── */
#define ACCESS_SEQUENTIAL 0
#define ACCESS_DIRECT     1
#define ACCESS_INDEXED    2

/* ── Size ───────────────────────────────────────────────────── */
/* Must equal INODE_SERIAL_SIZE defined in disk.h (256) */
typedef struct {
    int32_t  inode_id;                    /*  4 */
    int32_t  file_type;                   /*  4 */
    int32_t  access_method;              /*  4 */
    int32_t  size;                        /*  4 */
    double   created_at;                  /*  8 */
    double   modified_at;                 /*  8 */
    int32_t  block_count;                 /*  4 */
    int32_t  index_block;                 /*  4  (-1 = none) */
    int32_t  is_valid;                    /*  4 */
    int32_t  block_pointers[MAX_BLOCK_PTR]; /* 192 */
    uint8_t  _pad[20];                   /* 20  -> total 256 bytes */
} Inode;


void inode_init(Inode *n, int inode_id, int file_type, int access_method);
void inode_add_block(Inode *n, int block_id);
void inode_remove_blocks(Inode *n);
void inode_serialize(const Inode *n, uint8_t *buf);
int  inode_deserialize(Inode *n, const uint8_t *buf);
void inode_print(const Inode *n);

#endif /* INODE_H */
