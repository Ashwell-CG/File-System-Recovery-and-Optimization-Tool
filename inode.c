/*
 * inode.c - Inode implementation
 */

#include "inode.h"
#include <string.h>
#include <stdio.h>
#include <time.h>


void inode_init(Inode *n, int inode_id, int file_type, int access_method) {
    memset(n, 0, sizeof(Inode));
    n->inode_id      = inode_id;
    n->file_type     = file_type;
    n->access_method = access_method;
    n->size          = 0;
    n->block_count   = 0;
    n->index_block   = -1;
    n->is_valid      = 1;
    n->created_at    = (double)time(NULL);
    n->modified_at   = n->created_at;
    for (int i = 0; i < MAX_BLOCK_PTR; i++)
        n->block_pointers[i] = -1;
}

void inode_add_block(Inode *n, int block_id) {
    if (n->block_count < MAX_BLOCK_PTR)
        n->block_pointers[n->block_count++] = block_id;
}

void inode_remove_blocks(Inode *n) {
    for (int i = 0; i < MAX_BLOCK_PTR; i++)
        n->block_pointers[i] = -1;
    n->block_count = 0;
    n->index_block = -1;
    n->size        = 0;
}

void inode_serialize(const Inode *n, uint8_t *buf) {
    memcpy(buf, n, INODE_SERIAL_SIZE);
}

int inode_deserialize(Inode *n, const uint8_t *buf) {
    memcpy(n, buf, INODE_SERIAL_SIZE);
    return n->is_valid;
}

void inode_print(const Inode *n) {
    const char *methods[] = {"SEQUENTIAL", "DIRECT", "INDEXED"};
    const char *types[]   = {"FILE", "DIR"};
    printf("  inode_id       : %d\n", n->inode_id);
    printf("  type           : %s\n", types[n->file_type]);
    printf("  access_method  : %s\n", methods[n->access_method]);
    printf("  size           : %d bytes\n", n->size);
    printf("  block_count    : %d\n", n->block_count);
    printf("  index_block    : %d\n", n->index_block);
    printf("  blocks         : [");
    for (int i = 0; i < n->block_count; i++)
        printf("%d%s", n->block_pointers[i], i < n->block_count-1 ? "," : "");
    printf("]\n");
    time_t ct = (time_t)n->created_at;
    time_t mt = (time_t)n->modified_at;
    printf("  created        : %s", ctime(&ct));
    printf("  modified       : %s", ctime(&mt));
}
// done
