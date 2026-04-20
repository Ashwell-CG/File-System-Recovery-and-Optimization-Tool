/*
 * directory.h - Hierarchical Directory Structure
 * ================================================
 * A flat-array tree: each directory (identified by its inode_id) owns a
 * fixed-size list of DirEntry children.
 */

#ifndef DIRECTORY_H
#define DIRECTORY_H

#define MAX_DIRS            256
#define MAX_ENTRIES_PER_DIR  64
#define MAX_NAME_LEN        128
#define MAX_PATH_LEN        512

typedef struct {
    char name[MAX_NAME_LEN];
    int  inode_id;
    int  is_dir;
    int  is_valid;
} DirEntry;

typedef struct {
    int      dir_inode;
    DirEntry entries[MAX_ENTRIES_PER_DIR];
    int      entry_count;
    int      is_valid;
} DirNode;

typedef struct {
    DirNode nodes[MAX_DIRS];
    int     node_count;
} DirTree;

void dir_init(DirTree *t);
DirNode *dir_find_node(DirTree *t, int dir_inode);
DirNode *dir_get_or_create_node(DirTree *t, int dir_inode);

int  dir_mkdir(DirTree *t, int parent_inode, const char *name, int new_inode);
int  dir_create_file(DirTree *t, int parent_inode, const char *name, int new_inode);
int  dir_delete_entry(DirTree *t, int parent_inode, const char *name);
int  dir_rename(DirTree *t, int parent_inode, const char *old_name, const char *new_name);

/* Returns inode_id or -1. Sets *is_dir */
int  dir_resolve_path(DirTree *t, const char *path, int *is_dir);
/* Returns parent inode or -1. Sets child_name */
int  dir_resolve_parent(DirTree *t, const char *path, char *child_name);

/* List entries in a dir; returns count, fills entries array */
int  dir_list(DirTree *t, int dir_inode, DirEntry *out, int max);

/* Recursively collect all file inodes */
int  dir_all_file_inodes(DirTree *t, int start_inode, int *out, int max);

/* Search by name; returns count of paths found */
int  dir_search(DirTree *t, const char *name, char results[][MAX_PATH_LEN], int max);

/* Pretty-print the tree */
void dir_print_tree(DirTree *t, int inode, const char *prefix, int is_last);

/* Serialization to/from a binary file */
int  dir_save(DirTree *t, const char *path);
int  dir_load(DirTree *t, const char *path);

#endif /* DIRECTORY_H */
