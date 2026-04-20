/*
 * directory.c - Directory tree implementation
 */

#include "directory.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>


void dir_init(DirTree *t) {
    memset(t, 0, sizeof(DirTree));
    /* Create root directory node (inode 0) */
    t->nodes[0].dir_inode   = 0;
    t->nodes[0].entry_count = 0;
    t->nodes[0].is_valid    = 1;
    t->node_count           = 1;
}

DirNode *dir_find_node(DirTree *t, int dir_inode) {
    for (int i = 0; i < MAX_DIRS; i++) {
        if (t->nodes[i].is_valid && t->nodes[i].dir_inode == dir_inode)
            return &t->nodes[i];
    }
    return NULL;
}

DirNode *dir_get_or_create_node(DirTree *t, int dir_inode) {
    DirNode *n = dir_find_node(t, dir_inode);
    if (n) return n;
    /* Find an empty slot */
    for (int i = 0; i < MAX_DIRS; i++) {
        if (!t->nodes[i].is_valid) {
            t->nodes[i].dir_inode   = dir_inode;
            t->nodes[i].entry_count = 0;
            t->nodes[i].is_valid    = 1;
            if (i >= t->node_count) t->node_count = i + 1;
            return &t->nodes[i];
        }
    }
    return NULL;
}

int dir_resolve_path(DirTree *t, const char *path, int *is_dir) {
    if (strcmp(path, "/") == 0) { *is_dir = 1; return 0; }
    char *parts[32];
    int   count = 0;
    char  buf[MAX_PATH_LEN];
    strncpy(buf, path, MAX_PATH_LEN - 1);
    int cur = 0;
    char *tok = strtok(buf, "/");
    while (tok) {
        parts[count++] = tok;
        tok = strtok(NULL, "/");
    }
    for (int i = 0; i < count; i++) {
        DirNode *node = dir_find_node(t, cur);
        if (!node) return -1;
        int found = 0;
        for (int j = 0; j < node->entry_count; j++) {
            if (node->entries[j].is_valid &&
                strcmp(node->entries[j].name, parts[i]) == 0) {
                cur     = node->entries[j].inode_id;
                *is_dir = node->entries[j].is_dir;
                found   = 1;
                break;
            }
        }
        if (!found) return -1;
    }
    return cur;
}

int dir_resolve_parent(DirTree *t, const char *path, char *child_name) {
    char buf[MAX_PATH_LEN];
    strncpy(buf, path, MAX_PATH_LEN - 1);
    /* Find last '/' */
    char *last_slash = strrchr(buf, '/');
    if (!last_slash) return -1;
    strncpy(child_name, last_slash + 1, MAX_NAME_LEN - 1);
    if (last_slash == buf) {
        /* Parent is root */
        return 0;
    }
    *last_slash = '\0';
    int is_dir = 0;
    int pid = dir_resolve_path(t, buf, &is_dir);
    return (is_dir) ? pid : -1;
}

static int dir_add_entry(DirTree *t, int parent_inode, const char *name,
                         int new_inode, int is_dir) {
    DirNode *node = dir_find_node(t, parent_inode);
    if (!node) return -1;
    if (node->entry_count >= MAX_ENTRIES_PER_DIR) return -1;
    /* Check duplicate */
    for (int i = 0; i < node->entry_count; i++) {
        if (node->entries[i].is_valid &&
            strcmp(node->entries[i].name, name) == 0) return -1;
    }
    DirEntry *e = &node->entries[node->entry_count++];
    strncpy(e->name, name, MAX_NAME_LEN - 1);
    e->inode_id = new_inode;
    e->is_dir   = is_dir;
    e->is_valid = 1;
    return 0;
}

int dir_mkdir(DirTree *t, int parent_inode, const char *name, int new_inode) {
    if (dir_add_entry(t, parent_inode, name, new_inode, 1) < 0) return -1;
    /* Create empty node for the new directory */
    dir_get_or_create_node(t, new_inode);
    return 0;
}

int dir_create_file(DirTree *t, int parent_inode, const char *name, int new_inode) {
    return dir_add_entry(t, parent_inode, name, new_inode, 0);
}

int dir_delete_entry(DirTree *t, int parent_inode, const char *name) {
    DirNode *node = dir_find_node(t, parent_inode);
    if (!node) return -1;
    for (int i = 0; i < node->entry_count; i++) {
        if (node->entries[i].is_valid &&
            strcmp(node->entries[i].name, name) == 0) {
            if (node->entries[i].is_dir) {
                /* Remove the child's DirNode too */
                DirNode *child = dir_find_node(t, node->entries[i].inode_id);
                if (child) child->is_valid = 0;
            }
            node->entries[i].is_valid = 0;
            /* Compact: shift remaining entries */
            for (int j = i; j < node->entry_count - 1; j++)
                node->entries[j] = node->entries[j+1];
            node->entry_count--;
            return 0;
        }
    }
    return -1;
}

int dir_rename(DirTree *t, int parent_inode, const char *old_name, const char *new_name) {
    DirNode *node = dir_find_node(t, parent_inode);
    if (!node) return -1;
    for (int i = 0; i < node->entry_count; i++) {
        if (node->entries[i].is_valid &&
            strcmp(node->entries[i].name, old_name) == 0) {
            strncpy(node->entries[i].name, new_name, MAX_NAME_LEN - 1);
            return 0;
        }
    }
    return -1;
}

int dir_list(DirTree *t, int dir_inode, DirEntry *out, int max) {
    DirNode *node = dir_find_node(t, dir_inode);
    if (!node) return 0;
    int count = 0;
    for (int i = 0; i < node->entry_count && count < max; i++) {
        if (node->entries[i].is_valid)
            out[count++] = node->entries[i];
    }
    return count;
}

int dir_all_file_inodes(DirTree *t, int start_inode, int *out, int max) {
    DirNode *node = dir_find_node(t, start_inode);
    if (!node) return 0;
    int count = 0;
    for (int i = 0; i < node->entry_count; i++) {
        if (!node->entries[i].is_valid) continue;
        if (node->entries[i].is_dir) {
            count += dir_all_file_inodes(t, node->entries[i].inode_id,
                                         out + count, max - count);
        } else {
            if (count < max) out[count++] = node->entries[i].inode_id;
        }
    }
    return count;
}

static void dir_search_recursive(DirTree *t, const char *name, int inode,
                                 const char *cur_path,
                                 char results[][MAX_PATH_LEN], int *found, int max) {
    DirNode *node = dir_find_node(t, inode);
    if (!node) return;
    for (int i = 0; i < node->entry_count; i++) {
        if (!node->entries[i].is_valid) continue;
        char child_path[MAX_PATH_LEN];
        snprintf(child_path, MAX_PATH_LEN, "%s/%s",
                 strcmp(cur_path, "/") == 0 ? "" : cur_path,
                 node->entries[i].name);
        if (strcmp(node->entries[i].name, name) == 0 && *found < max) {
            strncpy(results[(*found)++], child_path, MAX_PATH_LEN - 1);
        }
        if (node->entries[i].is_dir)
            dir_search_recursive(t, name, node->entries[i].inode_id,
                                 child_path, results, found, max);
    }
}

int dir_search(DirTree *t, const char *name, char results[][MAX_PATH_LEN], int max) {
    int found = 0;
    dir_search_recursive(t, name, 0, "/", results, &found, max);
    return found;
}

void dir_print_tree(DirTree *t, int inode, const char *prefix, int is_last) {
    DirNode *node = dir_find_node(t, inode);
    if (!node) return;
    for (int i = 0; i < node->entry_count; i++) {
        if (!node->entries[i].is_valid) continue;
        int last = (i == node->entry_count - 1);
        printf("%s%s %s %s  (inode %d)\n",
               prefix,
               last ? "`--" : "|--",
               node->entries[i].is_dir ? "[DIR]" : "[FILE]",
               node->entries[i].name,
               node->entries[i].inode_id);
        if (node->entries[i].is_dir) {
            char new_prefix[256];
            snprintf(new_prefix, 255, "%s%s", prefix, last ? "    " : "|   ");
            dir_print_tree(t, node->entries[i].inode_id, new_prefix, last);
        }
    }
}

int dir_save(DirTree *t, const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    fwrite(t, sizeof(DirTree), 1, f);
    fclose(f);
    return 0;
}

int dir_load(DirTree *t, const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fread(t, sizeof(DirTree), 1, f);
    fclose(f);
    return 0;
}
