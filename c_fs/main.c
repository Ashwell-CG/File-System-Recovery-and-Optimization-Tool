/*
 * main.c - Interactive CLI for the C File System Simulator
 * ==========================================================
 * Same commands as the Python cli.py, now in C.
 *
 * Usage:
 *   ./fs_sim
 *
 * Commands:
 *   format          - Create fresh file system
 *   mkdir <path>    - Create directory
 *   ls <path>       - List directory contents
 *   create <path> [seq|dir|idx] - Create a file
 *   write <path> <data...>      - Write text to file
 *   read <path>     - Read and print file content
 *   delete <path>   - Delete file
 *   rmdir <path>    - Remove empty directory
 *   rename <path> <newname>     - Rename file/directory
 *   search <name>   - Search for file by name
 *   stat <path>     - Show file/directory metadata
 *   tree            - Print directory tree
 *   checkpoint      - Save metadata snapshot
 *   crash [power|corrupt <blk>] - Simulate crash
 *   recover         - Recover from crash
 *   backup <tag>    - Create full disk backup
 *   restore <file>  - Restore from backup file
 *   defrag          - Defragment disk
 *   metrics         - Show performance stats
 *   help            - Show this list
 *   exit            - Save and quit
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fs.h"

#define DISK_PATH "c_virtual_disk.bin"
#define MAX_ARGS  16
#define MAX_LINE  1024

/* ── Parse a line into argv-style tokens ─────────────────── */
static int parse_line(char *line, char *argv[], int max_args) {
    int argc = 0;
    char *tok = strtok(line, " \t\r\n");
    while (tok && argc < max_args) {
        argv[argc++] = tok;
        tok = strtok(NULL, " \t\r\n");
    }
    return argc;
}

/* Auto-fix path: add leading '/' if missing */
static char _fixed_path[MAX_PATH_LEN];
static const char *fix_path(const char *p) {
    if (!p || p[0] == '/') return p;
    snprintf(_fixed_path, MAX_PATH_LEN, "/%s", p);
    return _fixed_path;
}

int main(void) {
    printf("\n");
    printf("+==============================================================+\n");
    printf("|       File System Recovery & Optimization Simulator          |\n");
    printf("|                  Interactive CLI  (C version)                |\n");
    printf("+==============================================================+\n");
    printf("Type 'help' for a list of commands.\n\n");

    static FileSystem fs;
    if (fs_init(&fs, DISK_PATH) < 0) {
        fprintf(stderr, "FATAL: cannot open disk file.\n");
        return 1;
    }

    /* Try to mount existing disk */
    if (fs_mount(&fs) < 0) {
        printf("[FS] No existing disk found. Run 'format' to begin.\n");
    }

    char line[MAX_LINE];
    char *argv[MAX_ARGS];

    for (;;) {
        printf("fs> ");
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;

        int argc = parse_line(line, argv, MAX_ARGS);
        if (argc == 0) continue;

        /* ── Commands ───────────────────────────────────── */

        if (strcmp(argv[0], "format") == 0) {
            fs_format(&fs);

        } else if (strcmp(argv[0], "mkdir") == 0) {
            if (argc < 2) { printf("Usage: mkdir <path>\n"); continue; }
            fs_mkdir(&fs, fix_path(argv[1]));

        } else if (strcmp(argv[0], "rmdir") == 0) {
            if (argc < 2) { printf("Usage: rmdir <path>\n"); continue; }
            fs_rmdir(&fs, fix_path(argv[1]));

        } else if (strcmp(argv[0], "ls") == 0) {
            const char *path = (argc >= 2) ? fix_path(argv[1]) : "/";
            DirEntry entries[MAX_ENTRIES_PER_DIR];
            fs_ls(&fs, path, entries, MAX_ENTRIES_PER_DIR);

        } else if (strcmp(argv[0], "create") == 0) {
            if (argc < 2) { printf("Usage: create <path> [seq|dir|idx]\n"); continue; }
            int method = ACCESS_SEQUENTIAL;
            if (argc >= 3) {
                if (strcmp(argv[2], "dir") == 0) method = ACCESS_DIRECT;
                else if (strcmp(argv[2], "idx") == 0) method = ACCESS_INDEXED;
            }
            fs_create_file(&fs, fix_path(argv[1]), method);

        } else if (strcmp(argv[0], "write") == 0) {
            if (argc < 3) { printf("Usage: write <path> <data...>\n"); continue; }
            /* Join remaining args into one string */
            char data[2048] = "";
            for (int i = 2; i < argc; i++) {
                if (i > 2) strcat(data, " ");
                strncat(data, argv[i], sizeof(data) - strlen(data) - 1);
            }
            fs_write_file(&fs, fix_path(argv[1]), (uint8_t *)data, (int)strlen(data));

        } else if (strcmp(argv[0], "read") == 0) {
            if (argc < 2) { printf("Usage: read <path>\n"); continue; }
            uint8_t buf[BLOCK_SIZE * MAX_BLOCK_PTR];
            int n = fs_read_file(&fs, fix_path(argv[1]), buf, sizeof(buf) - 1);
            if (n >= 0) {
                buf[n] = '\0';
                printf("  Content (%d bytes):\n  %s\n", n, (char *)buf);
            }

        } else if (strcmp(argv[0], "delete") == 0) {
            if (argc < 2) { printf("Usage: delete <path>\n"); continue; }
            fs_delete_file(&fs, fix_path(argv[1]));

        } else if (strcmp(argv[0], "rename") == 0) {
            if (argc < 3) { printf("Usage: rename <path> <newname>\n"); continue; }
            fs_rename(&fs, fix_path(argv[1]), argv[2]);

        } else if (strcmp(argv[0], "search") == 0) {
            if (argc < 2) { printf("Usage: search <name>\n"); continue; }
            char results[32][MAX_PATH_LEN];
            int n = fs_search(&fs, argv[1], results, 32);
            if (n == 0) printf("  Not found.\n");
            else for (int i = 0; i < n; i++) printf("  Found: %s\n", results[i]);

        } else if (strcmp(argv[0], "stat") == 0) {
            if (argc < 2) { printf("Usage: stat <path>\n"); continue; }
            Inode node;
            fs_stat(&fs, fix_path(argv[1]), &node);

        } else if (strcmp(argv[0], "tree") == 0) {
            fs_tree(&fs);

        } else if (strcmp(argv[0], "checkpoint") == 0) {
            fs_checkpoint(&fs);

        } else if (strcmp(argv[0], "crash") == 0) {
            const char *kind = (argc >= 2) ? argv[1] : "power";
            int bid = (argc >= 3) ? atoi(argv[2]) : DATA_START;
            fs_simulate_crash(&fs, kind, bid);

        } else if (strcmp(argv[0], "recover") == 0) {
            fs_recover(&fs);

        } else if (strcmp(argv[0], "backup") == 0) {
            const char *tag = (argc >= 2) ? argv[1] : "manual";
            fs_backup(&fs, tag);

        } else if (strcmp(argv[0], "restore") == 0) {
            if (argc < 2) { printf("Usage: restore <backup_filename>\n"); continue; }
            fs_restore(&fs, argv[1]);

        } else if (strcmp(argv[0], "defrag") == 0) {
            fs_defragment(&fs);

        } else if (strcmp(argv[0], "metrics") == 0) {
            fs_print_metrics(&fs);

        } else if (strcmp(argv[0], "help") == 0) {
            printf("\nAvailable commands:\n");
            printf("  format                  - Create fresh file system\n");
            printf("  mkdir  <path>           - Create directory\n");
            printf("  rmdir  <path>           - Remove empty directory\n");
            printf("  ls     [path]           - List directory (default /)\n");
            printf("  create <path> [seq|dir|idx] - Create file\n");
            printf("  write  <path> <data...> - Write text data to file\n");
            printf("  read   <path>           - Read file content\n");
            printf("  delete <path>           - Delete file\n");
            printf("  rename <path> <name>    - Rename file/directory\n");
            printf("  search <name>           - Search for file by name\n");
            printf("  stat   <path>           - Show file metadata\n");
            printf("  tree                    - Print directory tree\n");
            printf("  checkpoint              - Save metadata snapshot\n");
            printf("  crash  [power|corrupt <blk>] - Simulate crash\n");
            printf("  recover                 - Recover from crash\n");
            printf("  backup [tag]            - Create full backup\n");
            printf("  restore <filename>      - Restore from backup\n");
            printf("  defrag                  - Defragment disk\n");
            printf("  metrics                 - Show performance stats\n");
            printf("  exit                    - Save and quit\n\n");

        } else if (strcmp(argv[0], "exit") == 0 || strcmp(argv[0], "quit") == 0) {
            fs_checkpoint(&fs);
            fs_close(&fs);
            printf("Goodbye!\n");
            break;

        } else {
            printf("Unknown command: '%s'. Type 'help' for a list.\n", argv[0]);
        }
    }

    return 0;
}
