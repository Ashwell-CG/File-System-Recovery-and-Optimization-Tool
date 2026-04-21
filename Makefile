CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -std=c11
SRCS    = disk.c bitmap.c inode.c directory.c recovery.c fs.c
OBJS    = $(SRCS:.c=.o)

all: fs_sim fs_test

fs_sim: $(OBJS) main.c
	$(CC) $(CFLAGS) -o fs_sim $(OBJS) main.c

fs_test: $(OBJS) test.c
	$(CC) $(CFLAGS) -o fs_test $(OBJS) test.c

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	del /Q *.o fs_sim.exe fs_test.exe fs_gui c_virtual_disk.bin c_test_disk.bin fs_journal.bin fs_checkpoint.bin 2>NUL || true

run: fs_sim
	./fs_sim

test: fs_test
	./fs_test

.PHONY: all clean run test
