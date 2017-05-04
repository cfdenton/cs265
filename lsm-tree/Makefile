CC=gcc -std=c99
CFLAGS = -ggdb3 -W -Wall -Wextra -Werror -O3
LDFLAGS =
LIBS =

default: main 

%.o: %.c %.h
	$(CC) -c -o $@ $< $(CFLAGS)

main: murmur3.c bloom.c lsm_tree.c main.o 
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f main *.o
