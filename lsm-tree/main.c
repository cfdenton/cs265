#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <argp.h>
#include <sys/time.h>
#include "lsm_tree.h"

#define MAXLINE 256

#define PUT_OP 0
#define GET_OP 1
#define RANGE_OP 2
#define DELETE_OP 3
#define LOAD_OP 4
#define STAT_OP 5
#define QUIT_OP 6

#define MAX_LAYERS 4
#define DEFAULT_NAME "my-lsm"
#define DEFAULT_LAYERS 2
#define DEFAULT_MAIN 1
#define DEFAULT_SIZE0 8192 
#define DEFAULT_SIZE1 1048576

#define DEFAULT_SIZE2 16384
#define DEFAULT_SIZE3 65536 

char *get_input();
int process_input(struct lsm_tree *tree, char *input);
void workload(struct lsm_tree *tree, char *filename);
void quit();

int main(int argc, char *argv[]) {
    (void) argv;
    size_t *sizes;
    int num, num_main;
    if (argc == 1) { /* default values */
        sizes = (size_t *) malloc(MAX_LAYERS*sizeof(size_t));
        sizes[0] = DEFAULT_SIZE0;
        sizes[1] = DEFAULT_SIZE1;
        sizes[2] = DEFAULT_SIZE2;
        sizes[3] = DEFAULT_SIZE3;
        num = DEFAULT_LAYERS;
        num_main = DEFAULT_MAIN;
    } else if (argc == 2) {
        sizes = (size_t *) malloc(DEFAULT_LAYERS*sizeof(size_t));
        sizes[0] = DEFAULT_SIZE0;
        sizes[1] = DEFAULT_SIZE1;
        sizes[2] = DEFAULT_SIZE2;
        num = DEFAULT_LAYERS;
        num_main = DEFAULT_MAIN;
    } else {
        printf("Invalid input\n");
        exit(0);
    }

    struct timeval tval_before, tval_after, tval_result;
    printf("Initializing LSM Tree... ");
    fflush(stdout);
    gettimeofday(&tval_before, NULL);

    struct lsm_tree *tree = init(DEFAULT_NAME, num, num_main, sizes);

    gettimeofday(&tval_after, NULL);
    timersub(&tval_after, &tval_before, &tval_result);
    printf("done.\n");
    printf("Time elapsed: %ld.%06ld\n", (long) tval_result.tv_sec, 
        (long) tval_result.tv_usec);

    if (argc == 1) {
        char *input;
        while (1) {
            input = get_input();

            gettimeofday(&tval_before, NULL);
            process_input(tree, input);
            free(input);

            gettimeofday(&tval_after, NULL);
            timersub(&tval_after, &tval_before, &tval_result);
            printf("Time elapsed: %ld.%06ld\n", (long) tval_result.tv_sec, 
                (long) tval_result.tv_usec);
        }
    } else if (argc == 2) {
        gettimeofday(&tval_before, NULL);

        workload(tree, argv[1]);

        gettimeofday(&tval_after, NULL);
        timersub(&tval_after, &tval_before, &tval_result);
        printf("Time elapsed: %ld.%06ld\n", (long) tval_result.tv_sec, 
            (long) tval_result.tv_usec);
        quit(tree);
    }
    return 0;
}

char *get_input() {
    int max = 20;
    char* name = (char*) malloc(max); /* allocate buffer */
    if (!name) 
        quit();

    printf("input: ");

    /* skip leading whitespace */
    while (1) { 
        int c = getchar();
        if (c == EOF) break; /* end of file */
        if (!isspace(c)) {
             ungetc(c, stdin);
             break;
        }
    }

    int i = 0;
    while (1) {
        int c = getchar();
        if (c == '\n' || c == EOF) { /* at end, add terminating zero */
            name[i] = 0;
            break;
        }
        name[i] = c;
        if (i == max - 1) { /* buffer full */
            max += max;
            name = (char*) realloc(name, max); /* get a new and larger buffer */
            if (!name) 
                quit();
        }
        i++;
    }
    return name;
}

int process_input(struct lsm_tree *tree, char *input) {
    int args;
    char *token;
    int op;

    const char *s = " ";
    /* first token */
    token = strtok(input, s);
    if (!strcmp(token, "p")) {
        op = PUT_OP;
        args = 2;
    } else if (!strcmp(token, "g")) {
        op = GET_OP;
        args = 1;
    } else if (!strcmp(token, "r")) {
        op = RANGE_OP;
        args = 2;
    } else if (!strcmp(token, "d")) {
        op = DELETE_OP;
        args = 1;
    } else if (!strcmp(token, "l")) {
        op = LOAD_OP;
        args = 1;
    } else if (!strcmp(token, "s")) {
        op = STAT_OP;
        args = 0;
    } else if (!strcmp(token, "q")) {
        quit(tree);
        return 1;
    } else {
        printf("Invalid input, try again.\n");
        free(input);
        return 1;
    }

    char **argv = (char **) malloc(args*sizeof(char **));
    for (int i = 0; i < args; i++) {
        argv[i] = strdup(strtok(NULL, s));
    }

    switch (op) {
        case PUT_OP:
            put(tree, atoi(argv[0]), atoi(argv[1]));
            break;
        case GET_OP:
            get(tree, atoi(argv[0]));
            break;
        case RANGE_OP:
            range(tree, atoi(argv[0]), atoi(argv[1]));
            break;
        case DELETE_OP:
            delete(tree, atoi(argv[0]));
            break;
        case LOAD_OP:
            load(tree, argv[0]);
            break;
        case STAT_OP:
            stat(tree);
            break;
        case QUIT_OP:
            quit(tree);
            break;
    }
    return 0;
}

void workload(struct lsm_tree *tree, char *filename) {
    FILE *fptr = fopen(filename, "r");
    char *buf = malloc(MAXLINE);
    while (fgets(buf, MAXLINE, fptr)) {
        process_input(tree, buf);
    }
}

void quit(struct lsm_tree *tree) {
    struct timeval tval_before, tval_after, tval_result;
    printf("Destroying LSM tree... ");
    gettimeofday(&tval_before, NULL);

    destroy(tree);

    gettimeofday(&tval_after, NULL);
    timersub(&tval_after, &tval_before, &tval_result);
    printf("done.\n");

    printf("Time elapsed: %ld.%06ld\n", (long) tval_result.tv_sec, 
        (long) tval_result.tv_usec);
    exit(0);
}

