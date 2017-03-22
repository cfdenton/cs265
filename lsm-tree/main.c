#include <stdlib.h>
#include <stdio.h>

#include "lsm_tree.h"

#define LSMNAME "LSM_TREE"

int main(void) {
    size_t sizes[4] = {4, 7, 13, 10};
    printf("Initializing an LSM tree... ");
    struct lsm_tree *tree = init(LSMNAME, 4, 0, sizes);
    printf("done.\n");

    printf("Initialized an LSM tree with:\n");
    printf("    name: %s\n", tree->name);
    printf("    nlevels: %d\n", tree->nlevels);
    printf("    nlevels_main: %d\n", tree->nlevels_main);
    printf("    nlevels_disk: %d\n", tree->nlevels_disk);
    printf("    levels: %p\n", tree->levels);

    put(tree, 1, 2);
    put(tree, 10, 3);
    put(tree, 3, 1003);
    put(tree, 6, 255);
    put(tree, 4, 142);
    put(tree, 11, 25);
    put(tree, 17, 14);
    put(tree, 12, 15);
    put(tree, 13, 1);
    put(tree, 12, 2);
    put(tree, 15, 3);
    put(tree, 12, 24);
    put(tree, 18, 4);
    put(tree, 17, 25);
    put(tree, 5, 255);
    put(tree, 2, 255);
    delete(tree, 2);
    delete(tree, 13);
    put(tree, 21, 24);
    put(tree, 22, 21);
    get(tree, 22);
    get(tree, 2);
    range(tree, 1, 27);
    get(tree, 13);
    get(tree, 12);
    get(tree, 10);



    printf("Destroying LSM tree... ");
    destroy(tree);
    printf("done.\n");
}
