/*
 * This file contains the implementation of the hashtable API 
 * defined in htable.h
 */

#include "htable.h"

struct htable* htable_init(const char* name, hkey_t nslots) {
    assert(name);
    assert(nslots > 0 && nslots <= MAX_HSIZE);

    // allocate space for the table
    struct htable* table = malloc(sizeof(struct htable));

    // copy the name
    table->name = malloc(strlen(name) + 1);
    strcpy(table->name, name);

    // allocate node slots
    table->nodes = calloc(nslots*sizeof(hnode));
}


int htable_destroy(struct htable* table) {
    free(table->name);
    free(table->nodes);
    free(table);
    return 0;
}
