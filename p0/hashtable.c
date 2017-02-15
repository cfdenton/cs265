/*
 * This file contains the implementation of the hashtable API 
 * defined in htable.h
 */
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include "hashtable.h"

hashtable* init(const char* name, key_t nslots) {
    assert(name);

    // allocate space for the table
    hashtable* table = malloc(sizeof(hashtable));

    // copy the name
    table->name = malloc(strlen(name) + 1);
    strcpy(table->name, name);

    // allocate node slots
    table->nodes = calloc(nslots, sizeof(struct hashtable_node*));

    // set the nslots
    table->nslots = nslots;

    return table;
}

int destroy(hashtable* table) {
    free(table->name);
    free(table->nodes);
    free(table);
    return 0;
}

int hash(hashtable* table, key_t key) {
    return (int) (key % (key_t) table->nslots);
}

int put(hashtable* table, key_t key, val_t val) {
    int pos = hash(table, key);
    
    struct hashtable_node* new_node = (struct hashtable_node*) malloc(sizeof(struct hashtable_node));

    new_node->value = val;
    new_node->next_node = table->nodes[pos];
    new_node->last_node = NULL;
    if (table->nodes[pos]) 
        table->nodes[pos]->last_node = new_node;
    table->nodes[pos] = new_node;
    return 0;
}

int get(hashtable* table, key_t key, val_t* val, int num_val) {
    int pos = hash(table, key);
    struct hashtable_node* current = table->nodes[pos];
    
    int idx = 0;
    while (current && idx < num_val) {
        val[idx++] = current->value;
        current = current->next_node;
    }
    return 0;
}

int erase(hashtable* table, key_t key) {
    int pos = hash(table, key);
    
    struct hashtable_node* current = table->nodes[pos];
    struct hashtable_node* next;
    while (current) {
        next = current->next_node;
        free(current);
        current = next;    
    }
    return 0;
}

