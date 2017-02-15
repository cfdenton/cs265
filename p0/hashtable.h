/*
 * This include file defines the interface to our hash tables.
 * By Carl Denton
 */

typedef int key_t ;
typedef int val_t;

typedef struct hashtable {
    char* name;
    int nslots;
    struct hashtable_node** nodes;
} hashtable;

struct hashtable_node {
    key_t value;
    struct hashtable_node* next_node;    
    struct hashtable_node* last_node;
};

hashtable* init(const char*, key_t nslots);
int destroy(struct hashtable*);

int put(hashtable*, key_t, val_t);
int get(hashtable*, key_t, val_t*, int);

int erase(hashtable*, key_t);
