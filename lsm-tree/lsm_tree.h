/*
 * Header file for LSM Tree implementation
 * CS 265 Systems Project, Spring 2017
 * Carl Denton
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <pthread.h>

//#define _USE_BLOOM

#define MAIN_LEVEL 0
#define DISK_LEVEL 1
#define OP_ADD 1
#define OP_DEL 0
#define KV_INVAL 0
#define KV_VALID 1
#define GET_FAIL 0
#define GET_SUCCESS 1
#define BLOOM_NOTFOUND 0
#define BLOOM_FOUND 1

typedef int key_t;
typedef int val_t;

struct kv_pair {
    key_t key;
    val_t val;
    short op;
    short valid;
};

struct kv_node {
    struct kv_pair kv;
    struct kv_node *next_node;
};

/* opaque */
struct bloom; 


/* main-memory specific information */
struct main_level {
    /* (sorted) array of key value pairs */
#ifndef _USE_BTREE
    struct kv_pair *arr;    
#else
    struct b_tree *bt;
#endif
};

/* disk specific information */
struct disk_level {
    /* pointer to the file object for this level */
    FILE *file_ptr;

    /* filename */
    char *filename;
};

struct level {
    int type;
    size_t used;
    size_t size;
    struct bloom *bloom; 
    pthread_mutex_t mutex;

    union {
        struct main_level m;
        struct disk_level d;
    };
};

struct lsm_tree {
    /* Name of this LSM tree instance */
    char* name;

    /* 
     * Number of levels in each memory region
     * ensure that nlevels = nlevels_main + nlevels_disk
     */
    int nlevels;
    int nlevels_main;
    int nlevels_disk;

    /* pointer arrays to main memory and disk structs for each level */
    struct level *levels;
};

/* bookkeeping functions */
struct lsm_tree* init(const char* name, int main_num, int total_num, 
    size_t *sizes);
int destroy(struct lsm_tree *);

/* user interface to lsm tree */
int put(struct lsm_tree*, key_t, val_t);
int delete(struct lsm_tree*, key_t);
void get(struct lsm_tree*, key_t);
void range(struct lsm_tree*, key_t, key_t);
void load(struct lsm_tree *tree, const char *filename);
void stat(struct lsm_tree* tree);

void print_tree(struct lsm_tree*);


/* bloom filter things */
struct bloom *bloom_init(unsigned hashes);
void bloom_destroy(struct bloom* b);

void bloom_add(struct bloom *b, key_t key);
int bloom_check(struct bloom *b, key_t key);
void bloom_clear(struct bloom *b);

/* btree */
struct b_node {
    size_t used;
    int leaf;
    size_t degree;
    struct kv_pair *values;
    struct b_node **children;
};

struct b_tree {
    size_t degree;
    struct b_node *root;
};

struct kv_pair *b_tree_get(struct b_tree *bt, key_t key);
int b_tree_insert(struct b_tree *bt, struct kv_pair *kv);

/* random */
void migrate(struct lsm_tree *tree, int top);
void invalidate_kv(struct level *level, size_t pos);
void read_pair(struct level *level, size_t pos, struct kv_pair *result);
void main_level_range(struct level *level, key_t bottom, key_t top, 
    struct kv_node **head);
void disk_level_range(struct level *level, key_t bottom, key_t top, 
    struct kv_node **head);
void range_clean_list(struct kv_node **head);



void test_bloom();
void test_bloom1();
void test_bloom2();
void test_bloom3();

