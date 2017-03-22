/*
 * Header file for LSM Tree implementation
 * CS 265 Systems Project, Spring 2017
 * Carl Denton
 */

#define MAIN_LEVEL 0
#define DISK_LEVEL 1
#define OP_ADD 0
#define OP_DEL 1
#define KV_INVAL 0
#define KV_VALID 1
#define GET_FAIL 0
#define GET_SUCCESS 1


typedef long long key_t;
typedef long long val_t;

struct kv_pair {
    key_t key;
    val_t val;
    int op;
    int valid;
};

struct kv_node {
    struct kv_pair kv;
    struct kv_node *next_node;
};

/* main-memory specific information */
struct main_level {
    /* (sorted) array of key value pairs */
    struct kv_pair *arr;    
};

/* disk specific information */
struct disk_level {
    /* pointer to the file object for this level */
    FILE *file_ptr;
};

struct level {
    int type;
    size_t used;
    size_t size;
 
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
struct lsm_tree* init(const char* name, int main_num, int disk_num, size_t *sizes);
int destroy(struct lsm_tree *);

/* user interface to lsm tree */
int put(struct lsm_tree*, key_t, val_t);
int delete(struct lsm_tree*, key_t);
void get(struct lsm_tree*, key_t);
void range(struct lsm_tree*, key_t, key_t);

void print_tree(struct lsm_tree*);
