/*
 * This file contains the implementation of the hashtable API 
 * defined in htable.h
 */
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include "lsm_tree.h"

static void main_level_init(struct lsm_tree *tree, size_t *sizes, int levelno);
static void disk_level_init(struct lsm_tree *tree, size_t *sizes, int levelno);
static void level_destroy(struct level *level);

static void main_level_insert(struct lsm_tree *tree, struct kv_pair *kv);
static size_t main_level_find(struct level *level, key_t key);
static int main_level_get(struct level *level, key_t key, struct kv_pair *res);
static void main_level_range(struct level *level, key_t bottom, key_t top, struct kv_node **head);


static void disk_level_insert(struct lsm_tree *tree, struct kv_pair *kv);
static int disk_level_get(struct level *level, key_t key, struct kv_pair *res);
static void disk_level_range(struct level *level, key_t bottom, key_t top, struct kv_node **head);

static void range_clean_list(struct kv_node **head);

static void migrate(struct lsm_tree *tree, int level);
static struct kv_pair *read_pair(struct level *level, size_t pos);
static void write_pair(struct level *level, size_t pos, struct kv_pair *kv);
static int valid_entry(struct level *level, size_t pos);
static void migrate_add_kv_node(struct kv_node **head, struct kv_node **tail, struct kv_pair *kv);

static void range_add_kv_node(struct kv_pair *kv, struct kv_node **head);

static void print_level(struct level *level);


/*
 * Initialization for an LSM tree-- returns NULL on failure
 * TODO: Does not yet support disk levels
 * TODO: Not yet robust against failure
 */
struct lsm_tree *init(const char* name, int main_num, int disk_num, 
        size_t *sizes) 
{
    assert(name);

    /* allocate space for the table */
    struct lsm_tree* tree = malloc(sizeof(struct lsm_tree));

    /* copy the name */
    tree->name = malloc(strlen(name) + 1);
    strcpy(tree->name, name);
    
    /* assign level numbers */
    assert(main_num >= 0 && disk_num >= 0);
    tree->nlevels = main_num + disk_num;
    tree->nlevels_main = main_num;
    tree->nlevels_disk = disk_num;
    
    /* allocate space for array of levels */
    tree->levels = (struct level *) malloc(tree->nlevels*sizeof(struct level));
    
    /* initialize levels */
    for (int i = 0; i < tree->nlevels; i++) {
        if (i < main_num)
            main_level_init(tree, sizes, i);
        else
            disk_level_init(tree, sizes, i);
    }

    return tree;
}

/*
 * Destroys an LSM tree
 * TODO: Does not yet support disk levels
 * TODO: Not yet robust against failure
 */
int destroy(struct lsm_tree *tree) {
    free(tree->name);
    for (int i = 0; i < tree->nlevels_main; i++) 
        level_destroy(tree->levels + i);

    free(tree->levels);
    free(tree);
    return 0;
}


/* 
 * add a key-value pair to the LSM tree 
 * TODO: only supports main entry level
 */
int put(struct lsm_tree *tree, key_t key, val_t val) {
    printf("INSERTING %llu:%llu\n", key, val);
    assert(tree->nlevels_main > 0);
    struct kv_pair kv;
    kv.key = key;
    kv.val = val;
    kv.op = OP_ADD;
    kv.valid = KV_VALID;

    int res = 0;

    /* insert the new item into the now free level */
    if (tree->levels->type == MAIN_LEVEL)
        main_level_insert(tree, &kv);
    else if (tree->levels->type == DISK_LEVEL)
        disk_level_insert(tree, &kv);
    else 
        assert(0);

    return res;
}

int delete(struct lsm_tree *tree, key_t key) {
    printf("DELETING %llu\n", key);
    struct kv_pair kv;
    kv.key = key;
    kv.val = 0;
    kv.op = OP_DEL;
    kv.valid = KV_VALID;

    int res = 0;
    if (tree->levels->type == MAIN_LEVEL)
        main_level_insert(tree, &kv);
    else if (tree->levels->type == DISK_LEVEL)
        disk_level_insert(tree, &kv);
    else 
        assert(0);
    return res;
}

void get(struct lsm_tree *tree, key_t key) {
    struct kv_pair *retval = (struct kv_pair *) malloc(sizeof(struct kv_pair));
    int r;
    for (int i = 0; i < tree->nlevels; i++) {
        assert(tree->levels->type == MAIN_LEVEL || tree->levels->type == DISK_LEVEL);
        if (tree->levels->type == MAIN_LEVEL)
            r = main_level_get(tree->levels + i, key, retval);
        else 
            r = disk_level_get(tree->levels + i, key, retval);

        assert(r == GET_SUCCESS || r == GET_FAIL);
        assert(retval->op == OP_ADD || retval->op == OP_DEL);
        if (r == GET_SUCCESS && retval->op == OP_ADD) {
            printf("GET: %lld:%lld\n", retval->key, retval->val);
            break;
        } else if (r == GET_SUCCESS && retval->op == OP_DEL) {
            printf("GET: %lld:\n", key);
            break;
        } else if (r == GET_FAIL && i == tree->nlevels-1)
            printf("GET: %lld:\n", key);
    }
}

void range(struct lsm_tree *tree, key_t bottom, key_t top) {
    struct kv_node *head = NULL;
    for (int i = 0; i < tree->nlevels; i++) {
        assert(tree->levels->type == MAIN_LEVEL || tree->levels->type == DISK_LEVEL);
        if (tree->levels->type == MAIN_LEVEL)
            main_level_range(tree->levels + i, bottom, top, &head);
        else
            disk_level_range(tree->levels + i, bottom, top, &head);
    }

    range_clean_list(&head);

    printf("RANGE: ");
    while (head) {
        printf("%lld:%lld ", head->kv.key, head->kv.val);
        head = head->next_node;
    }
    printf("\n");
}


/* MAIN LEVEL OPERATIONS */
static void main_level_init(struct lsm_tree *tree, size_t *sizes, int levelno) {
    struct level *level = tree->levels + levelno;
    size_t size = sizes[levelno];
    level->type = MAIN_LEVEL;
    level->size = size;
    level->used = 0;
    level->m.arr = (struct kv_pair *) calloc(size, sizeof(struct kv_pair));
};

static void main_level_insert(struct lsm_tree *tree, struct kv_pair *kv) {
    struct level *level = tree->levels;
    assert(level->type == MAIN_LEVEL);

    if (level->used == level->size) 
            migrate(tree, 0);

    size_t pos = main_level_find(level, kv->key);

    if ((level->m.arr[pos].key != kv->key && level->m.arr[pos].valid == KV_VALID) 
            || level->m.arr[pos].valid == KV_INVAL) 
    {
        memcpy(level->m.arr+pos+1, level->m.arr+pos, (level->size-pos-1)*sizeof(struct kv_pair));
        level->m.arr[pos] = *kv;
        level->m.arr[pos].valid = KV_VALID;
        level->used++;
    } else if (level->m.arr[pos].key == kv->key && level->m.arr[pos].valid == KV_VALID
            && kv->op == OP_ADD) 
    {
        level->m.arr[pos] = *kv;
        level->m.arr[pos].valid = KV_VALID;
    } else if (level->m.arr[pos].key == kv->key && level->m.arr[pos].valid == KV_VALID 
            && kv->op == OP_DEL) {
        memcpy(level->m.arr+pos, level->m.arr+pos+1, (level->size-pos)*sizeof(struct kv_pair));
        level->m.arr[level->size-1].key = 0;
        level->m.arr[level->size-1].val = 0;
        level->m.arr[level->size-1].val = KV_INVAL; 
        level->used--;
    }
}

/* do binary search on a sorted array */
static size_t main_level_find(struct level *level, key_t key) {
    assert(level->type == MAIN_LEVEL);
    size_t bottom = 0;
    size_t top = level->used;
    size_t middle;

    while (top > bottom) {
        middle = (top + bottom)/2;
        if (level->m.arr[middle].key < key) 
            bottom = middle+1;
        else if (level->m.arr[middle].key > key)
            top = middle;
        else if (level->m.arr[middle].key == key)
            return middle;
    }
    return bottom;
}

static int main_level_get(struct level *level, key_t key, struct kv_pair *res) {
    size_t pos = main_level_find(level, key);
    if (level->m.arr[pos].key == key && level->m.arr[pos].valid == KV_VALID) {
        *res = level->m.arr[pos];
        return GET_SUCCESS;
    } 
    return GET_FAIL;
}

static void main_level_range(struct level *level, key_t bottom, key_t top, struct kv_node **head) {
    for (size_t i = 0; i < level->used; i++) {
        if (level->m.arr[i].key > bottom && level->m.arr[i].key < top) {
            if (level->m.arr[i].valid)
                range_add_kv_node(level->m.arr + i, head);
        }
    }
}

static void range_add_kv_node(struct kv_pair *kv, struct kv_node **head) {
    int add = 1;
    struct kv_node *last_node = NULL;
    struct kv_node *cur_node = *head;
    while (cur_node) {
        if (cur_node->kv.key == kv->key) 
            add = 0;
        last_node = cur_node;
        cur_node = cur_node->next_node;
    }

    if (add) {
        struct kv_node *new_node = (struct kv_node *) malloc(sizeof(struct kv_node));
        new_node->kv = *kv;
        new_node->next_node = NULL;
        if (*head) 
            last_node->next_node = new_node;
        else
            *head = new_node;
    }
}

static void range_clean_list(struct kv_node **head) {
    struct kv_node *last_node = NULL;
    struct kv_node *cur_node = *head;
    while (cur_node) {
        if (cur_node->kv.op == OP_DEL) {
            if (last_node) 
                last_node->next_node = cur_node->next_node;
            else if (*head == cur_node)
                *head = cur_node->next_node;
            free(cur_node);
            if (last_node)
                cur_node = last_node->next_node;
            else
                cur_node = NULL;
        }
        else {
            last_node = cur_node;
            cur_node = cur_node->next_node;
        }
    }
}


/* DISK LEVEL OPERATIONS */
static void disk_level_init(struct lsm_tree *tree, size_t *sizes, int levelno) {
    printf("initializing disk level\n");
    struct level *level = tree->levels + levelno;
    size_t size = sizes[levelno];

    level->type = DISK_LEVEL;
    level->size = size;
    level->used = 0;

    size_t buflen = 256;
    assert(buflen > strlen(tree->name) + 10);
    char *filename = (char *) malloc(buflen);
    snprintf(filename, buflen, "%s-%d.bin", tree->name, levelno);
    level->d.file_ptr = NULL;
}


static void disk_level_insert(struct lsm_tree *tree, struct kv_pair *kv) {
    (void) tree; (void) kv;
}


static int disk_level_get(struct level *level, key_t key, struct kv_pair *res) {
    (void) level; (void) key; (void) res;
    return 0;
}


static void disk_level_range(struct level *level, key_t bottom, key_t top, struct kv_node **head) {
    (void) level; (void) bottom; (void) top; (void) head;
}

/* MIGRATION */

// TODO: make sure this is correct
static void migrate(struct lsm_tree *tree, int top) {
    //printf("beginning migration\n");
    //print_tree(tree);
    //printf("\n");
    assert(top < tree->nlevels-1);

    // get levels in question 
    struct level *top_level = tree->levels + top;
    struct level *bottom_level = tree->levels + top + 1;

    size_t top_read = 0;
    size_t bottom_read = 0;
    size_t bottom_write = 0;
    struct kv_pair *next_top, *next_bottom;
    struct kv_node *list_head = NULL;
    struct kv_node *next_head = NULL;
    struct kv_node *list_tail = NULL;
    bottom_level->used = 0;

    // read entries until the top has been exhausted and we have nothing left to write
    while (top_read < top_level->size || list_head) {
        // migrate another level if necessary
        if (bottom_level->used == bottom_level->size) {
            // bottom level full, do another migration
            migrate(tree, top+1);
            bottom_read = 0;
            bottom_write = 0;
        } else if (bottom_write == bottom_level->size) {
            // lowest level of the tree is full, crash
            assert(0);
        }

        // try to write something
        if (!valid_entry(bottom_level, bottom_write) && list_head) {
            // write a key-value pair
            write_pair(bottom_level, bottom_write, &(list_head->kv));

            // update the head
            next_head = list_head->next_node;
            free(list_head);
            list_head = next_head;
            if (!list_head)
                list_tail = list_head;

            // update the write position
            bottom_write++;
            bottom_level->used++;
            if (bottom_read < bottom_write)
                bottom_read = bottom_write;
        } 
        // we can't write anything, so read something
        else {
            // read a pair each of top and bottom
            if (top_read < top_level->size)
                next_top = read_pair(top_level, top_read);
            else
                next_top = NULL;

            if (bottom_read < bottom_level->size) 
                next_bottom = read_pair(bottom_level, bottom_read);
            else 
                next_bottom = NULL;

            // neither of the two options is valid, break
            if ((!next_top || !next_top->valid) && (!next_bottom || !next_bottom->valid)) 
                break;
            // only the top is valid, add it to the queue
            else if (!next_bottom || !next_bottom->valid) {
                migrate_add_kv_node(&list_head, &list_tail, next_top);
                top_read++;
            }
            // only the bottom is valid, add it to the queue
            else if (!next_top || !next_top->valid) {
                migrate_add_kv_node(&list_head, &list_tail, next_bottom);
                bottom_read++;
            } 
            // add the better of the next pair to the queue
            else if (next_top->key < next_bottom->key) {
                migrate_add_kv_node(&list_head, &list_tail, next_top);
                top_read++;
            } else if (next_top->key > next_bottom->key) {
                migrate_add_kv_node(&list_head, &list_tail, next_bottom);
                bottom_read++;
            } else if (next_top->op == OP_ADD || top < tree->nlevels - 2) {
                // take the top and ignore the bottom
                migrate_add_kv_node(&list_head, &list_tail, next_top);
                // here we need to invalidate both top and bottom
                // TODO: make this disk-level compatible
                bottom_level->m.arr[bottom_read].key = 0;
                bottom_level->m.arr[bottom_read].val = 0;
                bottom_level->m.arr[bottom_read].valid = KV_INVAL; 
                top_read++;
                bottom_read++;
            } else if (next_top->op == OP_DEL && top == tree->nlevels - 1) {
                // take neither, invalidate both
                // TODO: make this disk-level compatible
                top_level->m.arr[bottom_read].key = 0;
                top_level->m.arr[bottom_read].val = 0;
                top_level->m.arr[bottom_read].valid = KV_INVAL;

                bottom_level->m.arr[bottom_read].key = 0;
                bottom_level->m.arr[bottom_read].val = 0;
                bottom_level->m.arr[bottom_read].valid = KV_INVAL;

                top_read++;
                bottom_read++;
            } else 
                assert(0);
        }
    }

    top_level->used = 0;
    assert(bottom_level->used == bottom_write);
}

static struct kv_pair *read_pair(struct level *level, size_t pos) {
    assert(pos < level->size);
    if (level->type == MAIN_LEVEL) {
        if (level->m.arr[pos].valid == KV_VALID)
            return level->m.arr + pos;
        else
            return NULL;
    }
    return NULL;
}

static void write_pair(struct level *level, size_t pos, struct kv_pair *kv) {
    assert(pos < level->size);
    if (level->type == MAIN_LEVEL) {
        level->m.arr[pos] = *kv;
        level->m.arr[pos].valid = KV_VALID;
    }
}


static int valid_entry(struct level *level, size_t pos) {
    if (level->type == MAIN_LEVEL) {
        return level->m.arr[pos].valid;
    }
    assert(0);
    return 0;
}

static void migrate_add_kv_node(struct kv_node **head, struct kv_node **tail, struct kv_pair *kv) {
    struct kv_node *next_tail;
    next_tail = (struct kv_node *) malloc(sizeof(struct kv_node));
    next_tail->next_node = NULL;
    next_tail->kv = *kv;
    if (*tail)
        (*tail)->next_node = next_tail;
    *tail = next_tail;
    if (!*head)
        *head = *tail;
        
    kv->valid = KV_INVAL;
    kv->key = 0;
    kv->val = 0;
}


/* BOOKKEEPING */
static void level_destroy(struct level *level) {
    if (level->type == MAIN_LEVEL)
        free(level->m.arr);
}


/* PRINTING */
void print_tree(struct lsm_tree *tree) {
    for (int i = 0; i < tree->nlevels; i++) {
        print_level(tree->levels + i);
    }
}

static void print_level(struct level *level) {
    if (level->type == MAIN_LEVEL) {
        printf("main level %zu/%zu: ", level->used, level->size);
        for (size_t i = 0; i < level->size; i++) {
            printf("%llu/%llu-%d-%d ", level->m.arr[i].key, 
                level->m.arr[i].val, level->m.arr[i].valid, level->m.arr[i].op);
        }
        printf("\n");
    }
    else
        printf("disk level: \n");
}

/*
static void print_list(struct kv_node *head) {
    while (head) {
        printf("%llu/%llu-%d-%d ", head->kv.key, head->kv.val, head->kv.valid, head->kv.op);
        head = head->next_node;
    }
    printf("\n");
}
*/
