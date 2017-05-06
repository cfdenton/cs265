/*
 * This file contains the implementation of the LSM Tree API
 * defined in lsm_tree.h
 *
 * By Carl Denton
 */

#include "lsm_tree.h"

//#define _USE_BLOOM
#define BLOOM_NUM 2

/* initialization/cleanup helper functions */
static void main_level_init(struct lsm_tree *tree, size_t *sizes, int levelno);
static void disk_level_init(struct lsm_tree *tree, size_t *sizes, int levelno);
static void level_destroy(struct level *level);

/* main level operations */
static void main_level_insert(struct lsm_tree *tree, struct kv_pair *kv);
static size_t main_level_find(struct level *level, key_t key);
static int main_level_get(struct level *level, key_t key, struct kv_pair *res);

/* disk level operations */
static void disk_level_insert(struct lsm_tree *tree, struct kv_pair *kv);
static size_t disk_level_find(struct level *level, key_t key);
static int disk_level_get(struct level *level, key_t key, struct kv_pair *res);

/* printing */
static void print_level(struct level *level);

void *put_thread(void *arg);
void *delete_thread(void *arg);
void *get_thread(void *arg);

/*** INITIALIZATION/CLEANUP ***/

/*
 * init:
 * Initialization for an LSM tree-- returns NULL on failure
 * TODO: Not yet robust against failure
 */
struct lsm_tree *init(const char* name, int total_num, int main_num, 
        size_t *sizes) 
{
    assert(name);

    /* allocate space for the table */
    struct lsm_tree* tree = malloc(sizeof(struct lsm_tree));

    /* copy the name */
    tree->name = malloc(strlen(name) + 1);
    strcpy(tree->name, name);
    
    /* assign level numbers */
    assert(main_num >= 0 && total_num >= main_num);
    int disk_num = total_num - main_num;
    tree->nlevels = total_num;
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
 * destroy:
 * Destroys an LSM tree
 * TODO: Does not yet support disk levels
 * TODO: Not yet robust against failure
 */
int destroy(struct lsm_tree *tree) {
    free(tree->name);
    for (int i = 0; i < tree->nlevels; i++) 
        level_destroy(tree->levels + i);

    free(tree->levels);
    free(tree);
    return 0;
}

static void main_level_init(struct lsm_tree *tree, size_t *sizes, int levelno) {
    struct level *level = tree->levels + levelno;
    size_t size = sizes[levelno];
    level->type = MAIN_LEVEL;
    level->size = size;
    level->used = 0;
#ifdef _USE_BTREE
    level->bt = (struct b_tree *) malloc(sizeof(struct b_tree));
#else
    level->m.arr = (struct kv_pair *) calloc(size, sizeof(struct kv_pair));
#endif

    pthread_mutex_init(&level->mutex, NULL);

#ifdef _USE_BLOOM
    level->bloom = bloom_init(BLOOM_NUM);
#endif
};

static void disk_level_init(struct lsm_tree *tree, size_t *sizes, int levelno) {
    struct level *level = tree->levels + levelno;
    size_t size = sizes[levelno];

    level->type = DISK_LEVEL;
    level->size = size;
    level->used = 0;

    pthread_mutex_init(&level->mutex, NULL);

    size_t buflen = 256;
    assert(buflen > strlen(tree->name) + 10);
    level->d.filename = (char *) malloc(buflen);
    snprintf(level->d.filename, buflen, "%s.level%d.bin", tree->name, levelno);
    level->d.file_ptr = fopen(level->d.filename, "wb+");

    /* expand the file to the requested size */
    struct kv_pair blank_kv;
    blank_kv.key = 0;
    blank_kv.val = 0;
    blank_kv.op = OP_DEL;
    blank_kv.valid = KV_INVAL;
    for (size_t i = 0; i < level->size; i++) 
        fwrite(&blank_kv, sizeof(struct kv_pair), 1, level->d.file_ptr); 

#ifdef _USE_BLOOM
    level->bloom = bloom_init(BLOOM_NUM);
#endif
}

struct arg {
    struct lsm_tree *tree;
    key_t key1;
    key_t key2;
    val_t val;
    char *filename;
};

int put(struct lsm_tree *tree, key_t key, val_t val) {
    struct arg *a = (struct arg *) malloc(sizeof(struct arg));
    a->tree = tree;
    a->key1 = key;
    a->val = val;

    pthread_t tid;
    pthread_create(&tid, NULL, put_thread, (void *) a);
    pthread_join(tid, NULL);
    free(a);

    return 0;
}

/* 
 * add a key-value pair to the LSM tree 
 */
void *put_thread(void *arg) {
    struct lsm_tree *tree = ((struct arg *) arg)->tree;
    key_t key = ((struct arg *) arg)->key1;
    val_t val = ((struct arg *) arg)->val;

    assert(tree->nlevels_main > 0);
    struct kv_pair kv;
    kv.key = key;
    kv.val = val;
    kv.op = OP_ADD;
    kv.valid = KV_VALID;

    int res = 0;

    /* insert the new item into the now free level */
    if (tree->levels[0].type == MAIN_LEVEL)
        main_level_insert(tree, &kv);
    else if (tree->levels[0].type == DISK_LEVEL)
        disk_level_insert(tree, &kv);
    else 
        assert(0);
    (void) res;

    return NULL;
}

int delete(struct lsm_tree *tree, key_t key) {
    struct arg *a = (struct arg *) malloc(sizeof(struct arg));
    a->tree = tree;
    a->key1 = key;

    pthread_t tid;
    pthread_create(&tid, NULL, delete_thread, (void *) a);
    pthread_join(tid, NULL);
    free(a);

    return 0;
}

void *delete_thread(void *arg) {
    struct lsm_tree *tree = ((struct arg *) arg)->tree;
    key_t key = ((struct arg *) arg)->key1;

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
    (void) res;
    return NULL;
}

void get(struct lsm_tree *tree, key_t key) {
    struct arg *a = (struct arg *) malloc(sizeof(struct arg));
    a->tree = tree;
    a->key1 = key;

    pthread_t tid;
    pthread_create(&tid, NULL, get_thread, (void *) a);
    pthread_join(tid, NULL);
    free(a);
}

void *get_thread(void *arg) {
    struct lsm_tree *tree = ((struct arg *) arg)->tree;
    key_t key = ((struct arg *) arg)->key1;

    struct kv_pair *retval = (struct kv_pair *) malloc(sizeof(struct kv_pair));
    int r;
    for (int i = 0; i < tree->nlevels; i++) {
        assert((tree->levels + i)->type == MAIN_LEVEL 
            || (tree->levels + i)->type == DISK_LEVEL);
        if ((tree->levels + i)->type == MAIN_LEVEL) {
            r = main_level_get(tree->levels + i, key, retval);
        } else {
            r = disk_level_get(tree->levels + i, key, retval);
        }

        assert(r == GET_SUCCESS || r == GET_FAIL);
        if (r == GET_SUCCESS) assert(retval->op == OP_ADD || retval->op == OP_DEL);

        if (r == GET_SUCCESS && retval->op == OP_ADD) {
            printf("%d\n", retval->val);
            break;
        } else if (r == GET_SUCCESS && retval->op == OP_DEL) {
            printf("\n");
            break;
        } else if (r == GET_FAIL && i == tree->nlevels-1)
            printf("\n");
            break;
    }
    return NULL;
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

    while (head) {
        printf("%d:%d ", head->kv.key, head->kv.val);
        head = head->next_node;
    }
    printf("\n");
}

void load(struct lsm_tree *tree, const char *filename) {
    FILE *fptr = fopen(filename, "rb");
    key_t key;
    val_t val;
    while (1) {
        fread(&key, sizeof(key_t), 1, fptr);
        fread(&val, sizeof(val_t), 1, fptr);
        put(tree, key, val);
    }
}

void stat(struct lsm_tree *tree) {
    long total = 0;
    for (int i = 0; i < tree->nlevels; i++) {
        total += (tree->levels + i)->used;
    }
    printf("Total Pairs: %ld\n", total);

    int first = 1;
    for (int i = 0; i < tree->nlevels; i++) {
        if ((tree->levels + i)->used > 0 && first) {
            printf("LVL%d: %ld", i+1, (tree->levels+i)->used);
            first = 0;
        } else if ((tree->levels+i)->used > 0 && !first)
            printf(", LVL%d: %ld", i+1, (tree->levels+i)->used);
    }
    printf("\n");
    
    struct kv_pair kv;
    for (int i = 0; i < tree->nlevels; i++) {
        for (size_t j = 0; j < (tree->levels+i)->used; j++) {
            read_pair(tree->levels+i, j, &kv);
            printf("%d:%d:L%d ", kv.key, kv.val, i+1);
        }
        printf("\n");
    }
}


/* MAIN LEVEL OPERATIONS */

/* 
 * find a key pair on a main-memory level. Returns GET_SUCCESS on success
 * and sets res, and otherwise returns GET_FAIL
 */
static int main_level_get(struct level *level, key_t key, struct kv_pair *res) {
#ifdef _USE_BLOOM
    /* check the bloom filter first */
    if (bloom_check(level->bloom, key) == BLOOM_NOTFOUND) {
        return GET_FAIL;
    } 
#endif

    size_t pos = main_level_find(level, key);
    if (level->m.arr[pos].key == key && level->m.arr[pos].valid == KV_VALID) {
        *res = level->m.arr[pos];
        return GET_SUCCESS;
    } 
    return GET_FAIL;
}

/* 
 * find a key pair on a disk level. Returns GET_SUCCESS on success
 * and sets res, and otherwise returns GET_FAIL
 */
static int disk_level_get(struct level *level, key_t key, struct kv_pair *res) {
#ifdef _USE_BLOOM    
    if (bloom_check(level->bloom, key) == BLOOM_NOTFOUND) {
        return GET_FAIL;
    } 
#endif

    size_t pos = disk_level_find(level, key);
    struct kv_pair kv;
    fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
    fread(&kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    if (kv.key == key && kv.valid == KV_VALID) {
        *res = kv;
        return GET_SUCCESS;
    } 
    return GET_FAIL;
}


/* 
 * insert a key-value pair in a main-memory level
 */
static void main_level_insert(struct lsm_tree *tree, struct kv_pair *kv) {
    struct level *level = tree->levels;
    assert(level->type == MAIN_LEVEL);

    /* migrate if necessary */
    if (level->used == level->size) {
        migrate(tree, 0);
    }

    pthread_mutex_lock(&level->mutex);
    /* find the position to insert this key */
    size_t pos = main_level_find(level, kv->key);

    /* case 1: the key does not yet exist in this level */
    if ((level->m.arr[pos].key != kv->key && level->m.arr[pos].valid == KV_VALID) 
            || level->m.arr[pos].valid == KV_INVAL) {

        memmove(level->m.arr+pos+1, level->m.arr+pos, 
            (level->size-pos-1)*sizeof(struct kv_pair));
        level->m.arr[pos] = *kv;
        level->m.arr[pos].valid = KV_VALID;
        level->used++;

    /* case 2: the key exists, so we update it */
    } else if (level->m.arr[pos].key == kv->key && level->m.arr[pos].valid == KV_VALID) {
        level->m.arr[pos] = *kv;
    } 

    /* if this is the last level and we're deleting, get rid of this */
    if (level->m.arr[pos].key == kv->key && level->m.arr[pos].valid == KV_VALID 
            && kv->op == OP_DEL) {
        memmove(level->m.arr+pos, level->m.arr+pos+1, 
            (level->size-pos)*sizeof(struct kv_pair));
        invalidate_kv(level, level->size-1);
        level->used--;
    }

#ifdef _USE_BLOOM
    /* add to the bloom filter */
    bloom_add(level->bloom, kv->key);
#endif
    pthread_mutex_unlock(&level->mutex);
}


/* 
 * this function  may actually work, but there's no reason we should use it,
 * so ignore it for now
 */
static void disk_level_insert(struct lsm_tree *tree, struct kv_pair *kv) {
    (void) tree; (void) kv;
    //struct level *level = tree->levels;
    //assert(level->type == DISK_LEVEL);
    //
    //if (level->used == level->size)
    //    migrate(tree, 0);
    //
    //size_t pos = disk_level_find(level, kv->key);

    ///* read the kv_pair stored at position pos */
    //struct kv_pair disk_kv, copy_kv, new_kv;
    //fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
    //fread(&disk_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);

    //if ((disk_kv.key != kv->key && disk_kv.valid == KV_VALID) || disk_kv.valid == KV_INVAL) {
    //    /* memmove */        
    //    for (size_t i = 0; i < level->size-pos-1; i++) {
    //        fread(&copy_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    //        fwrite(&copy_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    //        fseek(level->d.file_ptr, -(long) sizeof(struct kv_pair), SEEK_CUR);
    //    }
    //    /* write the new kv */
    //    new_kv = *kv;
    //    new_kv.valid = KV_VALID;
    //    fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
    //    fwrite(&new_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    //    level->used++;
    //} else if (disk_kv.key == kv->key && disk_kv.valid == KV_VALID && kv->op == OP_ADD) {
    //    /* write the new kv */
    //    new_kv = *kv;
    //    new_kv.valid = KV_VALID;
    //    fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
    //    fwrite(&new_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    //} else if (disk_kv.key == kv->key && disk_kv.valid == KV_VALID && kv->op == OP_DEL) {
    //    /* memmove */
    //    for (size_t i = 0; i < level->size-pos-1; i++) {
    //        fread(&copy_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    //        fseek(level->d.file_ptr, -(long) sizeof(struct kv_pair), SEEK_CUR);
    //        fwrite(&copy_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    //        fseek(level->d.file_ptr, sizeof(struct kv_pair), SEEK_CUR);
    //    }
    //    /* write a blank kv at the end */
    //    invalidate_kv(level, level->size-1);
    //    level->used--;
    //}
}


/* do binary search on a sorted array in main memory. Assumes 
 * that the level lock is held
 */
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

/* do binary search on a sorted array on disk */
static size_t disk_level_find(struct level *level, key_t key) {
    assert(level->type == DISK_LEVEL);
    size_t bottom = 0;
    size_t top = level->used;
    size_t middle;

    struct kv_pair kv;
    while (top > bottom) {
        middle = (top + bottom)/2;
        fseek(level->d.file_ptr, middle*sizeof(struct kv_pair), SEEK_SET);
        fread(&kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
        if (kv.key < key) 
            bottom = middle+1;
        else if (kv.key > key)
            top = middle;
        else if (kv.key == key)
            return middle;
    }
    return bottom;
}


void read_pair(struct level *level, size_t pos, struct kv_pair *result) {
    assert(pos < level->size);
    if (level->type == MAIN_LEVEL) {
        *result = level->m.arr[pos];
    } else if (level->type == DISK_LEVEL) {
        fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
        fread(result, sizeof(struct kv_pair), 1, level->d.file_ptr);
    }
}


void invalidate_kv(struct level *level, size_t pos) {
    if (level->type == MAIN_LEVEL) {
        level->m.arr[pos].key = 0;
        level->m.arr[pos].val = 0;
        level->m.arr[pos].op = OP_DEL;
        level->m.arr[pos].valid = KV_INVAL;
    } else if (level->type == DISK_LEVEL) {
        struct kv_pair blank;
        blank.key = 0;
        blank.val = 0;
        blank.op = OP_DEL;
        blank.valid = KV_INVAL;
        fseek(level->d.file_ptr, sizeof(struct kv_pair)*pos, SEEK_SET);
        fwrite(&blank, sizeof(struct kv_pair), 1, level->d.file_ptr);
    } else
        assert(0);
}


/* BOOKKEEPING */
static void level_destroy(struct level *level) {
    if (level->type == MAIN_LEVEL)
        free(level->m.arr);
    else if (level->type == DISK_LEVEL) {
        fclose(level->d.file_ptr);
        remove(level->d.filename);
        free(level->d.filename);
    }
#ifdef _USE_BLOOM
    bloom_destroy(level->bloom);
#endif
}


/* PRINTING */
void print_tree(struct lsm_tree *tree) {
    for (int i = 0; i < tree->nlevels; i++) {
        print_level(tree->levels + i);
    }
}

static void print_level(struct level *level) {
    if (level->type == MAIN_LEVEL) {
        printf("main level: ");
        for (size_t i = 0; i < level->size; i++) {
            printf("%d/%d-%d-%d ", level->m.arr[i].key, 
                level->m.arr[i].val, level->m.arr[i].valid, level->m.arr[i].op);
        }
        printf("\n");
    }
    else {
        printf("disk level: ");
        struct kv_pair kv;
        fseek(level->d.file_ptr, 0, SEEK_SET);
        for (size_t i = 0; i < level->size; i++) {
            fread(&kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
            printf("%d/%d-%d-%d ", kv.key, kv.val, kv.valid, kv.op);
        }
        printf("\n");
    }
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
