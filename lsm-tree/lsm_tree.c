/*
 * This file contains the implementation of the LSM Tree API
 * defined in lsm_tree.h
 *
 * By Carl Denton
 */

#include "lsm_tree.h"

#define BLOOM_NUM 5

/* initialization/cleanup helper functions */
static void main_level_init(struct lsm_tree *tree, size_t *sizes, int levelno);
static void disk_level_init(struct lsm_tree *tree, size_t *sizes, int levelno);
static void level_destroy(struct level *level);

/* main level operations */
static void main_level_insert(struct lsm_tree *tree, struct kv_pair *kv);
static size_t main_level_find(struct level *level, key_t key);
static int main_level_get(struct level *level, key_t key, struct kv_pair *res);
static void main_level_range(struct level *level, key_t bottom, key_t top, struct kv_node **head);

/* disk level operations */
static void disk_level_insert(struct lsm_tree *tree, struct kv_pair *kv);
static size_t disk_level_find(struct level *level, key_t key);
static int disk_level_get(struct level *level, key_t key, struct kv_pair *res);
static void disk_level_range(struct level *level, key_t bottom, key_t top, struct kv_node **head);

/* generic operations */
static void invalidate_kv(struct level *level, size_t pos);

/* migration functions */
static void migrate(struct lsm_tree *tree, int level);
static void read_pair(struct level *level, size_t pos, struct kv_pair *result);
static void write_pair(struct level *level, size_t pos, struct kv_pair *kv);
static int valid_entry(struct level *level, size_t pos);
static void migrate_add_kv_node(struct kv_node **head, struct kv_node **tail, struct kv_pair *kv);

/* range helper functions */
static void range_add_kv_node(struct kv_pair *kv, struct kv_node **head);
static void range_clean_list(struct kv_node **head);

/* printing */
static void print_level(struct level *level);


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
    level->m.arr = (struct kv_pair *) calloc(size, sizeof(struct kv_pair));
    level->bloom = bloom_init(BLOOM_NUM);
};

static void disk_level_init(struct lsm_tree *tree, size_t *sizes, int levelno) {
    struct level *level = tree->levels + levelno;
    size_t size = sizes[levelno];

    level->type = DISK_LEVEL;
    level->size = size;
    level->used = 0;

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

    level->bloom = bloom_init(BLOOM_NUM);
}


/* 
 * add a key-value pair to the LSM tree 
 * TODO: only supports main entry level
 */
int put(struct lsm_tree *tree, key_t key, val_t val) {
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
        assert((tree->levels + i)->type == MAIN_LEVEL || (tree->levels + i)->type == DISK_LEVEL);
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
static int main_level_get(struct level *level, key_t key, struct kv_pair *res) {
    size_t pos = main_level_find(level, key);
    if (level->m.arr[pos].key == key && level->m.arr[pos].valid == KV_VALID) {
        *res = level->m.arr[pos];
        return GET_SUCCESS;
    } 
    return GET_FAIL;
}

static int disk_level_get(struct level *level, key_t key, struct kv_pair *res) {
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

static void main_level_insert(struct lsm_tree *tree, struct kv_pair *kv) {
    struct level *level = tree->levels;
    assert(level->type == MAIN_LEVEL);

    if (level->used == level->size) 
            migrate(tree, 0);

    size_t pos = main_level_find(level, kv->key);

    if ((level->m.arr[pos].key != kv->key && level->m.arr[pos].valid == KV_VALID) 
            || level->m.arr[pos].valid == KV_INVAL) 
    {
        memmove(level->m.arr+pos+1, level->m.arr+pos, (level->size-pos-1)*sizeof(struct kv_pair));
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
        memmove(level->m.arr+pos, level->m.arr+pos+1, (level->size-pos)*sizeof(struct kv_pair));
        invalidate_kv(level, level->size-1);
        level->used--;
    }
}

static void disk_level_insert(struct lsm_tree *tree, struct kv_pair *kv) {
    struct level *level = tree->levels;
    assert(level->type == DISK_LEVEL);
    
    if (level->used == level->size)
        migrate(tree, 0);
    
    size_t pos = disk_level_find(level, kv->key);

    /* read the kv_pair stored at position pos */
    struct kv_pair disk_kv, copy_kv, new_kv;
    fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
    fread(&disk_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);

    if ((disk_kv.key != kv->key && disk_kv.valid == KV_VALID) || disk_kv.valid == KV_INVAL) {
        /* memmove */        
        for (size_t i = 0; i < level->size-pos-1; i++) {
            fread(&copy_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
            fwrite(&copy_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
            fseek(level->d.file_ptr, -(long) sizeof(struct kv_pair), SEEK_CUR);
        }
        /* write the new kv */
        new_kv = *kv;
        new_kv.valid = KV_VALID;
        fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
        fwrite(&new_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
        level->used++;
    } else if (disk_kv.key == kv->key && disk_kv.valid == KV_VALID && kv->op == OP_ADD) {
        /* write the new kv */
        new_kv = *kv;
        new_kv.valid = KV_VALID;
        fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
        fwrite(&new_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    } else if (disk_kv.key == kv->key && disk_kv.valid == KV_VALID && kv->op == OP_DEL) {
        /* memmove */
        for (size_t i = 0; i < level->size-pos-1; i++) {
            fread(&copy_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
            fseek(level->d.file_ptr, -(long) sizeof(struct kv_pair), SEEK_CUR);
            fwrite(&copy_kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
            fseek(level->d.file_ptr, sizeof(struct kv_pair), SEEK_CUR);
        }
        /* write a blank kv at the end */
        invalidate_kv(level, level->size-1);
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


static void main_level_range(struct level *level, key_t bottom, key_t top, struct kv_node **head) {
    for (size_t i = 0; i < level->used; i++) {
        if (level->m.arr[i].key > bottom && level->m.arr[i].key < top) {
            if (level->m.arr[i].valid)
                range_add_kv_node(level->m.arr + i, head);
        }
    }
}

static void disk_level_range(struct level *level, key_t bottom, key_t top, struct kv_node **head) {
    struct kv_pair kv;
    fseek(level->d.file_ptr, 0, SEEK_SET);
    for (size_t i = 0; i < level->used; i++) {
        fread(&kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
        if (kv.key > bottom && kv.key < top) {
            if (kv.valid)
                range_add_kv_node(&kv, head);
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

/* MIGRATION */

// TODO: make sure this is correct
static void migrate(struct lsm_tree *tree, int top) {
    assert(top < tree->nlevels-1);

    // get levels in question 
    struct level *top_level = tree->levels + top;
    struct level *bottom_level = tree->levels + top + 1;

    size_t top_read = 0;
    size_t bottom_read = 0;
    size_t bottom_write = 0;
    struct kv_pair *next_top = (struct kv_pair *) malloc(sizeof(struct kv_pair));
    struct kv_pair *next_bottom = (struct kv_pair *) malloc(sizeof(struct kv_pair));
    struct kv_node *list_head = NULL;
    struct kv_node *next_head = NULL;
    struct kv_node *list_tail = NULL;
    bottom_level->used = 0;

    /* read entries until the top has been exhausted and we have nothing left to write */
    while (top_read < top_level->size || list_head) {

        /* migrate another level if necessary */
        if (bottom_level->used == bottom_level->size) {
            migrate(tree, top+1);
            bottom_read = 0;
            bottom_write = 0;
        } else if (bottom_write == bottom_level->size) {
            /* lowest level of the tree is full, crash */
            // TODO: handle this more gracefully
            assert(0);
        }

        /* try to write something */
        if (!valid_entry(bottom_level, bottom_write) && list_head) {
            /* write a key-value pair */
            write_pair(bottom_level, bottom_write, &(list_head->kv));

            /* update the head */
            next_head = list_head->next_node;
            free(list_head);
            list_head = next_head;
            if (!list_head)
                list_tail = list_head;

            /* update the write position */
            bottom_write++;
            bottom_level->used++;
            if (bottom_read < bottom_write)
                bottom_read = bottom_write;

        } 
        /* we can't write anything, so read something */
        else {
            /* read a pair of the top level */
            if (top_read < top_level->size) {
                read_pair(top_level, top_read, next_top);
            } else
                next_top = NULL;

            /* read a pair of the bottom level */
            if (bottom_read < bottom_level->size) {
                read_pair(bottom_level, bottom_read, next_bottom);
            } else 
                next_bottom = NULL;


            /* process what we just read */

            /* neither of the two options is valid, break */
            if ((!next_top || !next_top->valid) && (!next_bottom || !next_bottom->valid)) {
                break;
            }

            /* only the top is valid, add it to the queue */
            else if (!next_bottom || !next_bottom->valid) {
                migrate_add_kv_node(&list_head, &list_tail, next_top);
                invalidate_kv(top_level, top_read);
                top_read++;
            }

            /* only the bottom is valid, add it to the queue */
            else if (!next_top || !next_top->valid) {
                migrate_add_kv_node(&list_head, &list_tail, next_bottom);
                invalidate_kv(bottom_level, bottom_read);
                bottom_read++;
            } 

            /* the top is better, take that */
            else if (next_top->key < next_bottom->key) {
                migrate_add_kv_node(&list_head, &list_tail, next_top);
                invalidate_kv(top_level, top_read);
                top_read++;
            } 

            /* the bottom is better, take that */
            else if (next_top->key > next_bottom->key) {
                migrate_add_kv_node(&list_head, &list_tail, next_bottom);
                invalidate_kv(bottom_level, bottom_read);
                bottom_read++;
            } 

            /* the keys are equal and the top is an add, so take that */
            else if (next_top->op == OP_ADD || top < tree->nlevels - 2) {
                // take the top and ignore the bottom
                migrate_add_kv_node(&list_head, &list_tail, next_top);
                invalidate_kv(top_level, top_read);
                invalidate_kv(bottom_level, bottom_read);
                top_read++;
                bottom_read++;
            } 

            /* the keys are equal and the top is a delete, so ignore both */
            else if (next_top->op == OP_DEL && top == tree->nlevels - 1) {
                // take neither, invalidate both
                invalidate_kv(top_level, top_read);
                invalidate_kv(bottom_level, bottom_read);
                top_read++;
                bottom_read++;
            } else 
                assert(0);
        }
    }
    /* free our allocations */
    free(next_top);
    free(next_bottom);

    /* top is now empty */
    top_level->used = 0;
    assert(bottom_level->used == bottom_write);
}

/* read a pair at a certain position into a preallocated kv_pair struct */
static void read_pair(struct level *level, size_t pos, struct kv_pair *result) {
    assert(pos < level->size);
    if (level->type == MAIN_LEVEL) {
        *result = level->m.arr[pos];
    } else if (level->type == DISK_LEVEL) {
        fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
        fread(result, sizeof(struct kv_pair), 1, level->d.file_ptr);
    }
}

/* write a pair at a certain position */
static void write_pair(struct level *level, size_t pos, struct kv_pair *kv) {
    assert(pos < level->size);
    if (level->type == MAIN_LEVEL) {
        level->m.arr[pos] = *kv;
        level->m.arr[pos].valid = KV_VALID;
    } else if (level->type == DISK_LEVEL) {
        fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
        fwrite(kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
    }
}

/* check if a given entry is valid */
static int valid_entry(struct level *level, size_t pos) {
    if (level->type == MAIN_LEVEL) {
        return level->m.arr[pos].valid;
    } else if (level->type == DISK_LEVEL) {
        struct kv_pair kv;
        fseek(level->d.file_ptr, pos*sizeof(struct kv_pair), SEEK_SET);
        fread(&kv, sizeof(struct kv_pair), 1, level->d.file_ptr);
        return kv.valid;
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
}

static void invalidate_kv(struct level *level, size_t pos) {
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
