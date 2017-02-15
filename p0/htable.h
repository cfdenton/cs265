/*
 * This include file defines the interface to our hash tables.
 * By Carl Denton
 */

#define MAX_HSIZE 4096


typedef hkey_t unsigned long long;
typedef hval_t int;

struct htable {
    char* name;
    unsigned long long nslots;
    struct hnode* nodes;
};

struct htable* htable_init(const char*, hkey_t);
int htable_destroy(struct htable*);

int put(struct htable*, hkey_t, hval_t);
int get(struct htable*, hkey_t, hval_t*);
