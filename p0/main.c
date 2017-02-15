#include <stdlib.h>
#include <stdio.h>

#include "hashtable.h"

#define HTNAME "name"
#define HTSIZE 13

// This is where you can implement your own tests for the hash table
// implementation. 
int main(void) {
  hashtable *ht = NULL;
  ht = init(HTNAME, HTSIZE);

  int key = 0;
  int value = -1;

  put(ht, key, value);

  int num_values = 1;

  val_t* values = malloc(1 * sizeof(val_t));

  int num_results = get(ht, key, values, num_values);
  if (num_results > num_values) {
    values = realloc(values, num_results * sizeof(val_t));
    get(ht, 0, values, num_results);
  }

  for (int i = 0; i < num_results; i++) {
    printf("value %d is %d \n", i, values[i]);
  }
  free(values);

  erase(ht, 0);

  destroy(ht);
  return 0;
}
