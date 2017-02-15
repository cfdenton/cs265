#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>

#include "hashtable.h"

#define HTNAME "name"
#define HTSIZE 13

// This code is designed to stress test your hash table implementation. You do
// not need to significantly change it, but you may want to vary the value of
// num_tests to control the amount of time and memory that benchmarking takes
// up. Compile and run it in the command line by typing:
// make benchmark; ./benchmark

int main(void) {
  hashtable* ht=NULL;
  ht = init(HTNAME, HTSIZE);

  int seed = 2;
  srand(seed);
  int num_tests = 50000000;
  printf("Performing stress test. Inserting 50 million keys.\n");

  struct timeval stop, start;
  gettimeofday(&start, NULL);

  for (int i = 0; i < num_tests; i += 1) {
    int key = rand();
    int val = rand();
    put(ht, key, val);
  }

  destroy(ht);
  gettimeofday(&stop, NULL);
  double secs = (double)(stop.tv_usec - start.tv_usec) / 1000000 + (double)(stop.tv_sec - start.tv_sec); 
  printf("50 million insertions took %f seconds\n", secs);

  return 0;
}
