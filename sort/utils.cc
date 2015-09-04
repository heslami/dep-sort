#include <cstring>
#include <cstdarg>
#include "dep_sort_config.h"
#include "utils.h"

#include "codecfactory.h"

using namespace std;

void TwoWayMerge(TYPE* a, int size_a, TYPE* b, int size_b, TYPE* output) {
  int a_ptr = 0, b_ptr = 0;
  int output_index = 0;
  for (; a_ptr < size_a && b_ptr < size_b; output_index++) {
    if (a[a_ptr] > b[b_ptr])
      output[output_index] = b[b_ptr++];
    else
      output[output_index] = a[a_ptr++];
  }
  if (a_ptr < size_a)
    memcpy(&output[output_index], &a[a_ptr], (size_a - a_ptr) * sizeof(TYPE));
  if (b_ptr < size_b)
    memcpy(&output[output_index], &b[b_ptr], (size_b - b_ptr) * sizeof(TYPE));
}

// Utility function to merge sorted runs of data into a bigger output array
// run[i] is a sorted run with size run_size[i]. 'ptr' is a helper ptr array to
// avoid 'malloc' and 'free' of this array everytime this function is called.
void MultiWayMerge(TYPE** run, int* run_size, int* ptr, int num_runs,
                   TYPE* output) {
  memset(ptr, 0, sizeof(int) * num_runs);
  int num_done = 0;
  for (int i = 0; i < num_runs; ++i)
    if (run_size[i] == 0) num_done++;

  for (int output_index = 0; num_done < num_runs; output_index++) {
    TYPE min_val = TYPE_MAX;
    int run_index = -1;
    for (int i = 0; i < num_runs; ++i) {
      if (ptr[i] < run_size[i] && run[i][ptr[i]] < min_val) {
        min_val = run[i][ptr[i]];
        run_index = i;
      }
    }
    output[output_index] = min_val;
    ptr[run_index]++;
    if (ptr[run_index] == run_size[run_index]) num_done++;
  }
}

void encodeArray(TYPE* a, int size_a, TYPE* b, int* size_b) {
  size_t new_len;
  SIMDCompressionLib::CODECFactory::getFromName("s4-bp128-dm")
      ->encodeArray(a, size_a, b, new_len);
  *size_b = (int) new_len;
/*  *size_b = size_a;
  memcpy(b, a, size_a * sizeof(TYPE));*/
}

void decodeArray(TYPE* a, int size_a, TYPE* b, int* size_b) {
  size_t new_len;
  SIMDCompressionLib::CODECFactory::getFromName("s4-bp128-dm")
      ->decodeArray(a, size_a, b, new_len);
  *size_b = (int) new_len;
/*  *size_b = size_a;
  memcpy(b, a, size_a * sizeof(TYPE));*/
}

void print_arr(int* arr, int len, const char* fmt, ...) {
#ifdef DEBUG_ENABLE
  int my_rank;
  va_list args;
  va_start(args, fmt);

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  char str[20000];
  snprintf(str, 20000, "%d: ", my_rank);
  vsnprintf(str + strlen(str), 20000, fmt, args);
  va_end(args);
  snprintf(str + strlen(str), 20000, ": ");
  for (int i = 0; i < len; ++i) snprintf(str + strlen(str), 20000, "%d ", arr[i]);
  DEBUG("%s\n", str);
#endif
}

void print_arr(TYPE* arr, int len, const char* fmt, ...) {
#ifdef DEBUG_ENABLE
  int my_rank;
  va_list args;
  va_start(args, fmt);

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  char str[20000];
  snprintf(str, 20000, "%d: ", my_rank);
  vsnprintf(str + strlen(str), 20000, fmt, args);
  va_end(args);
  snprintf(str + strlen(str), 20000, ": ");
  for (int i = 0; i < len; ++i) snprintf(str + strlen(str), 20000, "%u ", arr[i]);
  DEBUG("%s\n", str);
#endif
}

void print_arr(uint64_t* arr, int len, const char* fmt, ...) {
#ifdef DEBUG_ENABLE
  int my_rank;
  va_list args;
  va_start(args, fmt);

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  char str[20000];
  snprintf(str, 20000, "%d: ", my_rank);
  vsnprintf(str + strlen(str), 20000, fmt, args);
  va_end(args);
  snprintf(str + strlen(str), 20000, ": ");
  for (int i = 0; i < len; ++i) snprintf(str + strlen(str), 20000, "%lu ", arr[i]);
  DEBUG("%s\n", str);
#endif
}
