#ifndef _UTILS_H
#define _UTILS_H

#include <cstdlib>
#include <cstdio>
#include <iostream>
#include "mpi.h"

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      std::cerr << "Check `" #condition "` failed in " << __FILE__ << " line " \
                << __LINE__ << std::endl;                                      \
      std::exit(EXIT_FAILURE);                                                 \
    }                                                                          \
  } while (false)

#define MIN(a,b) ((a) < (b) ? (a) : (b))

#ifdef DEBUG_ENABLE
#define DEBUG(X, ...) fprintf(stderr, X, ##__VA_ARGS__)

#define MPI_ERR(X)                                                         \
  do {                                                                     \
    int error_code = X;                                                    \
    if (error_code != MPI_SUCCESS) {                                       \
      char error_string[1000];                                             \
      int length_of_error_string;                                          \
      MPI_Error_string(error_code, error_string, &length_of_error_string); \
      int my_rank;                                                         \
      MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);                             \
      fprintf(stderr, "%3d: %s\n", my_rank, error_string);                 \
      std::cerr << "Failed in " << __FILE__ << " line " << __LINE__        \
                << std::endl;                                              \
      std::exit(EXIT_FAILURE);                                             \
    }                                                                      \
  } while (false)

#else
#define DEBUG(X, ...)
#define MPI_ERR(X) X
#endif

#define show(X) DEBUG(#X" = %u\n", X)
#define pshow(R, X) DEBUG("%d: "#X" = %u\n", R, X)

void print_arr(int* arr, int len, const char* fmt, ...);
void print_arr(TYPE* arr, int len, const char* note, ...);
void print_arr(uint64_t* arr, int len, const char* fmt, ...);


void TwoWayMerge(TYPE* a, int size_a, TYPE* b, int size_b, TYPE* output);
void MultiWayMerge(TYPE** run, int* run_size, int* ptr, int num_runs,
                   TYPE* output);

void encodeArray(TYPE* a, int size_a, TYPE* b, int* size_b);
void decodeArray(TYPE* a, int size_a, TYPE* b, int* size_b);

#endif /* _UTILS_H */
