#ifndef _DEP_SORT_CONFIG_H
#define _DEP_SORT_CONFIG_H

#include <climits>
#include <stdint.h>

/* Type of elements to be sorted */
#define TYPE uint32_t
#define TYPE_MIN 0UL
#define TYPE_MAX 0xFFFFFFFFUL
//#define TYPE_MAX 256

/* System parameters*/
#define NUM_COMPUTE_NODE 4
#define ENTRY_PER_NODE 0x3FFFFFFFUL /* Shift right of TYPE_MAX by lg(NUM_COMPUTE_NODE) */
//#define ENTRY_PER_NODE (256/4)


#define NUM_IO_FORWARDER 2
#define NUM_FILE_SERVER 2
#define WORST_CASE_COMPRESSION_RATIO 1

/* Number of cores per node */
#define NUM_CORE_PER_NODE 2
#define DRAM_ALLOCATION 128 * 1024 * 1024

/* File server */
#define INPUT_FILE "../data/unsorted.dat"
#define OUTPUT_FILE "sorted.dat"
#define READ_CHUNK_SIZE (1 * 1024 * 1024)
#define FILE_SERVER_BUFF_SIZE (16 * 1024 * 1024)
#define NUM_CN_PER_FS NUM_COMPUTE_NODE / NUM_FILE_SERVER

/* Compute node */
// Each CN needs two compressed receive buffers --> 2
// Each CN needs two decompressed receive buffers --> 2
// Each CN needs two merge buffers and one more for compression --> 3
#define COMPUTE_NODE_MERGE_BUFF_SIZE (16 * 1024 * 1024)
#define COMPUTE_NODE_CHUNK_SIZE (512 * 1024)



#define COMPUTE_NODE_SEND_BUFF_SIZE FILE_SERVER_BUFF_SIZE
#define COMPUTE_NODE_RECV_BUFF_SIZE \
  ((size_t)(FILE_SERVER_BUFF_SIZE / NUM_COMPUTE_NODE * 1.5))

#define COMPUTE_NODE_CRECV_BUFF_SIZE \
  (size_t) (COMPUTE_NODE_RECV_BUFF_SIZE * WORST_CASE_COMPRESSION_RATIO)
#define CHUNKS_PER_MERGE_BUFFER                                                \
  ((COMPUTE_NODE_MERGE_BUFF_SIZE + COMPUTE_NODE_CHUNK_SIZE - 1) /              \
  COMPUTE_NODE_CHUNK_SIZE)
#define COMPUTE_NODE_CSEND_BUFF_SIZE \
  (size_t) (COMPUTE_NODE_SEND_BUFF_SIZE * WORST_CASE_COMPRESSION_RATIO)
// Assume it divides COMPUTE_NODE_MERGE_BUFF_SIZE

/* IO Forwarder */
#define LOCAL_FILE "data.bin"
#define NUM_CN_PER_IO NUM_COMPUTE_NODE / NUM_IO_FORWARDER


#endif /* _DEP_SORT_CONFIG_H */
