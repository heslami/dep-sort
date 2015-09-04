#include <algorithm>

#include "mpi.h"

#include "dep_sort_config.h"
#include "utils.h"

static void sendToIOForwarder(TYPE *buff, int buff_size, TYPE *cbuff,
                              int dest_rank, int series, MPI_Request SendReq[],
                              MPI_Comm cn_io_comm) {

  int chunks = (buff_size * sizeof(TYPE) + COMPUTE_NODE_CHUNK_SIZE - 1) /
               COMPUTE_NODE_CHUNK_SIZE;
  int ChunkSize[chunks]; // Chunk sizes in # of elements

  #pragma omp parallel for num_threads(NUM_CORE_PER_NODE)
  for (int i = 0; i < chunks; i++) {
    TYPE* cbuff_idx = cbuff + i * ((size_t) (COMPUTE_NODE_CHUNK_SIZE *
                      WORST_CASE_COMPRESSION_RATIO)) / sizeof(TYPE);
    encodeArray(buff + i * COMPUTE_NODE_CHUNK_SIZE / sizeof(TYPE),
                MIN(buff_size - i * COMPUTE_NODE_CHUNK_SIZE / sizeof(TYPE),
                    COMPUTE_NODE_CHUNK_SIZE / sizeof(TYPE)),
                cbuff_idx,
                &ChunkSize[i]);
  }

  int my_rank;
  MPI_Comm_rank(cn_io_comm, &my_rank);

  for (int i = 0; i < chunks; i++) {
    TYPE* cbuff_idx = cbuff + i * ((size_t) (COMPUTE_NODE_CHUNK_SIZE *
                      WORST_CASE_COMPRESSION_RATIO)) / sizeof(TYPE);
    print_arr(cbuff_idx, ChunkSize[i],
        "Compute Node %d, Read Phase: Send chunk %d (%d bytes) of series %d to "
        "IO forwarder %d.",
        my_rank, i, ChunkSize[i] * sizeof(TYPE), series, dest_rank);
    MPI_ERR(MPI_Isend(cbuff_idx, ChunkSize[i] * sizeof(TYPE), MPI_BYTE,
                      dest_rank, 0, cn_io_comm, &SendReq[i]));
  }

  DEBUG("Compute Node %d, Read Phase: Send finisher of series %d to "
        "IO forwarder %d.\n", my_rank, series, dest_rank);
  MPI_ERR(MPI_Isend(NULL, 0, MPI_BYTE, dest_rank, 0, cn_io_comm,
                    &SendReq[chunks]));

}

void doComputeNode(MPI_Comm cn_fs_comm, MPI_Comm cn_io_comm) {
/*  TYPE* buff = (TYPE*) malloc(FILE_SERVER_BUFF_SIZE);
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Status status;
  MPI_Recv(buff, FILE_SERVER_BUFF_SIZE, MPI_BYTE, MPI_ANY_SOURCE, 0, cn_fs_comm, &status);
  int msg_size;
  MPI_Get_count(&status, MPI_BYTE, &msg_size);
  pshow(rank, msg_size / 4);
  print_arr(buff, msg_size/4, "[COMPUTE %d] buffer", rank);
  free(buff);
*/

#if 1
  int rank; // rank amongst compute nodes
  int io_rank; // rank of the corresponding IO Forwarder in cn_io_comm
  int fs_rank; // rank of corresponding FileServer in cn_fs_comm
  int num_series = 0; // number of buffer sends to the
                        // corresponding IO Forwarder
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  MPI_Request recv_req;
  MPI_Status status;
  MPI_Request SendReq[CHUNKS_PER_MERGE_BUFFER + 1];
  for (int i = 0; i < CHUNKS_PER_MERGE_BUFFER + 1; ++i)
    SendReq[i] = MPI_REQUEST_NULL;

  TYPE* compressed_recv_buff = (TYPE*) malloc(COMPUTE_NODE_CRECV_BUFF_SIZE);
  TYPE* compressed_ready_buff = (TYPE*) malloc(COMPUTE_NODE_CRECV_BUFF_SIZE);
  TYPE* ready_buff = (TYPE*) malloc(COMPUTE_NODE_RECV_BUFF_SIZE);
  TYPE* smallcurr_buff = (TYPE*) malloc(COMPUTE_NODE_RECV_BUFF_SIZE);
  TYPE* curr_buff = (TYPE*) malloc(COMPUTE_NODE_MERGE_BUFF_SIZE);
  TYPE* merge_buff = (TYPE*) malloc(COMPUTE_NODE_MERGE_BUFF_SIZE);
  TYPE* compress_buff = (TYPE*) malloc(
    (size_t) (COMPUTE_NODE_MERGE_BUFF_SIZE * WORST_CASE_COMPRESSION_RATIO));
  TYPE* input_buff = curr_buff;

  // Sizes of buffers (in elements)
  int ready_buff_size;
  int smallcurr_buff_size = 0;
  int curr_buff_size = 0;
  int merge_buff_size;
  int input_buff_size = curr_buff_size;
  uint64_t total_num_elements = 0;

  // Sizes of compressed buffers (in bytes)
  int compressed_recv_buff_bytes;
  int compressed_ready_buff_bytes;


  io_rank = NUM_COMPUTE_NODE + rank / (NUM_CN_PER_IO);
  fs_rank = NUM_COMPUTE_NODE + rank / (NUM_CN_PER_FS);

  // ----- FIRST PHASE - Receiving from fileservers -----
  MPI_ERR(MPI_Irecv((void*) compressed_recv_buff,
                    COMPUTE_NODE_CRECV_BUFF_SIZE,
                    MPI_BYTE,
                    MPI_ANY_SOURCE,
                    0,
                    cn_fs_comm,
                    &recv_req));

  while (true) {
    MPI_ERR(MPI_Wait(&recv_req, &status));
    MPI_Get_count(&status, MPI_BYTE, &compressed_recv_buff_bytes);

    // empty message, used to signify end of data
    if (compressed_recv_buff_bytes == 0)
      break;

    std::swap(compressed_recv_buff, compressed_ready_buff);
    compressed_ready_buff_bytes = compressed_recv_buff_bytes;

    MPI_ERR(MPI_Irecv(compressed_recv_buff,
                      COMPUTE_NODE_CRECV_BUFF_SIZE,
                      MPI_BYTE,
                      MPI_ANY_SOURCE,
                      0,
                      cn_fs_comm,
                      &recv_req));

    decodeArray(compressed_ready_buff, compressed_ready_buff_bytes / sizeof(TYPE),
                ready_buff, &ready_buff_size);

    print_arr(ready_buff, ready_buff_size, "Compute Node %d, Read Phase: buffer received from fileserver", rank);

    if (ready_buff_size + input_buff_size > COMPUTE_NODE_MERGE_BUFF_SIZE / sizeof(TYPE)) {
      MPI_Waitall(CHUNKS_PER_MERGE_BUFFER, SendReq, MPI_STATUSES_IGNORE);
      num_series++;
#if 1
    DEBUG(
        "Compute Node %d, Read Phase: Check that merge buffer %d is sorted\n",
        rank, num_series - 1);
    int sorted_flag = 1;
    for (int i = 0; (i < input_buff_size-1) && sorted_flag; i++)
      if (input_buff[i] > input_buff[i+1]) {
        DEBUG("Merge buffer %d not sorted, at location %d\n", num_series - 1, i);
        sorted_flag = 0;
      }
    if (sorted_flag) {
      DEBUG("Merge buffer %d sorted, end of series %d\n",
            num_series - 1 , num_series - 1);
    }
#endif
      sendToIOForwarder(input_buff, input_buff_size, compress_buff, io_rank, num_series - 1,
                        SendReq, cn_io_comm);
      total_num_elements += input_buff_size;
      std::swap(ready_buff, smallcurr_buff);
      smallcurr_buff_size = ready_buff_size;
      input_buff = smallcurr_buff;
      input_buff_size = smallcurr_buff_size;
      DEBUG("Compute Node %d, input_buff_size = %d (beginning of a new series)\n", rank, input_buff_size);
    } else {
      DEBUG(
          "Compute Node %d, input_buff_size = %d , ready_buff_size = %d "
          "(before merge)\n",
          rank, input_buff_size, ready_buff_size);

      TwoWayMerge(ready_buff, ready_buff_size, input_buff, input_buff_size,
                  merge_buff);
      merge_buff_size = ready_buff_size + input_buff_size;
      std::swap(curr_buff, merge_buff);
      curr_buff_size = merge_buff_size;
      input_buff = curr_buff;
      input_buff_size = curr_buff_size;
      DEBUG("Compute Node %d, input_buff_size = %d (after merging)\n", rank, input_buff_size);
    }

  }

  MPI_Waitall(CHUNKS_PER_MERGE_BUFFER, SendReq, MPI_STATUSES_IGNORE);
  // Send the already gathered elements
  DEBUG("Compute Node %d, input_buff_size = %d (remained data)\n", rank, input_buff_size);
  if (input_buff_size > 0) {
    num_series++;
#ifdef DEBUG_ENABLE
    DEBUG("CN %d: flushing memory to IO forwarders\n", rank);
    DEBUG(
        "Compute Node %d, Read Phase: Check if remainder data in merge buffer "
        "%d is sorted\n",
        rank, num_series - 1);
    int sorted_flag = 1;
    for (int i = 0; (i < input_buff_size-1) && sorted_flag; i++)
      if (input_buff[i] > input_buff[i+1]) {
        DEBUG("Merge buffer %d not sorted, at location %d\n", num_series - 1, i);
        sorted_flag = 0;
      }
    if (sorted_flag) {
      DEBUG("Merge buffer %d sorted, end of run %d\n",
            num_series - 1, num_series - 1);
    }
#endif
    sendToIOForwarder(input_buff, input_buff_size, compress_buff, io_rank, num_series - 1,
                      SendReq, cn_io_comm);
    total_num_elements += input_buff_size;
    MPI_Waitall(CHUNKS_PER_MERGE_BUFFER, SendReq, MPI_STATUSES_IGNORE);
  }

  // Send finisher to the corresponding IO Forwarder
  DEBUG("Compute Node %d, Read Phase: Send finisher to IO forwarder %d.\n",
        rank, io_rank);
  MPI_ERR(MPI_Send(NULL, 0, MPI_BYTE, io_rank, 0, cn_io_comm));

  // ---- Free allocated buffers ----
  free(compress_buff);
  free(compressed_recv_buff);
  free(compressed_ready_buff);
  free(ready_buff);
  free(smallcurr_buff);
  free(curr_buff);
  free(merge_buff);

#ifdef DEBUG_ENABLE
    DEBUG("End of compute node %d read phase\n", rank);
#endif

  // ----- SECOND PHASE - Receiving from IO Forwarder -----

  // Notifiying assigned file-server of how many elements to expect
  DEBUG("Compute Node %d, Write Phase: Send number of assigned elements "
        "to fileserver %d.\n",
        rank, fs_rank);
  MPI_ERR(MPI_Send(&total_num_elements, 1, MPI_LONG_LONG, fs_rank, 0,
                   cn_fs_comm));
  // buffers
  TYPE **cchunk_recv_buffs = (TYPE **) malloc(num_series * sizeof(TYPE *));
  for (int i = 0; i < num_series; ++i)
    cchunk_recv_buffs[i] = (TYPE *) malloc(
      (size_t) (COMPUTE_NODE_CHUNK_SIZE * WORST_CASE_COMPRESSION_RATIO));

  TYPE **cchunk_ready_buffs = (TYPE **) malloc(num_series * sizeof(TYPE *));
  for (int i = 0; i < num_series; ++i)
    cchunk_ready_buffs[i] = (TYPE *) malloc(
      (size_t) (COMPUTE_NODE_CHUNK_SIZE * WORST_CASE_COMPRESSION_RATIO));

  TYPE **chunk_buffs = (TYPE **) malloc(num_series * sizeof(TYPE *));
  for (int i = 0; i < num_series; ++i)
    chunk_buffs[i] = (TYPE *) malloc(COMPUTE_NODE_CHUNK_SIZE);

  TYPE *mmerge_buff = (TYPE *) malloc(COMPUTE_NODE_SEND_BUFF_SIZE);
  TYPE *send_buff = (TYPE *) malloc(COMPUTE_NODE_SEND_BUFF_SIZE);
  TYPE *compressed_send_buff = (TYPE *) malloc(COMPUTE_NODE_CSEND_BUFF_SIZE);

  // Sizes of buffers (in elements)
  int *chunk_buff_sizes = (int *) malloc(num_series * sizeof(int));
  int send_buff_size;
  // Size of compressed send buffer (in elements)
  int compressed_send_buff_elements;

  // Sizes of compressed chunk buffers (in bytes)
  int *cchunk_buff_bytes = (int *) malloc(num_series * sizeof(int));

  // flags
  int num_series_done = 0;
  int *chunk_buffs_idx = (int *) malloc(num_series * sizeof(int));
  int *series_done = (int *) malloc(num_series * sizeof(int));

  // MPI request and status objects
  MPI_Request *RecvReq =
    (MPI_Request *) malloc(num_series * sizeof(MPI_Request));
  MPI_Status *RecvStatus =
    (MPI_Status *) malloc(num_series * sizeof(MPI_Status));
  MPI_Request send_req = MPI_REQUEST_NULL;

  for (int i = 0; i < num_series; ++i)
    MPI_ERR(MPI_Irecv(cchunk_ready_buffs[i],
                      (size_t) (COMPUTE_NODE_CHUNK_SIZE *
                                WORST_CASE_COMPRESSION_RATIO),
                      MPI_BYTE,
                      io_rank,
                      i,
                      cn_io_comm,
                      &RecvReq[i]));
  DEBUG("Compute Node %d, Write Phase: before wait\n", rank);

  MPI_Waitall(num_series, RecvReq, RecvStatus);

  DEBUG("Compute Node %d, Write Phase: after wait\n", rank);

  for (int i = 0; i < num_series; ++i)
    MPI_ERR(MPI_Irecv(cchunk_recv_buffs[i],
                      (size_t) (COMPUTE_NODE_CHUNK_SIZE *
                                WORST_CASE_COMPRESSION_RATIO),
                      MPI_BYTE,
                      io_rank,
                      i,
                      cn_io_comm,
                      &RecvReq[i]));

  // FIXME: maybe use openmp parallelism
  for (int i = 0; i < num_series; ++i) {
    MPI_Get_count(&RecvStatus[i], MPI_BYTE, &cchunk_buff_bytes[i]);
    decodeArray(cchunk_ready_buffs[i], cchunk_buff_bytes[i] / sizeof(TYPE),
                chunk_buffs[i], &chunk_buff_sizes[i]);

    print_arr(chunk_buffs[i], chunk_buff_sizes[i],
        "Compute Node %d, Write Phase: Received chunk 0 (%d bytes) of series %d "
        "from IO forwarder %d.",
        rank, cchunk_buff_bytes[i], i, io_rank);

    chunk_buffs_idx[i] = 0;
    series_done[i] = 0;
  }

  while (num_series_done < num_series) {

    int mmerge_i;
    for (mmerge_i = 0; mmerge_i < COMPUTE_NODE_SEND_BUFF_SIZE / sizeof(TYPE); ++mmerge_i) {
      TYPE min_val = TYPE_MAX;
      int min_series = -1;
      for (int i = 0; i < num_series; ++i) {

        if (!series_done[i]) {

          if (chunk_buffs_idx[i] < chunk_buff_sizes[i]) {

            if (chunk_buffs[i][chunk_buffs_idx[i]] < min_val) {
              min_val = chunk_buffs[i][chunk_buffs_idx[i]];
              min_series = i;
            }
          } else {
            DEBUG("Compute Node %d, OKAY_third_if\n", rank);

            DEBUG("Compute Node %d, OKAY_waiting\n", rank);

            MPI_ERR(MPI_Wait(&RecvReq[i], &RecvStatus[i]));
            DEBUG("Compute Node %d, OKAY_done_waiting\n", rank);
            MPI_Get_count(&RecvStatus[i], MPI_BYTE,
                          &cchunk_buff_bytes[i]);
            DEBUG(
                "Compute Node %d, Write Phase: Series: %d, Bytes of next msg: "
                "%d.\n",
                rank, i, cchunk_buff_bytes[i]);
            if (cchunk_buff_bytes[i] == 0) {

              series_done[i] = 1;
              num_series_done++;

              DEBUG(
                  "Compute Node %d, Write Phase: Received finisher "
                  "of series %d "
                  "from IO forwarder %d.\n",
                  rank, i, io_rank);

            } else {

              DEBUG("Compute Node %d, Write Phase: Sending request for next "
                    "chunk of series %d.\n", rank, i);

              MPI_ERR(MPI_Send(&i, 1, MPI_INT, io_rank, 0, cn_io_comm));
              std::swap(cchunk_recv_buffs[i], cchunk_ready_buffs[i]);
              
              MPI_ERR(MPI_Irecv(cchunk_recv_buffs[i],
                                (size_t) (COMPUTE_NODE_CHUNK_SIZE *
                                          WORST_CASE_COMPRESSION_RATIO),
                                MPI_BYTE,
                                io_rank,
                                i,
                                cn_io_comm,
                                &RecvReq[i]));
              decodeArray(cchunk_ready_buffs[i],
                          cchunk_buff_bytes[i] / sizeof(TYPE),
                          chunk_buffs[i], &chunk_buff_sizes[i]);

              print_arr(chunk_buffs[i], chunk_buff_sizes[i],
                  "Compute Node %d, Write Phase: Received next chunk (%d "
                  "bytes) "
                  "of series %d "
                  "from IO forwarder %d.",
                  rank, cchunk_buff_bytes[i], i, io_rank);

              chunk_buffs_idx[i] = 0;
              if (chunk_buffs[i][0] < min_val) {

                min_val = chunk_buffs[i][0];
                min_series = i;
              }
            }
          }
        }
      }
      if (num_series_done == num_series) break;
      CHECK(min_series != -1);
      DEBUG(
          "Compute Node %d, Write Phase: Adding element (series:%d, chunk_idx:%d) to the mmerge buffer "
          "at index %d.\n",
          rank, min_series, chunk_buffs_idx[min_series], mmerge_i);
      mmerge_buff[mmerge_i] = min_val;
      chunk_buffs_idx[min_series]++;
    }
    DEBUG("Compute Node %d, Write Phase: Merge buffer is full.\n", rank);

    send_buff_size = mmerge_i;
    if (send_buff_size == 0) break;

    MPI_Wait(&send_req, MPI_STATUS_IGNORE); 
    std::swap(mmerge_buff, send_buff);

    print_arr(send_buff, send_buff_size,
              "Compute Node %d, Write Phase: Sending sorted buffer "
              "to FileServer %d.",
              rank, fs_rank);

    encodeArray(send_buff, send_buff_size,
                compressed_send_buff, &compressed_send_buff_elements);
    DEBUG("Compute Node %d, Write Phase: Send new send buffer "
          "to fileserver %d.\n",
          rank, io_rank);
    MPI_ERR(MPI_Isend(compressed_send_buff,
                      compressed_send_buff_elements * sizeof(TYPE),
                      MPI_BYTE,
                      fs_rank,
                      0,
                      cn_fs_comm,
                      &send_req));
  }

  MPI_Wait(&send_req, MPI_STATUS_IGNORE);
  DEBUG(
      "Compute Node %d, Write Phase: Send of last send buffer to fileserver %d "
      "is complete.\n",
      rank, fs_rank);

  // Send finisher to the corresponding FileServer
  // FIXME: Maybe do this with Isend
  DEBUG("Compute Node %d, Write Phase: Send finisher to fileserver %d.\n",
        rank, fs_rank);
  MPI_ERR(MPI_Send(NULL, 0, MPI_BYTE, fs_rank, 0, cn_fs_comm));

  // ---- Free allocated buffers ----
  for (int i = 0; i < num_series; ++i)
    free(cchunk_recv_buffs[i]);
  free(cchunk_recv_buffs);
  for (int i = 0; i < num_series; ++i)
    free(cchunk_ready_buffs[i]);
  free(cchunk_ready_buffs);
  for (int i = 0; i < num_series; ++i)
    free(chunk_buffs[i]);
  free(chunk_buffs);

  free(mmerge_buff);
  free(send_buff);
  free(compressed_send_buff);

  free(cchunk_buff_bytes);
  free(chunk_buff_sizes);

  free(chunk_buffs_idx);
  free(series_done);

  free(RecvReq);
  free(RecvStatus);

  DEBUG("Compute Node %d, DONE YIHAAAAAAAAAAA\n", rank);
#endif
}
