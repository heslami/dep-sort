#include <algorithm>
#include <omp.h>
#include "mpi.h"
#include <cstdlib>
#include <cstring>

#include "dep_sort_config.h"
#include "utils.h"


static void SendToComputeNodes(TYPE* buff, int buff_size, TYPE** scratch,
                               TYPE** send_buff, int* split_index,
                               int* send_count, MPI_Comm* send_req,
                               MPI_Comm cn_fs_comm) {
  int t_id = omp_get_thread_num();
  const TYPE entry_per_node = ENTRY_PER_NODE;
  #pragma omp for nowait
  for (int i = 1; i < NUM_COMPUTE_NODE; ++i)
    split_index[i] =
        std::upper_bound(&buff[0], &buff[buff_size], i * entry_per_node) - buff;
  #pragma omp master
  {
    split_index[NUM_COMPUTE_NODE] = buff_size;
    split_index[0] = 0;
  }
  #pragma omp barrier
#pragma omp master
  {
    print_arr(split_index, NUM_COMPUTE_NODE + 1, "FILESERVER, split_index: ");
  }
  #pragma omp for
  for (int i = 0; i < NUM_COMPUTE_NODE; ++i) {
    memcpy(scratch[i], &buff[split_index[i]],
           sizeof(TYPE) * (split_index[i + 1] - split_index[i]));
  }

  #pragma omp master
  { 
  // TODO: add this line instead of right after the Isend
    MPI_Waitall(NUM_COMPUTE_NODE, send_req, MPI_STATUSES_IGNORE);
  }
  #pragma omp barrier
  #pragma omp for
  for (int i = 0; i < NUM_COMPUTE_NODE; ++i) {
    CHECK(split_index[i + 1] - split_index[i] <= COMPUTE_NODE_RECV_BUFF_SIZE);
    encodeArray(scratch[i], split_index[i + 1] - split_index[i], send_buff[i],
                &send_count[i]);
//    DEBUG("FILESERVER, compression ratio = %lf\n",
//          (double)send_count[i] / (split_index[i + 1] - split_index[i]));
  }

  #pragma omp master
  {
    for (int i = 0; i < NUM_COMPUTE_NODE; i++) {
      if (send_count[i] != 0) {
        print_arr(send_buff[i], send_count[i],
                  "FILESERVER: %d sending to comput node %d, %d elements",
                  t_id, i, send_count[i]);
        MPI_Issend(send_buff[i], send_count[i] * sizeof(TYPE), MPI_BYTE, i, 0,
                   cn_fs_comm, &send_req[i]);
      } else {
        send_req[i] = MPI_REQUEST_NULL;
      }
    }
//    MPI_Waitall(NUM_COMPUTE_NODE, send_req, MPI_STATUSES_IGNORE);
  }
}

void doFileServer(MPI_Comm fileserver_comm, MPI_Comm cn_fs_comm) {
  int rank; // rank amongst file servers
  MPI_File fh;
  MPI_Offset file_size;
  uint64_t num_entry;
  MPI_Request read_req;
  MPI_Request send_req[NUM_COMPUTE_NODE];
  for (int i = 0; i < NUM_COMPUTE_NODE; ++i) send_req[i] = MPI_REQUEST_NULL;

  TYPE* read_buff = (TYPE*)malloc(READ_CHUNK_SIZE);
  TYPE* sort_buff = (TYPE*)malloc(READ_CHUNK_SIZE);
  TYPE* merge_buff = (TYPE*)malloc(READ_CHUNK_SIZE);
  TYPE* mem_sort_buff = (TYPE*)malloc(FILE_SERVER_BUFF_SIZE);
  TYPE* mem_merge_buff = (TYPE*)malloc(FILE_SERVER_BUFF_SIZE);
  TYPE** scratch_buff = (TYPE**)malloc(NUM_COMPUTE_NODE * sizeof(TYPE*));
  for (int i = 0; i < NUM_COMPUTE_NODE; ++i)
    scratch_buff[i] = (TYPE*)malloc(COMPUTE_NODE_RECV_BUFF_SIZE);
  TYPE** send_buff = (TYPE**)malloc(NUM_COMPUTE_NODE * sizeof(TYPE*));
  for (int i = 0; i < NUM_COMPUTE_NODE; ++i)
    send_buff[i] = (TYPE*)malloc(COMPUTE_NODE_CRECV_BUFF_SIZE);
  int mem_buff_size = 0;

  MPI_Comm_rank(fileserver_comm, &rank);
  
  // ----- FIRST PHASE - READING THE INPUT -----
  // Open the file in read only mode.
  DEBUG("FILESERVER: before file open, rank = %d\n", rank);
  MPI_ERR(MPI_File_open(fileserver_comm, INPUT_FILE, MPI_MODE_RDONLY,
                        MPI_INFO_NULL, &fh));
  DEBUG("FILESERVER: after file open\n");

  MPI_ERR(MPI_File_get_size(fh, &file_size));

  DEBUG("FILESERVER: after get size\n");
  num_entry = file_size / sizeof(TYPE);
  MPI_Offset start_offset = (rank * num_entry / NUM_FILE_SERVER) * sizeof(TYPE);
  MPI_Offset end_offset =
      ((rank + 1) * num_entry / NUM_FILE_SERVER) * sizeof(TYPE);


  bool data_in_mem = false;
  bool done = false;
  int prev_read_count, read_count;
  int thread_index[NUM_CORE_PER_NODE + 1];
  for (int i = 0; i <= NUM_CORE_PER_NODE; ++i)
    thread_index[i] = i * READ_CHUNK_SIZE / sizeof(TYPE) / NUM_CORE_PER_NODE;
  TYPE* run[NUM_CORE_PER_NODE];
  int run_size[NUM_CORE_PER_NODE];
  int merge_ptr[NUM_CORE_PER_NODE];

  int split_index[NUM_COMPUTE_NODE + 1];
  int send_index[NUM_COMPUTE_NODE];

  #pragma omp parallel num_threads(NUM_CORE_PER_NODE)
  {
    bool first_read = true;
    int t_id = omp_get_thread_num();
//    int t_id = 0;
    while (!done) {
      #pragma omp barrier
      #pragma omp master
      {
        if (start_offset < end_offset) {
          read_count = (start_offset + READ_CHUNK_SIZE <= end_offset)
                           ? READ_CHUNK_SIZE
                           : (end_offset - start_offset);
 
          MPI_ERR(MPI_File_iread_at(fh, start_offset, read_buff, read_count,
                                    MPI_BYTE, &read_req));
          start_offset += read_count;
        } else {
          done = true;
        }
      }
      if (!first_read) {
        // Previously read elements are in the sort_buff. We have to sort them.
        if (prev_read_count != READ_CHUNK_SIZE) {

          // This is the last chunk we read from the file.
          #pragma omp master
          {
            for (int i = 0; i <= NUM_CORE_PER_NODE; ++i)
              thread_index[i] =
                  i * prev_read_count / sizeof(TYPE) / NUM_CORE_PER_NODE;
          }
          #pragma omp barrier
        }
        std::sort(&sort_buff[thread_index[t_id]],
                  &sort_buff[thread_index[t_id + 1]]);

        #pragma omp barrier
        #pragma omp master
        {
          TYPE* merge_target;
          if (!data_in_mem)
            merge_target = mem_sort_buff;
          else
            merge_target = merge_buff;

          for (int i = 0; i < NUM_CORE_PER_NODE; ++i) {
            run[i] = &sort_buff[thread_index[i]];
            run_size[i] = thread_index[i + 1] - thread_index[i];
          }
          MultiWayMerge(run, run_size, merge_ptr, NUM_CORE_PER_NODE,
                        merge_target);

          if (!data_in_mem) {
            data_in_mem = true;
          } else {
            TwoWayMerge(merge_buff, prev_read_count / sizeof(TYPE),
                        mem_sort_buff, mem_buff_size / sizeof(TYPE),
                        mem_merge_buff);
            std::swap(mem_sort_buff, mem_merge_buff);
          }
          mem_buff_size += prev_read_count;
        }
        #pragma omp barrier

        if (mem_buff_size + READ_CHUNK_SIZE > FILE_SERVER_BUFF_SIZE) {
          SendToComputeNodes(mem_sort_buff, mem_buff_size / sizeof(TYPE),
                             scratch_buff, send_buff, split_index, send_index,
                             send_req, cn_fs_comm);
          data_in_mem = false;
          mem_buff_size = 0;
        }
      } else {
        first_read = false;
      }

      #pragma omp master
      {
        if (!done) {
          prev_read_count = read_count;
          MPI_Wait(&read_req, MPI_STATUS_IGNORE);
          std::swap(read_buff, sort_buff);
        }
      }
      #pragma omp barrier
    }

    DEBUG("FILESERVER: %d flushing the memory to network for %lu elements\n",
          t_id, mem_buff_size / sizeof(TYPE));
    // Flushing the in-mem buff
    if (mem_buff_size != 0)
      SendToComputeNodes(mem_sort_buff, mem_buff_size / sizeof(TYPE),
                         scratch_buff, send_buff, split_index, send_index,
                         send_req, cn_fs_comm);
  }
  MPI_Waitall(NUM_COMPUTE_NODE, send_req, MPI_STATUSES_IGNORE);

  MPI_Barrier(fileserver_comm);
  MPI_ERR(MPI_File_close(&fh));

  if (rank == 0) {
    DEBUG("FILESERVER: sending finishers to compute nodes\n");
    // Sending finishers to compute nodes
    for (int i = 0; i < NUM_COMPUTE_NODE; ++i)
      MPI_Send(NULL, 0, MPI_BYTE, i, 0, cn_fs_comm);
  }

  for (int i = 0; i < NUM_COMPUTE_NODE; ++i) {
    free(scratch_buff[i]);
    free(send_buff[i]);
  }
  free(scratch_buff);
  free(send_buff);
  free(mem_merge_buff);
  free(mem_sort_buff);
  free(merge_buff);
  free(sort_buff);
  free(read_buff);

  DEBUG("FILESERVER: starting the write phase\n");

  // ----- SECOND PHASE - WRITING THE OUTPUT -----
  uint64_t num_elems_expected[NUM_CN_PER_FS];
  int compute_rank_offset = rank * NUM_CN_PER_FS;
  uint64_t total_elem_exptected = 0;

  for (int i=0; i<NUM_CN_PER_FS; ++i) {
    MPI_Recv(&num_elems_expected[i], 1, MPI_LONG_LONG, compute_rank_offset + i,
             0, cn_fs_comm, MPI_STATUS_IGNORE);
    DEBUG("FILESERVER: %d: num_elems_expected from %d = %lu\n", rank, i, num_elems_expected[i]);
    total_elem_exptected += num_elems_expected[i];
  }


  uint64_t ex_scan = 0;
  MPI_Exscan(&total_elem_exptected, &ex_scan, 1, MPI_LONG_LONG, MPI_SUM,
             fileserver_comm);
  MPI_Bcast(&total_elem_exptected, 1, MPI_LONG_LONG, NUM_FILE_SERVER - 1, fileserver_comm);

  uint64_t file_offset[NUM_CN_PER_FS] = {0};
  for (int i = 1; i < NUM_CN_PER_FS; ++i)
    file_offset[i] = file_offset[i - 1] + num_elems_expected[i - 1];

  for (int i=0; i<NUM_CN_PER_FS; ++i)
    file_offset[i] = (file_offset[i] + ex_scan) * sizeof(TYPE);

  print_arr(file_offset, NUM_CN_PER_FS, "FILESERVER: file_offset");

  MPI_ERR(MPI_File_open(fileserver_comm, OUTPUT_FILE,
                        MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_INFO_NULL, &fh));

  MPI_ERR(MPI_File_set_size(fh, total_elem_exptected * sizeof(TYPE)));
 
  MPI_Request recv_req;
  MPI_Request write_req = MPI_REQUEST_NULL;
  TYPE* recv_buff =
      (TYPE*)malloc(FILE_SERVER_BUFF_SIZE * WORST_CASE_COMPRESSION_RATIO);
  TYPE* ready_buff =
      (TYPE*)malloc(FILE_SERVER_BUFF_SIZE * WORST_CASE_COMPRESSION_RATIO);
  TYPE* output_buff = (TYPE*)malloc(FILE_SERVER_BUFF_SIZE);

  MPI_Irecv(recv_buff, FILE_SERVER_BUFF_SIZE * WORST_CASE_COMPRESSION_RATIO,
            MPI_BYTE, MPI_ANY_SOURCE, 0, cn_fs_comm, &recv_req);

  int num_done = 0;
  while (num_done < NUM_CN_PER_FS) {
    MPI_Status recv_status;

    DEBUG("FILESERVER: before wait\n");

    MPI_ERR(MPI_Wait(&recv_req, &recv_status));

    DEBUG("FILESERVER: after wait\n");

    std::swap(recv_buff, ready_buff);
    MPI_Irecv(recv_buff, FILE_SERVER_BUFF_SIZE * WORST_CASE_COMPRESSION_RATIO,
              MPI_BYTE, MPI_ANY_SOURCE, 0, cn_fs_comm, &recv_req);
    int msg_size;
    MPI_Get_count(&recv_status, MPI_BYTE, &msg_size);
    if (msg_size == 0) {
      DEBUG("FILESERVER: done receiving from CN %d\n", recv_status.MPI_SOURCE);
      num_done ++;
    } else {
      MPI_Wait(&write_req, MPI_STATUS_IGNORE);
      int output_size;
      decodeArray(ready_buff, msg_size / sizeof(TYPE), output_buff,
                  &output_size);

      DEBUG("FILESERVER, compression rate = %lf\n",
            (double)msg_size / sizeof(TYPE) / output_size);
      print_arr(output_buff, output_size / sizeof(TYPE),
                "FILESERVER: writing phase: recv buffer");
      int src = recv_status.MPI_SOURCE;
      int file_offset_index = src - compute_rank_offset;
      DEBUG("FILESERVER: src = %d, file_offset_index = %d\n", src,
            file_offset_index);
      DEBUG("FILESERVER: writing %lu bytes starting from offset %lu to file\n",
            output_size * sizeof(TYPE), file_offset[file_offset_index]);
      MPI_ERR(MPI_File_iwrite_at(fh, file_offset[file_offset_index],
                                 output_buff, output_size * sizeof(TYPE),
                                 MPI_BYTE
//                                 , MPI_STATUS_IGNORE));
                                 ,&write_req));
      file_offset[file_offset_index] += output_size * sizeof(TYPE);
    }
  }
  print_arr(file_offset, NUM_CN_PER_FS, "FILESERVER: file_offset");

  MPI_Wait(&write_req, MPI_STATUS_IGNORE);

  MPI_Cancel(&recv_req);

  MPI_File_close(&fh);

  free(output_buff);
  free(ready_buff);
  free(recv_buff);
}
