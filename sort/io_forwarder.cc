#include <vector>
#include <utility>

#include "mpi.h"

#include "dep_sort_config.h"
#include "utils.h"

static void writeToFile(char *buff, size_t count, FILE *file) {
  size_t written = 0;
  while (written < count) {
    size_t more = fwrite(buff + written, 1, count - written, file);
    written += more;
  }
}

static void readFromFile(char *buff, long offset, size_t count, FILE *file) {
  fseek(file, offset, SEEK_SET);

  size_t read = 0;
  while (read < count) {
    size_t more = fread(buff + read, 1, count - read, file);
    read += more;
  }
}

void doIOForwarder(MPI_Comm cn_io_comm) {
  int rank;
  int erank;
  int first_cn_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  erank = rank - NUM_COMPUTE_NODE;
  first_cn_rank = erank * NUM_CN_PER_IO;

  char local_file_name[200];
  snprintf(local_file_name, 200, "%s.%d", LOCAL_FILE, erank);
  // file_index:
  //   1st dim -> cn rank (relative)
  //   2nd dim -> series
  //   3rd dim -> chunk
  //   stores pair of (size, offset)
  std::vector< std::vector<std::pair<int, long> > > file_index[NUM_CN_PER_IO];
  FILE* file = fopen(local_file_name, "wb");
  long file_offset = 0L;

  // ----- FIRST PHASE - Receiving from compute nodes -----

  DEBUG("IOForwarder %d, Read Phase: Starting read phase,"
        "wating for chunks to commit...\n", erank);

  int max_msg_bytes = COMPUTE_NODE_CHUNK_SIZE * WORST_CASE_COMPRESSION_RATIO;
  /* Buffer for receives and file writing */
  TYPE* buff = (TYPE *) malloc((NUM_CN_PER_IO + 1) * max_msg_bytes);
  TYPE* recv_buff[NUM_CN_PER_IO];
  for (int i = 0; i < NUM_CN_PER_IO; i++)
    recv_buff[i] = buff + i * (max_msg_bytes / sizeof(TYPE));

  TYPE* fw_buff = buff + NUM_CN_PER_IO * (max_msg_bytes / sizeof(TYPE));
  int expected_chunk[NUM_CN_PER_IO];
  for (int i = 0; i < NUM_CN_PER_IO; i++)
    expected_chunk[i] = 0;
  int end_series_flag[NUM_CN_PER_IO];
  for (int i = 0; i < NUM_CN_PER_IO; i++)
    end_series_flag[i] = 0;

  MPI_Request RecvReq[NUM_CN_PER_IO];
  MPI_Status status;
  int done_rank;
  int recv_index;
  int recv_buff_bytes;
  int flag = 0;

  for (int i = 0; i < NUM_CN_PER_IO; i++)
    MPI_ERR(MPI_Irecv(recv_buff[i],
                      max_msg_bytes,
                      MPI_BYTE,
                      first_cn_rank + i,
                      0,
                      cn_io_comm,
                      &RecvReq[i]));
  while (true) {
    MPI_Waitany(NUM_CN_PER_IO, RecvReq, &recv_index, &status);
    done_rank = recv_index + first_cn_rank;
    MPI_Get_count(&status, MPI_BYTE, &recv_buff_bytes);

    if (recv_buff_bytes == 0) {
      if (end_series_flag[done_rank - first_cn_rank]) {
        DEBUG("IOForwarder %d, Read Phase: Received finisher from "
              "compute node %d.\n", erank, done_rank);

        if (++flag == NUM_CN_PER_IO)
          break;
      } else {
        // TODO: remove the next line
        DEBUG("IOForwarder %d, done_rank = %d, first_cn_rank = %d\n", erank,
              done_rank, first_cn_rank);
        int series = file_index[done_rank - first_cn_rank].size() - 1;
        DEBUG("IOForwarder %d, Read Phase: Received series %d finisher from "
              "compute node %d.\n", erank, series, done_rank);
        end_series_flag[done_rank - first_cn_rank] = 1;
        expected_chunk[done_rank - first_cn_rank] = 0;

        MPI_ERR(MPI_Irecv(recv_buff[done_rank - first_cn_rank], max_msg_bytes,
                          MPI_BYTE, done_rank, 0, cn_io_comm,
                          &RecvReq[done_rank - first_cn_rank]));
      }
    } else {
      std::swap(recv_buff[done_rank - first_cn_rank], fw_buff);
      MPI_ERR(MPI_Irecv(recv_buff[done_rank - first_cn_rank],
                        max_msg_bytes,
                        MPI_BYTE,
                        done_rank,
                        0,
                        cn_io_comm,
                        &RecvReq[done_rank - first_cn_rank]));

      int chunk_num = expected_chunk[done_rank - first_cn_rank];
      if (chunk_num == 0) { // new series is starting
        end_series_flag[done_rank - first_cn_rank] = 0;
        file_index[done_rank - first_cn_rank].push_back(
          std::vector<std::pair<int,long> >());
      }
      expected_chunk[done_rank - first_cn_rank]++;
      int series = file_index[done_rank - first_cn_rank].size() - 1;
      file_index[done_rank - first_cn_rank][series].push_back(
        std::pair<int, long>(recv_buff_bytes, file_offset));

      DEBUG(
        "IOForwarder %d, Read Phase: Commiting chunk %d of series %d from "
        "compute node %d.\n", erank, chunk_num, series, done_rank);
      DEBUG(
        "IOForwarder %d, Read Phase: Commited %d bytes starting at offset "
        "%ld.\n", erank, recv_buff_bytes, file_offset);
      DEBUG(
        "IOForwarder %d, Read Phase: file_index[%d][%d][%d] = (%d, %ld).\n",
        erank, done_rank - first_cn_rank, series,
        file_index[done_rank-first_cn_rank][series].size()-1,
        file_index[done_rank-first_cn_rank][series].back().first,
        file_index[done_rank-first_cn_rank][series].back().second);

      writeToFile((char *) fw_buff, recv_buff_bytes, file);
      file_offset += recv_buff_bytes;
    }
  }

  fclose(file);

  DEBUG("IOForwarder %d, Read Phase: End of read phase.\n", erank);

  // ----- SECOND PHASE - Sending to compute nodes -----

  DEBUG("IOForwarder %d, Write Phase: Starting write phase.\n", erank);

  // Note that to find the number of series for each node
  // you can do file_index[rel_cn_rank].size()

  TYPE* send_buff[NUM_CN_PER_IO];
  for (int i = 0; i < NUM_CN_PER_IO; i++)
    send_buff[i] = buff + i * (max_msg_bytes / sizeof(TYPE));

  DEBUG("IOForwarder %d, Write Phase: file_index[0].size() = %d\n", erank,
        file_index[0].size());
  DEBUG("IOForwarder %d, Write Phase: hi 1\n", erank);
  // Stores the next chunk that should be sent to a specific CN
  // and for a specific series
  //   1st dim -> cn rank (relative)
  //   2nd dim -> series
  std::vector<int> next_chunk[NUM_CN_PER_IO];
  DEBUG("IOForwarder %d, Write Phase: hi gamisou\n", erank);
  for (int i = 0; i < NUM_CN_PER_IO; ++i) {
    DEBUG("IOForwarder %d, Write Phase: i - file_index[0].size() = %d\n", erank,
           file_index[i].size());
    for (int j = 0; j < file_index[i].size(); ++j) {
//      return;
      DEBUG("IOForwarder %d, Write Phase: j - file_index[0].size() = %d\n",
            erank, file_index[i].size());
      next_chunk[i].push_back(0);
    }
  }

  DEBUG("IOForwarder %d, Write Phase: hi 2\n", erank);
  // flags
  int num_cn_nodes_done;
  int cn_node_done[NUM_CN_PER_IO];
  int series;
  int num_series_done[NUM_CN_PER_IO];
  for (int i = 0; i < NUM_CN_PER_IO; ++i)
    num_series_done[i] = 0;  
  int Series[NUM_CN_PER_IO];

  DEBUG("IOForwarder %d, Write Phase: hi 3\n", erank);

  // MPI_Request objects
  MPI_Request SendReq[NUM_CN_PER_IO];
  for (int i = 0; i < NUM_CN_PER_IO; ++i)
    SendReq[i] = MPI_REQUEST_NULL;

  DEBUG("IOForwarder %d, Write Phase: before file open\n", erank);
  // open file for reading
  file = fopen(local_file_name, "rb");

  DEBUG("IOForwarder %d, Write Phase: after file open\n", erank);
  // send chunk0 of all series to each of your compute nodes
  num_cn_nodes_done = 0;
  for (int i = 0; i < NUM_CN_PER_IO; ++i)
    cn_node_done[i] = 0;
  series = 0;
  while (num_cn_nodes_done < NUM_CN_PER_IO) {
    for (int i = 0; i < NUM_CN_PER_IO; ++i) {
      if (cn_node_done[i]) continue;

      if (series == file_index[i].size()) {
        ++num_cn_nodes_done;
        cn_node_done[i] = 1;
        continue;
      }

      long offset = file_index[i][series][0].second;
      int count = file_index[i][series][0].first;
      readFromFile((char *) send_buff[i], offset, count, file);

      DEBUG("IOForwarder %d, Write Phase: Posting send of chunk 0, series %d "
            "to compute node %d.\n", erank, series, i + first_cn_rank);
      DEBUG("IOForwarder %d, Write Phase: Read from local file as %d bytes "
            "from offset %ld.\n", erank, count, offset);

      MPI_ERR(MPI_Isend(send_buff[i],
                        count,
                        MPI_BYTE,
                        i + first_cn_rank,
                        series,
                        cn_io_comm,
                        &SendReq[i]));

      next_chunk[i][series]++;
    }

    MPI_Waitall(NUM_CN_PER_IO, SendReq, MPI_STATUSES_IGNORE);
    ++series;
  }

  // send chunk1 of all series to each of your compute nodes
  num_cn_nodes_done = 0;
  for (int i = 0; i < NUM_CN_PER_IO; ++i)
    cn_node_done[i] = 0;
  series = 0;
  while (num_cn_nodes_done < NUM_CN_PER_IO) {
    for (int i = 0; i < NUM_CN_PER_IO; ++i) {
      if (cn_node_done[i]) continue;

      if (series == file_index[i].size()) {
        ++num_cn_nodes_done;
        cn_node_done[i] = 1;
        continue;
      }

      if (file_index[i][series].size() == 1) {
        // no chunk1 for this node and these series, send finisher
        DEBUG("IOForwarder %d, Write Phase: All chunks of series %d have been"
              "sent to compute node %d.\n", erank, series, i + first_cn_rank);
        DEBUG("IOForwarder %d, Write Phase: Sending series %d finisher "
              "to compute node %d.\n", erank, series, i + first_cn_rank);

        MPI_ERR(MPI_Isend(NULL, 0, MPI_BYTE, i + first_cn_rank, series,
                          cn_io_comm, &SendReq[i]));
        
        num_series_done[i]++;

        continue;
      }

      long offset = file_index[i][series][1].second;
      int count = file_index[i][series][1].first;
      readFromFile((char *) send_buff[i], offset, count, file);

      DEBUG("IOForwarder %d, Write Phase: Posting send of chunk 1, series %d "
            "to compute node %d.\n", erank, series, i + first_cn_rank);
      DEBUG("IOForwarder %d, Write Phase: Read from local file as %d bytes "
            "from offset %ld.\n", erank, count, offset);

      MPI_ERR(MPI_Isend(send_buff[i],
                        count,
                        MPI_BYTE,
                        i + first_cn_rank,
                        series,
                        cn_io_comm,
                        &SendReq[i]));

      next_chunk[i][series]++;
    }

    MPI_Waitall(NUM_CN_PER_IO, SendReq, MPI_STATUSES_IGNORE);
    ++series;
  }

  // wait for a series request from a node, and serve these requests
  // as they come
  num_cn_nodes_done = 0;
  for (int i = 0; i < NUM_CN_PER_IO; ++i) {
//    num_series_done[i] = 0;
//    for (int j = 0; j < file_index[i].size(); ++j)
//      if (next_chunk[i][j] == file_index[i][j].size())
//        num_series_done[i]++;

    if (num_series_done[i] == file_index[i].size()) {
      ++num_cn_nodes_done;
      continue;
    }

    DEBUG(
        "IOForwarder %d, Write Phase: Posting receive for next series "
        "from compute node %d (1).\n",
        erank, done_rank);

    MPI_ERR(MPI_Irecv(&Series[i], 1, MPI_INT, i + first_cn_rank, 0,
                      cn_io_comm, &RecvReq[i]));
  }

  print_arr(num_series_done, NUM_CN_PER_IO, "IOForwarder %d, Before while: num_series_done: ", erank);
  DEBUG("IOForwarder %d, OKAY_before_while: num_cn_nodes_done=%d\n", erank,
        num_cn_nodes_done);

  while (num_cn_nodes_done < NUM_CN_PER_IO) {
    MPI_Waitany(NUM_CN_PER_IO, RecvReq, &recv_index, MPI_STATUS_IGNORE);
    done_rank = recv_index + first_cn_rank;

    series = Series[done_rank - first_cn_rank];
    int chunk = next_chunk[done_rank - first_cn_rank][series];

    DEBUG("IOForwarder %d, Write Phase: Received request for next chunk "
          "of series %d from compute node %d.\n",
          erank, series, done_rank);

    if (file_index[done_rank - first_cn_rank][series].size() == chunk) {
      // no more chunks for this node and these series, send finisher
      DEBUG("IOForwarder %d, Write Phase: All chunks of series %d have been"
            "sent to compute node %d.\n",
            erank, series, done_rank);
      DEBUG("IOForwarder %d, Write Phase: Sending series %d finisher "
            "to compute node %d.\n",
            erank, series, done_rank);

      MPI_Send(NULL, 0, MPI_BYTE, done_rank, series, cn_io_comm);

      num_series_done[done_rank - first_cn_rank]++;
      if (num_series_done[done_rank - first_cn_rank] ==
          file_index[done_rank - first_cn_rank].size())
        ++num_cn_nodes_done;

      DEBUG("IOForwarder %d, Write Phase: Posting receive for next series "
            "from compute node %d (2).\n",
            erank, done_rank);

      MPI_ERR(MPI_Irecv(&Series[done_rank - first_cn_rank], 1, MPI_INT,
                        done_rank, 0, cn_io_comm,
                        &RecvReq[done_rank - first_cn_rank]));
      continue;
    }

    // FIXME Maybe double buffering?
    long offset = file_index[done_rank - first_cn_rank][series][chunk].second;
    int count = file_index[done_rank - first_cn_rank][series][chunk].first;
    
    MPI_Wait(&SendReq[0], MPI_STATUS_IGNORE);

    readFromFile((char *) send_buff[0], offset,
                 count, file);

    DEBUG("IOForwarder %d, Write Phase: Posting send of chunk %d, series %d "
          "to compute node %d.\n",
          erank, chunk, series, done_rank);
    DEBUG("IOForwarder %d, Write Phase: Read from local file as %d bytes "
          "from offset %ld.\n", erank, count, offset);

    MPI_ERR(MPI_Isend(send_buff[0],
                      count,
                      MPI_BYTE,
                      done_rank,
                      series,
                      cn_io_comm,
                      &SendReq[0]));

    next_chunk[done_rank - first_cn_rank][series]++;

    DEBUG(
        "IOForwarder %d, Write Phase: Posting receive for next series "
        "from compute node %d (3).\n",
        erank, done_rank);

    MPI_ERR(MPI_Irecv(&Series[done_rank - first_cn_rank], 1, MPI_INT,
                      done_rank, 0, cn_io_comm,
                      &RecvReq[done_rank - first_cn_rank]));
  }

  for (int i=0; i<NUM_CN_PER_IO; ++i) {
    CHECK(RecvReq[i] != MPI_REQUEST_NULL);
    MPI_Cancel(&RecvReq[i]);
  }
  // free allocated resources
  fclose(file);
  free(buff);

  DEBUG("IOForwarder %d, Write Phase: End of write phase.\n", erank);
}
