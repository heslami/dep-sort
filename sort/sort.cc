#include <cstdio>
#include <iostream>

#include "mpi.h"

#include "dep_sort_config.h"
#include "utils.h"

void doComputeNode(MPI_Comm cn_fs_comm, MPI_Comm cn_io_comm);
void doIOForwarder(MPI_Comm cn_io_comm);
void doFileServer(MPI_Comm fileserver_comm, MPI_Comm cn_fs_comm);

int main(int argc, char** argv) {
  CHECK(NUM_COMPUTE_NODE % NUM_IO_FORWARDER == 0);
  CHECK(NUM_COMPUTE_NODE % NUM_FILE_SERVER == 0);
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  CHECK(provided == MPI_THREAD_MULTIPLE);

//  MPI_Init(0, 0);
  int num_all_nodes =
      NUM_COMPUTE_NODE + NUM_IO_FORWARDER + NUM_FILE_SERVER;
  int world_size, rank;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  CHECK(num_all_nodes == world_size);

  /* Communicators for all the groups of nodes that may need to exchange *
   * messages or synchronize                                             */
  MPI_Comm cnio_fs_comm;
  MPI_Comm cnfs_io_comm;

  /***** The order of nodes is assumed to be CN, IO, FS *****/

  MPI_Comm_split(MPI_COMM_WORLD,
                 (rank < NUM_COMPUTE_NODE + NUM_IO_FORWARDER) ? 0 : 1,
                 rank, &cnio_fs_comm);

  MPI_Comm_split(MPI_COMM_WORLD,
                 ((rank < NUM_COMPUTE_NODE + NUM_IO_FORWARDER) &&
                  (rank >= NUM_COMPUTE_NODE)) ? 0 : 1,
                 rank, &cnfs_io_comm);

  if (rank < NUM_COMPUTE_NODE) {
    // We are a compute node
    doComputeNode(cnfs_io_comm, cnio_fs_comm);
  } else if (rank < NUM_COMPUTE_NODE + NUM_IO_FORWARDER) {
    // We are a compute side data node
    doIOForwarder(cnio_fs_comm);
  } else {
    // We are a storage side data node
    doFileServer(cnio_fs_comm, cnfs_io_comm);
  }

  MPI_Comm_free(&cnio_fs_comm);
  MPI_Comm_free(&cnfs_io_comm);
  MPI_Finalize();
  return 0;
}
