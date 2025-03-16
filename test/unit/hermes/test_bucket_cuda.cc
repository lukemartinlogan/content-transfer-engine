/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
#define HERMES_ENABLE_GDS_STAGER
#include <mpi.h>

#define HERMES_ENABLE_NVIDIA_GDS_ADAPTER
#include "hermes/data_stager/stager_factory.h"

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);

  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // create dataset
  std::string home_dir = getenv("HOME");
  std::string path = home_dir + "/test.txt";
  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  size_t page_size = KILOBYTES(4);
  size_t file_size = nprocs * page_size * 16;
  std::vector<char> data(file_size, 0);
  if (rank == 0) {
    FILE *file = fopen(path.c_str(), "w");
    fwrite(data.data(), sizeof(char), data.size(), file);
    fclose(file);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Initialize Hermes on all nodes
  TRANSPARENT_HERMES();

  // Create a stageable bucket
  hermes::Context ctx = hermes::NvidiaGdsStager::BuildContext(page_size);
  hermes::Bucket bkt(path, ctx, file_size);

  // Put a few blobs in the bucket
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(page_size);
    memset(blob.data(), i % 256, blob.size());
    chi::string blob_name = hermes::adapter::BlobPlacement::CreateBlobName(i);
    bkt.Put(blob_name.str(), blob, ctx);
    hermes::Blob blob2;
    bkt.Get(blob_name.str(), blob2, ctx);
    assert(blob2.size() == page_size);
    assert(blob2 == blob);
  }
  for (size_t i = off; i < proc_count; ++i) {
    chi::string blob_name = hermes::adapter::BlobPlacement::CreateBlobName(i);
    HILOG(kInfo, "ContainsBlob Iteration: {}", i);
    assert(bkt.ContainsBlob(blob_name.str()));
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Flush all data to final disk
  CHI_ADMIN->Flush(HSHM_DEFAULT_MEM_CTX, chi::DomainQuery::GetGlobalBcast());
  HILOG(kInfo, "Flushing finished");

  // Get the size of data on disk
  if (rank == 0) {
    size_t end_size = stdfs::file_size(path);
    assert(end_size == file_size);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Verify the file contents flushed
  FILE *file = fopen(path.c_str(), "r");
  for (size_t i = off; i < proc_count; ++i) {
    fseek(file, i * page_size, SEEK_SET);
    hermes::Blob blob(page_size);
    fread(blob.data(), sizeof(char), page_size, file);
    for (size_t j = 0; j < page_size; ++j) {
      assert(blob.data()[j] == i % 256);
    }
  }
  fclose(file);
  MPI_Barrier(MPI_COMM_WORLD);

  // Remove the file
  if (rank == 0) {
    stdfs::remove(path);
  }

  MPI_Finalize();
  return 0;
}
