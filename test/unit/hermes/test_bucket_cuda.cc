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

// #include "hermes/data_stager/nvidia_gds_stager.h"
// #include "basic_test.h"
// #include "chimaera/api/chimaera_client.h"
// #include "chimaera_admin/chimaera_admin.h"
// #include "hermes/bucket.h"
#include "hermes/data_stager/stager_factory.h"
// #include "hermes/hermes.h"

// TEST_CASE("TestHermesGdsDataStager") {
//   int rank, nprocs;
//   MPI_Barrier(MPI_COMM_WORLD);
//   MPI_Comm_rank(MPI_COMM_WORLD, &rank);
//   MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

//   // create dataset
//   std::string home_dir = getenv("HOME");
//   std::string path = home_dir + "/test.txt";
//   size_t count_per_proc = 16;
//   size_t off = rank * count_per_proc;
//   size_t proc_count = off + count_per_proc;
//   size_t page_size = KILOBYTES(4);
//   size_t file_size = nprocs * page_size * 16;
//   std::vector<char> data(file_size, 0);
//   if (rank == 0) {
//     FILE *file = fopen(path.c_str(), "w");
//     fwrite(data.data(), sizeof(char), data.size(), file);
//     fclose(file);
//   }
//   MPI_Barrier(MPI_COMM_WORLD);

//   // Initialize Hermes on all nodes
//   HERMES->ClientInit();

//   // Create a stageable bucket
//   hermes::Context ctx = hermes::BinaryFileStager::BuildContext(page_size);
//   hermes::Bucket bkt(HSHM_DEFAULT_MEM_CTX, path, ctx, file_size);

//   // Put a few blobs in the bucket
//   for (size_t i = off; i < proc_count; ++i) {
//     HILOG(kInfo, "Iteration: {}", i);
//     // Put a blob
//     hermes::Blob blob(page_size);
//     memset(blob.data(), i % 256, blob.size());
//     chi::string blob_name =
//     hermes::adapter::BlobPlacement::CreateBlobName(i);
//     bkt.Put(blob_name.str(), blob, ctx);
//     hermes::Blob blob2;
//     bkt.Get(blob_name.str(), blob2, ctx);
//     assert(blob2.size() == page_size);
//     assert(blob2 == blob);
//   }
//   for (size_t i = off; i < proc_count; ++i) {
//     chi::string blob_name =
//     hermes::adapter::BlobPlacement::CreateBlobName(i); HILOG(kInfo,
//     "ContainsBlob Iteration: {}", i);
//     assert(bkt.ContainsBlob(blob_name.str()));
//   }
//   MPI_Barrier(MPI_COMM_WORLD);

//   // Flush all data to final disk
//   CHI_ADMIN->Flush(HSHM_DEFAULT_MEM_CTX, chi::DomainQuery::GetGlobalBcast());
//   HILOG(kInfo, "Flushing finished");

//   // Get the size of data on disk
//   if (rank == 0) {
//     size_t end_size = stdfs::file_size(path);
//     assert(end_size == file_size);
//   }
//   MPI_Barrier(MPI_COMM_WORLD);

//   // Verify the file contents flushed
//   FILE *file = fopen(path.c_str(), "r");
//   for (size_t i = off; i < proc_count; ++i) {
//     fseek(file, i * page_size, SEEK_SET);
//     hermes::Blob blob(page_size);
//     fread(blob.data(), sizeof(char), page_size, file);
//     for (size_t j = 0; j < page_size; ++j) {
//       assert(blob.data()[j] == i % 256);
//     }
//   }
//   fclose(file);
//   MPI_Barrier(MPI_COMM_WORLD);

//   // Remove the file
//   if (rank == 0) {
//     stdfs::remove(path);
//   }
// }

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
  HERMES->ClientInit();

  // Create a stageable bucket
  hermes::Context ctx = hermes::BinaryFileStager::BuildContext(page_size);
  hermes::Bucket bkt(HSHM_DEFAULT_MEM_CTX, path, ctx, file_size);

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

// #include <mpi.h>

// #include <cassert>
// #include <cstring>
// #include <iostream>
// #include <vector>

// #include "hermes/data_stager/nvidia_gds_stager.h"

// void test_register_stager() {
//   std::string test_file = "/tmp/test_nvidia_gds_stager.bin";
//   size_t page_size = 4096;
//   u32 flags = 0;

//   hermes::NvidiaGdsStager stager;
//   hermes::Context ctx = hermes::NvidiaGdsStager::BuildContext(page_size,
//   flags); stager.RegisterStager(HSHM_DEFAULT_MEM_CTX, test_file,
//   ctx.bkt_params_);

//   std::cout << "[PASSED] test_register_stager" << std::endl;
// }

// void test_stage_in() {
//   std::string test_file = "/tmp/test_nvidia_gds_stager.bin";
//   size_t page_size = 4096;

//   // Create a test file with sample data
//   std::vector<char> input_data(page_size, 'A');
//   FILE *file = fopen(test_file.c_str(), "wb");
//   fwrite(input_data.data(), sizeof(char), input_data.size(), file);
//   fclose(file);

//   // Initialize stager and stage data
//   hermes::NvidiaGdsStager stager;
//   hermes::Context ctx = hermes::NvidiaGdsStager::BuildContext(page_size);
//   hermes::Client client;
//   stager.RegisterStager(HSHM_DEFAULT_MEM_CTX, test_file, ctx.bkt_params_);
//   stager.StageIn(HSHM_DEFAULT_MEM_CTX, client, hermes::TagId(1), "blob_1",
//   0.0);

//   std::cout << "[PASSED] test_stage_in" << std::endl;
// }

// void test_stage_out() {
//   std::string test_file = "/tmp/test_nvidia_gds_stager.bin";
//   size_t page_size = 4096;

//   hermes::NvidiaGdsStager stager;
//   hermes::Context ctx = hermes::NvidiaGdsStager::BuildContext(page_size);
//   hermes::Client client;

//   stager.RegisterStager(HSHM_DEFAULT_MEM_CTX, test_file, ctx.bkt_params_);

//   // Simulate data to stage out
//   hipc::Pointer data_p = hipc::Pointer::Create(page_size);
//   memset(data_p.get(), 'B', page_size);

//   stager.StageOut(HSHM_DEFAULT_MEM_CTX, client, hermes::TagId(1), "blob_2",
//                   data_p, page_size);

//   // Verify contents written to file
//   std::vector<char> output_data(page_size);
//   FILE *file = fopen(test_file.c_str(), "rb");
//   fread(output_data.data(), sizeof(char), output_data.size(), file);
//   fclose(file);

//   assert(std::all_of(output_data.begin(), output_data.end(),
//                      [](char c) { return c == 'B'; }));
//   std::cout << "[PASSED] test_stage_out" << std::endl;
// }

// void test_update_size() {
//   std::string test_file = "/tmp/test_nvidia_gds_stager.bin";
//   size_t page_size = 4096;

//   hermes::NvidiaGdsStager stager;
//   hermes::Context ctx = hermes::NvidiaGdsStager::BuildContext(page_size);
//   hermes::Client client;

//   stager.RegisterStager(HSHM_DEFAULT_MEM_CTX, test_file, ctx.bkt_params_);
//   stager.UpdateSize(HSHM_DEFAULT_MEM_CTX, client, hermes::TagId(1), "blob_3",
//   0,
//                     page_size);

//   std::cout << "[PASSED] test_update_size" << std::endl;
// }

// int main(int argc, char **argv) {
//   MPI_Init(&argc, &argv);

//   int rank;
//   MPI_Comm_rank(MPI_COMM_WORLD, &rank);

//   if (rank == 0) {
//     std::cout << "Running NvidiaGdsStager Tests..." << std::endl;

//     test_register_stager();
//     test_stage_in();
//     test_stage_out();
//     test_update_size();

//     std::cout << "All tests passed successfully!" << std::endl;
//   }

//   MPI_Barrier(MPI_COMM_WORLD);
//   MPI_Finalize();
//   return 0;
// }
