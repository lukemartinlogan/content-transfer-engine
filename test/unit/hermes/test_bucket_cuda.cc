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

HSHM_GPU_KERNEL void test_bucket(hermes::Bucket bkt) {
  printf("IN KERNEL!!! A\n");
  hermes::Blob blob(1024);
  printf("I DIDN'T DIE?\n");
  memset(blob.data(), 0, blob.size());
  printf("I STILL DIDN'T DIE?\n");
  // bkt.mdm_.AsyncPutBlobAlloc(
  //     HSHM_MCTX, CHI_CLIENT->MakeTaskNodeId(),
  //     chi::DomainQuery::GetDynamic(), bkt.id_, chi::string("a"),
  //     hermes::BlobId::GetNull(), 0, blob.size(), blob.data(), 1, 0, 0,
  //     hermes::Context());
  bkt.Put(chi::string("a"), blob, hermes::Context());
}

int main(int argc, char **argv) {
  HERMES_INIT();
  hermes::Bucket bkt(chi::string("bucket"));
  HILOG(kInfo, "Created bucket: {}", bkt.GetId());
  test_bucket<<<1, 1>>>(bkt);
  hshm::GpuApi::Synchronize();
  return 0;
}
