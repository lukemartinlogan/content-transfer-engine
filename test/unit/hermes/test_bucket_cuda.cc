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

HSHM_GPU_KERNEL void test_kernel(hermes::Bucket bkt) {
  hermes::Blob blob(1024);
  bkt.Put(chi::string("a"), blob, hermes::Context());
}

int main(int argc, char **argv) {
  HERMES_INIT();
  hermes::Bucket bkt(chi::string("bucket"));
  test_kernel<<<1, 1>>>(bkt);
  return 0;
}
