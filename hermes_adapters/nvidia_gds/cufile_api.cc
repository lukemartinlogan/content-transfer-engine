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

bool cuFile_Intercepted = true;

#include "cufile_api.h"

#include <limits.h>
#include <sys/file.h>

#include <cstdio>

namespace stdfs = std::filesystem;

extern "C" {
// Interceptor functions
CUfileError_t cuFileHandleRegister(CUfileHandle_t *fh, CUfileDescr_t *descr) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileHandleRegister(fh, descr);
}

void cuFileHandleDeregister(CUfileHandle_t fh) {
  //    printf("Intercepted the REAL API\n");
  HERMES_CUFILE_API->cuFileHandleDeregister(fh);
}

CUfileError_t cuFileBufRegister(const void *buf, size_t size, int flags) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileBufRegister(buf, size, flags);
}

CUfileError_t cuFileBufDeregister(const void *buf) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileBufDeregister(buf);
}

ssize_t cuFileRead(CUfileHandle_t fh, void *buf, size_t size, off_t offset,
                   off_t offset2) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileRead(fh, buf, size, offset, offset2);
}

ssize_t cuFileWrite(CUfileHandle_t fh, const void *buf, size_t size,
                    off_t offset, off_t offset2) {
  //    printf("Intercepted the REAL API\n");

  return HERMES_CUFILE_API->cuFileWrite(fh, buf, size, offset, offset2);
}

long cuFileUseCount() {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileUseCount();
}

CUfileError_t cuFileDriverGetProperties(CUfileDrvProps_t *props) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileDriverGetProperties(props);
}

CUfileError_t cuFileDriverSetPollMode(bool poll_mode,
                                      size_t poll_threshold_size) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileDriverSetPollMode(poll_mode,
                                                    poll_threshold_size);
}

CUfileError_t cuFileDriverSetMaxDirectIOSize(size_t size) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileDriverSetMaxDirectIOSize(size);
}

CUfileError_t cuFileDriverSetMaxCacheSize(size_t size) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileDriverSetMaxCacheSize(size);
}

CUfileError_t cuFileDriverSetMaxPinnedMemSize(size_t size) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileDriverSetMaxPinnedMemSize(size);
}

CUfileError_t cuFileBatchIOSetUp(CUfileBatchHandle_t *handle, unsigned flags) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileBatchIOSetUp(handle, flags);
}

CUfileError_t cuFileBatchIOSubmit(CUfileBatchHandle_t handle, unsigned num_ios,
                                  CUfileIOParams_t *io_params,
                                  unsigned int flags) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileBatchIOSubmit(handle, num_ios, io_params,
                                                flags);
}

CUfileError_t cuFileBatchIOGetStatus(CUfileBatchHandle_t handle,
                                     unsigned num_ios, unsigned *num_completed,
                                     CUfileIOEvents_t *events,
                                     struct timespec *timeout) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileBatchIOGetStatus(
      handle, num_ios, num_completed, events, timeout);
}

CUfileError_t cuFileBatchIOCancel(CUfileBatchHandle_t handle) {
  //    printf("Intercepted the REAL API\n");
  return HERMES_CUFILE_API->cuFileBatchIOCancel(handle);
}

void cuFileBatchIODestroy(CUfileBatchHandle_t handle) {
  //    printf("Intercepted the REAL API\n");
  HERMES_CUFILE_API->cuFileBatchIODestroy(handle);
}
}  // extern C
