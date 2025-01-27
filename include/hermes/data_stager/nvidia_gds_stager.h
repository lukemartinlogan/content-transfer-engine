// // Created by [shazzadul islam] on [28th dec 2024]

// #ifndef HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_
// #define HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_

// #include <cuda_runtime.h>

// #include "abstract_stager.h"
// #include "hermes_adapters/mapper/abstract_mapper.h"
// namespace hermes {

// class NvidiaGdsStager : public AbstractStager {
//  public:
//   size_t page_size_;
//   std::string path_;
//   bitfield32_t flags_;

//  public:
//   /** Default constructor */
//   NvidiaGdsStager() = default;

//   /** Destructor */
//   ~NvidiaGdsStager() {}

//   /** Build context for staging */
//   static Context BuildContext(size_t page_size, u32 flags = 0,
//                               size_t elmt_size = 1) {
//     Context ctx;
//     ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
//     ctx.bkt_params_ = BuildFileParams(page_size, flags, elmt_size);
//     return ctx;
//   }

//   /** Build serialized file parameter pack */
//   static std::string BuildFileParams(size_t page_size, u32 flags = 0,
//                                      size_t elmt_size = 1) {
//     chi::string params(32);
//     page_size = (page_size / elmt_size) * elmt_size;
//     hipc::LocalSerialize srl(params);
//     srl << std::string("nvidia_gds");
//     srl << flags;
//     srl << page_size;
//     return params.str();
//   }

//   /** Initialize the stager */
//   void RegisterStager(const hipc::MemContext &mctx, const std::string
//   &tag_name,
//                       const std::string &params) override {
//     std::string protocol;
//     hipc::LocalDeserialize srl(params);
//     srl >> protocol;
//     srl >> flags_.bits_;
//     srl >> page_size_;
//     path_ = tag_name;
//   }

//   /** Stage data in from a remote source */
//   void StageIn(const hipc::MemContext &mctx, hermes::Client &client,
//                const TagId &tag_id, const std::string &blob_name,
//                float score) override {
//     if (flags_.Any(HERMES_STAGE_NO_READ)) {
//       return;
//     }

//     adapter::BlobPlacement plcmnt;
//     plcmnt.DecodeBlobName(blob_name, page_size_);

//     HILOG(kDebug, "Staging {} bytes from {} at offset {}", page_size_, path_,
//           plcmnt.bucket_off_);

//     // Allocate GPU memory
//     void *gpu_ptr = nullptr;
//     cudaError_t err = cudaMalloc(&gpu_ptr, page_size_);
//     if (err != cudaSuccess) {
//       HELOG(kError, "Failed to allocate GPU memory: {}",
//             cudaGetErrorString(err));
//       return;
//     }

//     char *host_buffer = new char[page_size_];
//     int fd = open(path_.c_str(), O_CREAT | O_RDWR, 0666);
//     if (fd < 0) {
//       HELOG(kError, "Failed to open file: {}", path_);
//       delete[] host_buffer;
//       cudaFree(gpu_ptr);
//       return;
//     }

//     ssize_t real_size =
//         pread(fd, host_buffer, page_size_, (off_t)plcmnt.bucket_off_);
//     close(fd);

//     if (real_size <= 0) {
//       HELOG(kError, "Failed to read data from file: {}", path_);
//       delete[] host_buffer;
//       cudaFree(gpu_ptr);
//       return;
//     }

//     // Copy data from host to GPU
//     err = cudaMemcpy(gpu_ptr, host_buffer, real_size,
//     cudaMemcpyHostToDevice); delete[] host_buffer; if (err != cudaSuccess) {
//       HELOG(kError, "Failed to copy data to GPU memory: {}",
//             cudaGetErrorString(err));
//       cudaFree(gpu_ptr);
//       return;
//     }

//     HILOG(kDebug, "Staged {} bytes from {} to GPU memory", real_size, path_);

//     cudaFree(gpu_ptr);  // Free GPU memory after use
//   }

//   /** Stage data out to a remote source */
//   void StageOut(const hipc::MemContext &mctx, hermes::Client &client,
//                 const TagId &tag_id, const std::string &blob_name,
//                 hipc::Pointer &data_p, size_t data_size) override {
//     if (flags_.Any(HERMES_STAGE_NO_WRITE)) {
//       return;
//     }

//     adapter::BlobPlacement plcmnt;
//     plcmnt.DecodeBlobName(blob_name, page_size_);

//     HILOG(kDebug, "Staging {} bytes to {} at offset {}", data_size, path_,
//           plcmnt.bucket_off_);

//     // Allocate GPU memory
//     void *gpu_ptr = nullptr;
//     cudaError_t err = cudaMalloc(&gpu_ptr, data_size);
//     if (err != cudaSuccess) {
//       HELOG(kError, "Failed to allocate GPU memory: {}",
//             cudaGetErrorString(err));
//       return;
//     }

//     // Simulate data retrieval from GPU memory to host buffer
//     char *host_buffer = new char[data_size];
//     err = cudaMemcpy(host_buffer, gpu_ptr, data_size,
//     cudaMemcpyDeviceToHost); cudaFree(gpu_ptr); if (err != cudaSuccess) {
//       HELOG(kError, "Failed to copy data from GPU memory: {}",
//             cudaGetErrorString(err));
//       delete[] host_buffer;
//       return;
//     }

//     int fd = open(path_.c_str(), O_CREAT | O_RDWR, 0666);
//     if (fd < 0) {
//       HELOG(kError, "Failed to open file: {}", path_);
//       delete[] host_buffer;
//       return;
//     }

//     ssize_t real_size =
//         pwrite(fd, host_buffer, data_size, (off_t)plcmnt.bucket_off_);
//     close(fd);
//     delete[] host_buffer;

//     if (real_size < 0) {
//       HELOG(kError, "Failed to write data to file: {}", path_);
//       return;
//     }

//     HILOG(kDebug, "Staged {} bytes to {}", real_size, path_);
//   }

//   /** Update metadata size */
//   void UpdateSize(const hipc::MemContext &mctx, hermes::Client &client,
//                   const TagId &tag_id, const std::string &blob_name,
//                   size_t blob_off, size_t data_size) override {
//     adapter::BlobPlacement p;
//     p.DecodeBlobName(blob_name, page_size_);
//     HILOG(kDebug, "Updated size for blob {} with offset {} and size {}",
//           blob_name, blob_off, data_size);
//   }
// };

// }  // namespace hermes

// #endif  // HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_

// #ifndef HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_
// #define HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_

// #include <cuda_runtime.h>
// #include <cufile.h>
// // #include <cufile_api.h>

// #include "abstract_stager.h"
// #include "hermes_adapters/mapper/abstract_mapper.h"

// namespace hermes {

// class NvidiaGdsStager : public AbstractStager {
//  public:
//   size_t page_size_;
//   std::string path_;
//   bitfield32_t flags_;

//  public:
//   /** Default constructor */
//   NvidiaGdsStager() = default;

//   /** Destructor */
//   ~NvidiaGdsStager() {}

//   /** Build context for staging */
//   static Context BuildContext(size_t page_size, u32 flags = 0,
//                               size_t elmt_size = 1) {
//     Context ctx;
//     ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
//     ctx.bkt_params_ = BuildFileParams(page_size, flags, elmt_size);
//     return ctx;
//   }

//   /** Build serialized file parameter pack */
//   static std::string BuildFileParams(size_t page_size, u32 flags = 0,
//                                      size_t elmt_size = 1) {
//     chi::string params(32);
//     page_size = (page_size / elmt_size) * elmt_size;
//     hipc::LocalSerialize srl(params);
//     srl << std::string("nvidia_gds");
//     srl << flags;
//     srl << page_size;
//     return params.str();
//   }

//   /** Initialize the stager */
//   void RegisterStager(const hipc::MemContext &mctx, const std::string
//   &tag_name,
//                       const std::string &params) override {
//     std::string protocol;
//     hipc::LocalDeserialize srl(params);
//     srl >> protocol;
//     srl >> flags_.bits_;
//     srl >> page_size_;
//     path_ = tag_name;
//   }

//   /** Stage data in from a remote source */
//   void StageIn(const hipc::MemContext &mctx, hermes::Client &client,
//                const TagId &tag_id, const std::string &blob_name,
//                float score) override {
//     if (flags_.Any(HERMES_STAGE_NO_READ)) {
//       return;
//     }

//     adapter::BlobPlacement plcmnt;
//     plcmnt.DecodeBlobName(blob_name, page_size_);

//     HILOG(kDebug, "Staging {} bytes from {} at offset {}", page_size_, path_,
//           plcmnt.bucket_off_);

//     void *gpu_ptr = nullptr;
//     cudaError_t err = cudaMalloc(&gpu_ptr, page_size_);
//     if (err != cudaSuccess) {
//       HELOG(kError, "Failed to allocate GPU memory: {}",
//             cudaGetErrorString(err));
//       return;
//     }

//     CUfileHandle_t cufile_handle;
//     CUfileError_t cf_err =
//         cuFileHandleRegister(&cufile_handle, path_.c_str(), O_RDONLY);
//     if (cf_err.err != CU_FILE_SUCCESS) {
//       HELOG(kError, "Failed to register file with CUFILE API: {}",
//       cf_err.err); cudaFree(gpu_ptr); return;
//     }

//     ssize_t real_size =
//         cuFileRead(cufile_handle, gpu_ptr, page_size_, plcmnt.bucket_off_,
//         0);
//     cuFileHandleDeregister(cufile_handle);

//     if (real_size < 0) {
//       HELOG(kError, "Failed to read data via CUFILE API");
//       cudaFree(gpu_ptr);
//       return;
//     }

//     HILOG(kDebug, "Staged {} bytes from {} to GPU memory", real_size, path_);
//   }

//   /** Stage data out to a remote source */
//   void StageOut(const hipc::MemContext &mctx, hermes::Client &client,
//                 const TagId &tag_id, const std::string &blob_name,
//                 hipc::Pointer &data_p, size_t data_size) override {
//     if (flags_.Any(HERMES_STAGE_NO_WRITE)) {
//       return;
//     }

//     adapter::BlobPlacement plcmnt;
//     plcmnt.DecodeBlobName(blob_name, page_size_);

//     HILOG(kDebug, "Staging {} bytes to {} at offset {}", data_size, path_,
//           plcmnt.bucket_off_);

//     CUfileHandle_t cufile_handle;
//     CUfileError_t cf_err =
//         cuFileHandleRegister(&cufile_handle, path_.c_str(), O_WRONLY);
//     if (cf_err.err != CU_FILE_SUCCESS) {
//       HELOG(kError, "Failed to register file with CUFILE API: {}",
//       cf_err.err); return;
//     }

//     ssize_t real_size = cuFileWrite(cufile_handle, data_p.get(), data_size,
//                                     plcmnt.bucket_off_, 0);
//     cuFileHandleDeregister(cufile_handle);

//     if (real_size < 0) {
//       HELOG(kError, "Failed to write data via CUFILE API");
//       return;
//     }

//     HILOG(kDebug, "Staged {} bytes to {}", real_size, path_);
//   }
// };

// }  // namespace hermes

// #endif  // HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_

// #ifndef HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_
// #define HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_

// #include <cuda_runtime.h>
// #include <cufile.h>
// #include <fcntl.h>
// #include <unistd.h>

// #include "abstract_stager.h"
// #include "hermes_adapters/mapper/abstract_mapper.h"

// namespace hermes {

// class NvidiaGdsStager : public AbstractStager {
//  public:
//   size_t page_size_;
//   std::string path_;
//   bitfield32_t flags_;

//  public:
//   /** Default constructor */
//   NvidiaGdsStager() = default;

//   /** Destructor */
//   ~NvidiaGdsStager() override {}

//   /** Build context for staging */
//   static Context BuildContext(size_t page_size, u32 flags = 0,
//                               size_t elmt_size = 1) {
//     Context ctx;
//     ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
//     ctx.bkt_params_ = BuildFileParams(page_size, flags, elmt_size);
//     return ctx;
//   }

//   /** Build serialized file parameter pack */
//   static std::string BuildFileParams(size_t page_size, u32 flags = 0,
//                                      size_t elmt_size = 1) {
//     chi::string params(32);
//     page_size = (page_size / elmt_size) * elmt_size;
//     hipc::LocalSerialize srl(params);
//     srl << std::string("nvidia_gds");
//     srl << flags;
//     srl << page_size;
//     return params.str();
//   }

//   /** Initialize the stager */
//   void RegisterStager(const hipc::MemContext &mctx, const std::string
//   &tag_name,
//                       const std::string &params) override {
//     std::string protocol;
//     hipc::LocalDeserialize srl(params);
//     srl >> protocol;
//     srl >> flags_.bits_;
//     srl >> page_size_;
//     path_ = tag_name;
//   }

//   /** Stage data in from a remote source */
//   void StageIn(const hipc::MemContext &mctx, hermes::Client &client,
//                const TagId &tag_id, const std::string &blob_name,
//                float score) override {
//     if (flags_.Any(HERMES_STAGE_NO_READ)) {
//       return;
//     }

//     adapter::BlobPlacement plcmnt;
//     plcmnt.DecodeBlobName(blob_name, page_size_);

//     HILOG(kDebug, "Staging {} bytes from {} at offset {}", page_size_, path_,
//           plcmnt.bucket_off_);

//     void *gpu_ptr = nullptr;
//     cudaError_t err = cudaMalloc(&gpu_ptr, page_size_);
//     if (err != cudaSuccess) {
//       HELOG(kError, "Failed to allocate GPU memory: {}",
//             cudaGetErrorString(err));
//       return;
//     }

//     CUfileHandle_t cufile_handle;
//     CUfileDescr_t cufile_descr = {};
//     cufile_descr.handle.fd = open(path_.c_str(), O_RDONLY);
//     if (cufile_descr.handle.fd < 0) {
//       HELOG(kError, "Failed to open file: {}", strerror(errno));
//       cudaFree(gpu_ptr);
//       return;
//     }

//     CUfileError_t cf_err = cuFileHandleRegister(&cufile_handle,
//     &cufile_descr); if (cf_err.err != CU_FILE_SUCCESS) {
//       HELOG(kError, "Failed to register file with CUFILE API: {}",
//       cf_err.err); close(cufile_descr.handle.fd); cudaFree(gpu_ptr); return;
//     }

//     ssize_t real_size =
//         cuFileRead(cufile_handle, gpu_ptr, page_size_, plcmnt.bucket_off_,
//         0);
//     cuFileHandleDeregister(cufile_handle);
//     close(cufile_descr.handle.fd);

//     if (real_size < 0) {
//       HELOG(kError, "Failed to read data via CUFILE API");
//       cudaFree(gpu_ptr);
//       return;
//     }

//     HILOG(kDebug, "Staged {} bytes from {} to GPU memory", real_size, path_);
//   }

//   /** Stage data out to a remote source */
//   void StageOut(const hipc::MemContext &mctx, hermes::Client &client,
//                 const TagId &tag_id, const std::string &blob_name,
//                 hipc::Pointer &data_p, size_t data_size) override {
//     if (flags_.Any(HERMES_STAGE_NO_WRITE)) {
//       return;
//     }

//     adapter::BlobPlacement plcmnt;
//     plcmnt.DecodeBlobName(blob_name, page_size_);

//     HILOG(kDebug, "Staging {} bytes to {} at offset {}", data_size, path_,
//           plcmnt.bucket_off_);

//     CUfileHandle_t cufile_handle;
//     CUfileDescr_t cufile_descr = {};
//     cufile_descr.handle.fd = open(path_.c_str(), O_WRONLY);
//     if (cufile_descr.handle.fd < 0) {
//       HELOG(kError, "Failed to open file: {}", strerror(errno));
//       return;
//     }

//     CUfileError_t cf_err = cuFileHandleRegister(&cufile_handle,
//     &cufile_descr); if (cf_err.err != CU_FILE_SUCCESS) {
//       HELOG(kError, "Failed to register file with CUFILE API: {}",
//       cf_err.err); close(cufile_descr.handle.fd); return;
//     }

//     void *raw_data = reinterpret_cast<void *>(&data_p);
//     ssize_t real_size =
//         cuFileWrite(cufile_handle, raw_data, data_size, plcmnt.bucket_off_,
//         0);
//     cuFileHandleDeregister(cufile_handle);
//     close(cufile_descr.handle.fd);

//     if (real_size < 0) {
//       HELOG(kError, "Failed to write data via CUFILE API");
//       return;
//     }

//     HILOG(kDebug, "Staged {} bytes to {}", real_size, path_);
//   }
// };

// }  // namespace hermes

// #endif  // HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_

// Created by [shazzadul islam] on [28th dec 2024]

#ifndef HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_
#define HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_

#include <cuda_runtime.h>
#include <cufile.h>
#include <fcntl.h>
#include <unistd.h>

#include "abstract_stager.h"
#include "hermes_adapters/mapper/abstract_mapper.h"

namespace hermes {

class NvidiaGdsStager : public AbstractStager {
 public:
  size_t page_size_;
  std::string path_;
  bitfield32_t flags_;
  CUfileHandle_t cufile_handle_;

 public:
  /** Default constructor */
  NvidiaGdsStager() = default;

  /** Destructor */
  ~NvidiaGdsStager() {}

  /** Build context for staging */
  static Context BuildContext(size_t page_size, u32 flags = 0,
                              size_t elmt_size = 1) {
    Context ctx;
    ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
    ctx.bkt_params_ = BuildFileParams(page_size, flags, elmt_size);
    return ctx;
  }

  /** Build serialized file parameter pack */
  static std::string BuildFileParams(size_t page_size, u32 flags = 0,
                                     size_t elmt_size = 1) {
    chi::string params(32);
    page_size = (page_size / elmt_size) * elmt_size;
    hipc::LocalSerialize srl(params);
    srl << std::string("nvidia_gds");
    srl << flags;
    srl << page_size;
    return params.str();
  }

  /** Initialize the stager */
  void RegisterStager(const hipc::MemContext &mctx, const std::string &tag_name,
                      const std::string &params) override {
    std::string protocol;
    hipc::LocalDeserialize srl(params);
    srl >> protocol;
    srl >> flags_.bits_;
    srl >> page_size_;
    path_ = tag_name;

    CUfileDescr_t cufile_descr = {};
    cufile_descr.handle.fd = open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    cufile_descr.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;

    if (cufile_descr.handle.fd < 0) {
      HELOG(kError, "Failed to open file: {}", path_);
      return;
    }

    CUfileError_t status = cuFileHandleRegister(&cufile_handle_, &cufile_descr);
    if (status.err != CU_FILE_SUCCESS) {
      HELOG(kError, "Failed to register file with cuFile: {}", path_);
      close(cufile_descr.handle.fd);
    } else {
      HILOG(kDebug, "Successfully registered file with cuFile: {}", path_);
    }
  }

  /** Stage data in from a remote source */
  void StageIn(const hipc::MemContext &mctx, hermes::Client &client,
               const TagId &tag_id, const std::string &blob_name,
               float score) override {
    if (flags_.Any(HERMES_STAGE_NO_READ)) {
      return;
    }

    adapter::BlobPlacement plcmnt;
    plcmnt.DecodeBlobName(blob_name, page_size_);

    void *gpu_ptr = nullptr;
    cudaError_t err = cudaMalloc(&gpu_ptr, page_size_);
    if (err != cudaSuccess) {
      HELOG(kError, "Failed to allocate GPU memory: {}",
            cudaGetErrorString(err));
      return;
    }

    ssize_t real_size =
        cuFileRead(cufile_handle_, gpu_ptr, page_size_, plcmnt.bucket_off_, 0);
    if (real_size < 0) {
      HELOG(kError, "Failed to read data using cuFile from: {}", path_);
      cudaFree(gpu_ptr);
      return;
    }
  }

  /** Stage data out to a remote source */
  void StageOut(const hipc::MemContext &mctx, hermes::Client &client,
                const TagId &tag_id, const std::string &blob_name,
                hipc::Pointer &data_p, size_t data_size) override {
    if (flags_.Any(HERMES_STAGE_NO_WRITE)) {
      return;
    }

    adapter::BlobPlacement plcmnt;
    plcmnt.DecodeBlobName(blob_name, page_size_);

    void *gpu_ptr = nullptr;
    cudaError_t err = cudaMalloc(&gpu_ptr, data_size);
    if (err != cudaSuccess) {
      HELOG(kError, "Failed to allocate GPU memory: {}",
            cudaGetErrorString(err));
      return;
    }

    ssize_t real_size =
        cuFileWrite(cufile_handle_, gpu_ptr, data_size, plcmnt.bucket_off_, 0);
    cudaFree(gpu_ptr);
    if (real_size < 0) {
      HELOG(kError, "Failed to write data using cuFile to: {}", path_);
      return;
    }
  }

  /** Update metadata size */
  void UpdateSize(const hipc::MemContext &mctx, hermes::Client &client,
                  const TagId &tag_id, const std::string &blob_name,
                  size_t blob_off, size_t data_size) override {
    adapter::BlobPlacement p;
    p.DecodeBlobName(blob_name, page_size_);
    HILOG(kDebug, "Updated size for blob {} with offset {} and size {}",
          blob_name, blob_off, data_size);
  }
};

}  // namespace hermes

#endif  // HERMES_TASKS_DATA_STAGER_SRC_NVIDIA_GDS_STAGER_H_
