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

#ifndef HRUN_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_
#define HRUN_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_

#include "hermes/config_manager.h"
#include "hermes/hermes_types.h"
#include "hermes_core/hermes_core.h"

namespace hermes {

class Bucket {
 public:
  hermes::Client *mdm_;
  TagId id_;
  std::string name_;
  Context ctx_;
  hipc::MemContext mctx_;
  bitfield32_t flags_;

 public:
  /**====================================
   * Bucket Operations
   * ===================================*/

  /**
   * Get or create \a bkt_name bucket.
   *
   * Called from hermes.h in GetBucket(). Should not
   * be used directly.
   * */
  explicit Bucket(const hipc::MemContext &mctx, const std::string &bkt_name,
                  size_t backend_size = 0, u32 flags = 0) {
    mctx_ = mctx;
    mdm_ = &HERMES_CONF->mdm_;
    id_ = mdm_->GetOrCreateTag(
        mctx_,
        chi::DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0),
        chi::string(bkt_name), true, backend_size, flags);
    name_ = bkt_name;
  }

  /**
   * Get or create \a bkt_name bucket.
   *
   * Called from hermes.h in GetBucket(). Should not
   * be used directly.
   * */
  explicit Bucket(const hipc::MemContext &mctx, const std::string &bkt_name,
                  Context &ctx, size_t backend_size = 0, u32 flags = 0) {
    mctx_ = mctx;
    mdm_ = &HERMES_CONF->mdm_;
    chi::string x;
    id_ = mdm_->GetOrCreateTag(
        mctx_,
        chi::DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0),
        chi::string(bkt_name), true, backend_size, flags, ctx);
    name_ = bkt_name;
  }

  /**
   * Get an existing bucket.
   * */
  explicit Bucket(TagId tag_id) {
    id_ = tag_id;
    mdm_ = &HERMES_CONF->mdm_;
  }

  /** Default constructor */
  Bucket() = default;

  /** Default copy constructor */
  Bucket(const Bucket &other) = default;

  /** Default copy assign */
  Bucket &operator=(const Bucket &other) = default;

  /** Default move constructor */
  Bucket(Bucket &&other) = default;

  /** Default move assign */
  Bucket &operator=(Bucket &&other) = default;

 public:
  /**
   * Get the name of this bucket. Name is cached instead of
   * making an RPC. Not coherent if Rename is called.
   * */
  const std::string &GetName() const { return name_; }

  /**
   * Get the identifier of this bucket
   * */
  TagId GetId() const { return id_; }

  /**
   * Get the context object of this bucket
   * */
  Context &GetContext() { return ctx_; }

  /**
   * Attach a trait to the bucket
   * */
  void AttachTrait(TraitId trait_id) {
    // TODO(llogan)
  }

  /**
   * Get the current size of the bucket
   * */
  size_t GetSize() {
    return mdm_->GetSize(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_);
  }

  /**
   * Set the current size of the bucket
   * */
  void SetSize(size_t new_size) {
    // mdm_->AsyncUpdateSize(id_, new_size, UpdateSizeMode::kCap);
  }

  /**
   * Rename this bucket
   * */
  void Rename(const std::string &new_bkt_name) {
    // mdm_->RenameTag(id_, chi::string(new_bkt_name));
  }

  /**
   * Clears the buckets contents, but doesn't destroy its metadata
   * */
  void Clear() {
    mdm_->TagClearBlobs(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_);
  }

  /**
   * Destroys this bucket along with all its contents.
   * */
  void Destroy() {
    mdm_->DestroyTag(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_);
  }

  /**
   * Check if this bucket is valid
   * */
  bool IsNull() { return id_.IsNull(); }

 public:
  /**====================================
   * Blob Operations
   * ===================================*/

  /**
   * Get the id of a blob from the blob name
   *
   * @param blob_name the name of the blob
   * @param blob_id (output) the returned blob_id
   * @return
   * */
  BlobId GetBlobId(const std::string &blob_name) {
    return mdm_->GetBlobId(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        chi::string(blob_name));
  }

  /**
   * Get the name of a blob from the blob id
   *
   * @param blob_id the blob_id
   * @param blob_name the name of the blob
   * @return The Status of the operation
   * */
  std::string GetBlobName(const BlobId &blob_id) {
    return mdm_->GetBlobName(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        blob_id);
  }

  /**
   * Get the score of a blob from the blob id
   *
   * @param blob_id the blob_id
   * @return The Status of the operation
   * */
  float GetBlobScore(const BlobId &blob_id) {
    return mdm_->GetBlobScore(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        blob_id);
  }

  /**
   * Label \a blob_id blob with \a tag_name TAG
   * */
  Status TagBlob(BlobId &blob_id, TagId &tag_id) {
    mdm_->TagAddBlob(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0),
        tag_id, blob_id);
    return Status();
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template <bool PARTIAL, bool ASYNC>
  HSHM_INLINE BlobId ShmBasePut(const std::string &blob_name,
                                const BlobId &orig_blob_id,
                                const FullPtr<char> &blob, size_t blob_off,
                                size_t blob_size, bitfield32_t task_flags,
                                Context &ctx) {
    BlobId blob_id = orig_blob_id;
    bitfield32_t hermes_flags;
    // Put to shared memory
    chi::string blob_name_buf(blob_name);
    if constexpr (!ASYNC) {
      if (blob_id.IsNull()) {
        hermes_flags.SetBits(HERMES_GET_BLOB_ID);
        task_flags.UnsetBits(TASK_FIRE_AND_FORGET);
      }
    } else {
      task_flags.SetBits(TASK_FIRE_AND_FORGET);
    }
    if constexpr (!PARTIAL) {
      hermes_flags.SetBits(HERMES_BLOB_REPLACE);
    }
    FullPtr<PutBlobTask> task;
    task = mdm_->AsyncPutBlob(
        mctx_,
        chi::DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0),
        id_, blob_name_buf, blob_id, blob_off, blob_size, blob.shm_,
        ctx.blob_score_, task_flags.bits_, hermes_flags.bits_, ctx);
    if constexpr (!ASYNC) {
      if (hermes_flags.Any(HERMES_GET_BLOB_ID)) {
        task->Wait();
        blob_id = task->blob_id_;
        CHI_CLIENT->DelTask(HSHM_DEFAULT_MEM_CTX, task);
      }
    }
    return blob_id;
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template <bool PARTIAL, bool ASYNC, typename BlobT = Blob>
  BlobId PrivateMemBasePut(const std::string &blob_name,
                           const BlobId &orig_blob_id, const BlobT &blob,
                           size_t blob_off, Context &ctx) {
    FullPtr<char> blob_p = CHI_CLIENT->AllocateBuffer(mctx_, blob.size());
    memcpy(blob_p.ptr_, blob.data(), blob.size());
    return ShmBasePut<PARTIAL, ASYNC>(blob_name, orig_blob_id, blob_p, blob_off,
                                      blob.size(),
                                      bitfield32_t(TASK_DATA_OWNER), ctx);
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template <typename T, bool PARTIAL, bool ASYNC>
  HSHM_INLINE BlobId SrlBasePut(const std::string &blob_name,
                                const BlobId &orig_blob_id, const T &data,
                                Context &ctx) {
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar << data;
    std::string blob = ss.str();
    return PrivateMemBasePut<PARTIAL, ASYNC, std::string>(
        blob_name, orig_blob_id, blob, 0, ctx);
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template <typename T = Blob>
  BlobId Put(const std::string &blob_name, const T &blob, Context &ctx) {
    if constexpr (std::is_same_v<T, Blob>) {
      return PrivateMemBasePut<false, false>(blob_name, BlobId::GetNull(), blob,
                                             0, ctx);
    } else {
      return SrlBasePut<T, false, false>(blob_name, BlobId::GetNull(), blob,
                                         ctx);
    }
  }

  /**
   * Put \a blob_id Blob into the bucket
   * */
  template <typename T = Blob>
  BlobId Put(const BlobId &blob_id, const T &blob, Context &ctx) {
    if constexpr (std::is_same_v<T, Blob>) {
      return PrivateMemBasePut<false, false>("", blob_id, blob, 0, ctx);
    } else {
      return SrlBasePut<T, false, false>("", blob_id, blob, ctx);
    }
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template <typename T = Blob>
  HSHM_INLINE void AsyncPut(const std::string &blob_name, const Blob &blob,
                            Context &ctx) {
    if constexpr (std::is_same_v<T, Blob>) {
      PrivateMemBasePut<false, true>(blob_name, BlobId::GetNull(), blob, 0,
                                     ctx);
    } else {
      SrlBasePut<T, false, true>(blob_name, BlobId::GetNull(), blob, ctx);
    }
  }

  /**
   * Put \a blob_id Blob into the bucket
   * */
  template <typename T>
  HSHM_INLINE void AsyncPut(const BlobId &blob_id, const Blob &blob,
                            Context &ctx) {
    if constexpr (std::is_same_v<T, Blob>) {
      PrivateMemBasePut<false, true>("", blob_id, blob, 0, ctx);
    } else {
      SrlBasePut<T, false, true>("", blob_id, blob, ctx);
    }
  }

  /**
   * PartialPut \a blob_name Blob into the bucket
   * */
  BlobId PartialPut(const std::string &blob_name, const Blob &blob,
                    size_t blob_off, Context &ctx) {
    return PrivateMemBasePut<true, false>(blob_name, BlobId::GetNull(), blob,
                                          blob_off, ctx);
  }

  /**
   * PartialPut \a blob_id Blob into the bucket
   * */
  BlobId PartialPut(const BlobId &blob_id, const Blob &blob, size_t blob_off,
                    Context &ctx) {
    return PrivateMemBasePut<true, false>("", blob_id, blob, blob_off, ctx);
  }

  /**
   * AsyncPartialPut \a blob_name Blob into the bucket
   * */
  void AsyncPartialPut(const std::string &blob_name, const Blob &blob,
                       size_t blob_off, Context &ctx) {
    PrivateMemBasePut<true, true>(blob_name, BlobId::GetNull(), blob, blob_off,
                                  ctx);
  }

  /**
   * AsyncPartialPut \a blob_id Blob into the bucket
   * */
  void AsyncPartialPut(const BlobId &blob_id, const Blob &blob, size_t blob_off,
                       Context &ctx) {
    PrivateMemBasePut<true, true>("", blob_id, blob, blob_off, ctx);
  }

  /**
   * Append \a blob_name Blob into the bucket (fully asynchronous)
   * */
  void Append(const Blob &blob, size_t page_size, Context &ctx) {
    //    FullPtr<char> p = CHI_CLIENT->AllocateBufferClient(blob.size());
    //    char *data = p.ptr_;
    //    memcpy(data, blob.data(), blob.size());
    //    mdm_->AppendBlob(
    //        id_, blob.size(), p.shm_, page_size,
    //        ctx.blob_score_, ctx.node_id_, ctx);
  }

  /**
   * Reorganize a blob to a new score or node
   * */
  void ReorganizeBlob(const std::string &name, float score,
                      const Context &ctx = Context()) {
    mdm_->AsyncReorganizeBlob(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        chi::string(name), BlobId::GetNull(), score, true, ctx);
  }

  /**
   * Reorganize a blob to a new score or node
   * */
  void ReorganizeBlob(const BlobId &blob_id, float score,
                      const Context &ctx = Context()) {
    mdm_->AsyncReorganizeBlob(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        chi::string(""), blob_id, score, true, ctx);
  }

  /**
   * Reorganize a blob to a new score or node
   *
   * @depricated
   * */
  void ReorganizeBlob(const BlobId &blob_id, float score, u32 node_id,
                      Context &ctx) {
    ctx.node_id_ = node_id;
    mdm_->AsyncReorganizeBlob(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        chi::string(""), blob_id, score, true, ctx);
  }

  /**
   * Get the current size of the blob in the bucket
   * */
  size_t GetBlobSize(const BlobId &blob_id) {
    return mdm_->GetBlobSize(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        chi::string(""), blob_id);
  }

  /**
   * Get the current size of the blob in the bucket
   * */
  size_t GetBlobSize(const std::string &name) {
    return mdm_->GetBlobSize(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        chi::string(name), BlobId::GetNull());
  }

  /**
   * Get \a blob_id Blob from the bucket (async)
   * */
  FullPtr<GetBlobTask> HSHM_INLINE ShmAsyncBaseGet(
      const std::string &blob_name, const BlobId &blob_id, FullPtr<char> &blob,
      size_t blob_off, size_t blob_size, Context &ctx) {
    bitfield32_t hermes_flags;
    // Get the blob ID
    if (blob_id.IsNull()) {
      hermes_flags.SetBits(HERMES_GET_BLOB_ID);
    }
    // Get from shared memory
    FullPtr<GetBlobTask> task;
    task = mdm_->AsyncGetBlob(
        mctx_,
        chi::DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0),
        id_, chi::string(blob_name), blob_id, blob_off, blob_size, blob.shm_,
        hermes_flags.bits_, ctx);
    return task;
  }

  /**
   * Get \a blob_id Blob from the bucket (async)
   * */
  FullPtr<GetBlobTask> HSHM_INLINE
  PrivateMemAsyncBaseGet(const std::string &blob_name, const BlobId &blob_id,
                         Blob &blob, size_t blob_off, Context &ctx) {
    bitfield32_t hermes_flags;
    // Get the blob ID
    if (blob_id.IsNull()) {
      hermes_flags.SetBits(HERMES_GET_BLOB_ID);
    }
    // Get from shared memory
    size_t data_size = blob.size();
    FullPtr<char> data_p = CHI_CLIENT->AllocateBuffer(mctx_, blob.size());
    return ShmAsyncBaseGet(blob_name, blob_id, data_p, blob_off, data_size,
                           ctx);
  }

  /**
   * Get \a blob_id Blob from the bucket (sync)
   * */
  BlobId ShmBaseGet(const std::string &blob_name, const BlobId &orig_blob_id,
                    FullPtr<char> &blob, size_t blob_off, size_t blob_size,
                    Context &ctx) {
    HILOG(kDebug, "Getting blob of size {}", blob_size);
    BlobId blob_id;
    FullPtr<GetBlobTask> task;
    task = ShmAsyncBaseGet(blob_name, orig_blob_id, blob, blob_off, blob_size,
                           ctx);
    task->Wait();
    blob_id = task->blob_id_;
    CHI_CLIENT->DelTask(HSHM_DEFAULT_MEM_CTX, task);
    return blob_id;
  }

  /**
   * Get \a blob_id Blob from the bucket (sync)
   * */
  BlobId PrivateMemBaseGet(const std::string &blob_name,
                           const BlobId &orig_blob_id, Blob &blob,
                           size_t blob_off, Context &ctx) {
    // TODO(llogan): intercept mmap to avoid copy
    size_t data_size = blob.size();
    if (blob.size() == 0) {
      data_size = mdm_->GetBlobSize(
          mctx_,
          DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0),
          id_, chi::string(blob_name), orig_blob_id);
      blob.resize(data_size);
    }
    HILOG(kDebug, "Getting blob of size {}", data_size);
    BlobId blob_id;
    FullPtr<GetBlobTask> task;
    task = PrivateMemAsyncBaseGet(blob_name, orig_blob_id, blob, blob_off, ctx);
    task->Wait();
    blob_id = task->blob_id_;
    char *data = CHI_CLIENT->GetDataPointer(task->data_);
    memcpy(blob.data(), data, task->data_size_);
    blob.resize(task->data_size_);
    CHI_CLIENT->FreeBuffer(task->data_);
    CHI_CLIENT->DelTask(HSHM_DEFAULT_MEM_CTX, task);
    return blob_id;
  }

  /**
   * Get \a blob_id Blob from the bucket (sync)
   * */
  template <typename T>
  BlobId SrlBaseGet(const std::string &blob_name, const BlobId &orig_blob_id,
                    T &data, Context &ctx) {
    Blob blob;
    BlobId blob_id = PrivateMemBaseGet(blob_name, orig_blob_id, blob, 0, ctx);
    if (blob.size() == 0) {
      return BlobId::GetNull();
    }
    std::stringstream ss(std::string(blob.data(), blob.size()));
    cereal::BinaryInputArchive ar(ss);
    ar >> data;
    return blob_id;
  }

  /**
   * Get \a blob_id Blob from the bucket
   * */
  template <typename T>
  BlobId Get(const std::string &blob_name, T &blob, Context &ctx) {
    if constexpr (std::is_same_v<T, Blob>) {
      return PrivateMemBaseGet(blob_name, BlobId::GetNull(), blob, 0, ctx);
    } else {
      return SrlBaseGet<T>(blob_name, BlobId::GetNull(), blob, ctx);
    }
  }

  /**
   * Get \a blob_id Blob from the bucket
   * */
  template <typename T>
  BlobId Get(const BlobId &blob_id, T &blob, Context &ctx) {
    if constexpr (std::is_same_v<T, Blob>) {
      return PrivateMemBaseGet("", blob_id, blob, 0, ctx);
    } else {
      return SrlBaseGet<T>("", blob_id, blob, ctx);
    }
  }

  /**
   * AsyncGet \a blob_name Blob from the bucket
   * */
  FullPtr<GetBlobTask> AsyncGet(const std::string &blob_name, Blob &blob,
                                Context &ctx) {
    return PrivateMemAsyncBaseGet(blob_name, BlobId::GetNull(), blob, 0, ctx);
  }

  /**
   * AsyncGet \a blob_id Blob from the bucket
   * */
  FullPtr<GetBlobTask> AsyncGet(const BlobId &blob_id, Blob &blob,
                                Context &ctx) {
    return PrivateMemAsyncBaseGet("", blob_id, blob, 0, ctx);
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  BlobId PartialGet(const std::string &blob_name, Blob &blob, size_t blob_off,
                    Context &ctx) {
    return PrivateMemBaseGet(blob_name, BlobId::GetNull(), blob, blob_off, ctx);
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  BlobId PartialGet(const BlobId &blob_id, Blob &blob, size_t blob_off,
                    Context &ctx) {
    return PrivateMemBaseGet("", blob_id, blob, blob_off, ctx);
  }

  /**
   * AsyncGet \a blob_name Blob from the bucket
   * */
  FullPtr<GetBlobTask> AsyncPartialGet(const std::string &blob_name, Blob &blob,
                                       size_t blob_off, Context &ctx) {
    return PrivateMemAsyncBaseGet(blob_name, BlobId::GetNull(), blob, blob_off,
                                  ctx);
  }

  /**
   * AsyncGet \a blob_id Blob from the bucket
   * */
  FullPtr<GetBlobTask> AsyncPartialGet(const BlobId &blob_id, Blob &blob,
                                       size_t blob_off, Context &ctx) {
    return PrivateMemAsyncBaseGet("", blob_id, blob, blob_off, ctx);
  }

  /**
   * Determine if the bucket contains \a blob_id BLOB
   * */
  bool ContainsBlob(const std::string &blob_name) {
    BlobId new_blob_id = mdm_->GetBlobId(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        chi::string(blob_name));
    return !new_blob_id.IsNull();
  }

  /**
   * Rename \a blob_id blob to \a new_blob_name new name
   * */
  void RenameBlob(const BlobId &blob_id, std::string new_blob_name,
                  Context &ctx) {
    // mdm_->RenameBlob(id_, blob_id, chi::string(new_blob_name));
  }

  /**
   * Delete \a blob_id blob
   * */
  void DestroyBlob(const BlobId &blob_id, Context &ctx) {
    mdm_->DestroyBlob(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_,
        blob_id);
  }

  /**
   * Get the set of blob IDs contained in the bucket
   * */
  std::vector<BlobId> GetContainedBlobIds() {
    return mdm_->TagGetContainedBlobIds(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_);
  }

  /** Flush the bucket */
  void Flush() {
    mdm_->TagFlush(
        mctx_,
        DomainQuery::GetDirectHash(chi::SubDomainId::kLocalContainers, 0), id_);
  }
};

}  // namespace hermes

#endif  // HRUN_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_
