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

#ifndef CHI_hermes_core_H_
#define CHI_hermes_core_H_

#include "hermes_core_tasks.h"

namespace hermes {

/** Create hermes_core requests */
class Client : public ModuleClient {

 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a task state */
  void Create(const DomainQuery &dom_query,
              const DomainQuery &affinity,
              const std::string &pool_name,
              const CreateContext &ctx = CreateContext()) {
    LPointer<CreateTask> task = AsyncCreate(
        dom_query, affinity, pool_name, ctx);
    task->Wait();
    Init(task->ctx_.id_);
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(Create);

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void Destroy(const DomainQuery &dom_query) {
    CHI_ADMIN->DestroyContainer(dom_query, id_);
  }

  /**====================================
   * Tag Operations
   * ===================================*/

  /** Update statistics after blob PUT (fire & forget) */
  CHI_TASK_METHODS(TagUpdateSize);

  /** Create a tag or get the ID of existing tag */
  HSHM_ALWAYS_INLINE
  TagId GetOrCreateTag(const DomainQuery &dom_query,
                       const hshm::charbuf &tag_name,
                       bool blob_owner,
                       size_t backend_size,
                       u32 flags,
                       const Context &ctx = Context()) {
    LPointer<GetOrCreateTagTask> task =
        AsyncGetOrCreateTag(dom_query, tag_name, blob_owner,
                            backend_size, flags, ctx);
    task->Wait();
    TagId tag_id = task->tag_id_;
    CHI_CLIENT->DelTask(task);
    return tag_id;
  }
  CHI_TASK_METHODS(GetOrCreateTag);

  /** Get tag ID */
  TagId GetTagId(const DomainQuery &dom_query,
                 const hshm::charbuf &tag_name) {
    LPointer<GetTagIdTask> task =
        AsyncGetTagId(dom_query, tag_name);
    task->Wait();
    TagId tag_id = task->tag_id_;
    CHI_CLIENT->DelTask(task);
    return tag_id;
  }
  CHI_TASK_METHODS(GetTagId);

  /** Get tag name */
  hshm::string GetTagName(const DomainQuery &dom_query,
                          const TagId &tag_id) {
    LPointer<GetTagNameTask> task =
        AsyncGetTagName(dom_query, tag_id);
    task->Wait();
    hshm::string tag_name = hshm::to_charbuf<hipc::string>(task->tag_name_);
    CHI_CLIENT->DelTask(task);
    return tag_name;
  }
  CHI_TASK_METHODS(GetTagName);

  /** Destroy tag */
  void DestroyTag(const DomainQuery &dom_query, const TagId &tag_id) {
    LPointer<DestroyTagTask> task =
        AsyncDestroyTag(dom_query, tag_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(DestroyTag);

  /** Add a blob to a tag */
  void TagAddBlob(const DomainQuery &dom_query,
                  const TagId &tag_id,
                  const BlobId &blob_id) {
    LPointer<TagAddBlobTask> task =
        AsyncTagAddBlob(dom_query, tag_id, blob_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TagAddBlob);

  /** Remove a blob from a tag */
  void TagRemoveBlob(const DomainQuery &dom_query,
                     const TagId &tag_id,
                     const BlobId &blob_id) {
    LPointer<TagRemoveBlobTask> task =
        AsyncTagRemoveBlob(dom_query, tag_id, blob_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TagRemoveBlob);

  /** Clear blobs from a tag */
  void TagClearBlobs(const DomainQuery &dom_query,
                     const TagId &tag_id) {
    LPointer<TagClearBlobsTask> task =
        AsyncTagClearBlobs(dom_query, tag_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TagClearBlobs);

  /** Get the size of a bucket */
  size_t GetSize(const DomainQuery &dom_query,
                 const TagId &tag_id) {
    LPointer<TagGetSizeTask> task =
        AsyncTagGetSize(dom_query, tag_id);
    task->Wait();
    size_t size = task->size_;
    CHI_CLIENT->DelTask(task);
    return size;
  }
  CHI_TASK_METHODS(TagGetSize);

  /** Get contained blob ids */
  std::vector<BlobId> TagGetContainedBlobIds(const DomainQuery &dom_query,
                                             const TagId &tag_id) {
    LPointer<TagGetContainedBlobIdsTask> task =
        AsyncTagGetContainedBlobIds(dom_query, tag_id);
    task->Wait();
    std::vector<BlobId> blob_ids = task->blob_ids_.vec();
    CHI_CLIENT->DelTask(task);
    return blob_ids;
  }
  CHI_TASK_METHODS(TagGetContainedBlobIds);

  /**====================================
   * Blob Operations
   * ===================================*/

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  BlobId GetOrCreateBlob(const DomainQuery &dom_query,
                           const TagId &tag_id,
                           const hshm::charbuf &blob_name) {
    LPointer<GetOrCreateBlobIdTask> task =
        AsyncGetOrCreateBlobId(dom_query, tag_id, blob_name);
    task->Wait();
    BlobId blob_id = task->blob_id_;
    CHI_CLIENT->DelTask(task);
    return blob_id;
  }
  CHI_TASK_METHODS(GetOrCreateBlobId);

  /**
  * Create a blob's metadata
  *
  * @param tag_id id of the bucket
  * @param blob_name semantic blob name
  * @param blob_id the id of the blob
  * @param blob_off the offset of the data placed in existing blob
  * @param blob_size the amount of data being placed
  * @param blob a SHM pointer to the data to place
  * @param score the current score of the blob
  * @param replace whether to replace the blob if it exists
  * @param[OUT] did_create whether the blob was created or not
  * */
  size_t PutBlob(const DomainQuery &dom_query,
                 TagId tag_id,
                 const hshm::charbuf &blob_name,
                 const BlobId &blob_id,
                 size_t blob_off, size_t blob_size,
                 const hipc::Pointer &blob,
                 float score,
                 u32 task_flags,
                 u32 hermes_flags,
                 Context ctx = Context()) {
    LPointer<PutBlobTask> task =
        AsyncPutBlob(dom_query,
                     tag_id, blob_name, blob_id,
                     blob_off, blob_size,
                     blob, score, task_flags, hermes_flags, ctx);
    task->Wait();
    size_t true_size = task->data_size_;
    CHI_CLIENT->DelTask(task);
    return true_size;
  }
  CHI_TASK_METHODS(PutBlob);

  /** Get a blob's data */
  size_t GetBlob(const DomainQuery &dom_query,
                 const TagId &tag_id,
                 const BlobId &blob_id,
                 size_t off,
                 ssize_t data_size,
                 hipc::Pointer &data,
                 u32 hermes_flags,
                 const Context &ctx = Context()) {
    LPointer<GetBlobTask> task =
        AsyncGetBlob(dom_query, tag_id, hshm::charbuf(""),
                     blob_id, off, data_size, data, hermes_flags, ctx);
    task->Wait();
    data = task->data_;
    size_t true_size = task->data_size_;
    CHI_CLIENT->DelTask(task);
    return true_size;
  }
  CHI_TASK_METHODS(GetBlob);

  /**
   * Reorganize a blob
   *
   * @param blob_id id of the blob being reorganized
   * @param score the new score of the blob
   * @param node_id the node to reorganize the blob to
   * */
  CHI_TASK_METHODS(ReorganizeBlob);

  /**
 * Tag a blob
 *
 * @param blob_id id of the blob being tagged
 * @param tag_name tag name
 * */
  void TagBlob(const DomainQuery &dom_query,
               const TagId &tag_id,
               const BlobId &blob_id,
               const TagId &tag) {
    LPointer<TagBlobTask> task =
        AsyncTagBlob(dom_query, tag_id, blob_id, tag);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TagBlob);

  /**
   * Check if blob has a tag
   * */
  bool BlobHasTag(const DomainQuery &dom_query,
                  const TagId &tag_id,
                  const BlobId &blob_id,
                  const TagId &tag) {
    LPointer<BlobHasTagTask> task =
        AsyncBlobHasTag(dom_query, tag_id, blob_id, tag);
    task->Wait();
    bool has_tag = task->has_tag_;
    CHI_CLIENT->DelTask(task);
    return has_tag;
  }
  CHI_TASK_METHODS(BlobHasTag);

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  BlobId GetBlobId(const DomainQuery &dom_query,
                   const TagId &tag_id,
                   const hshm::charbuf &blob_name) {
    LPointer<GetBlobIdTask> task =
        AsyncGetBlobId(dom_query, tag_id, blob_name);
    task->Wait();
    BlobId blob_id = task->blob_id_;
    CHI_CLIENT->DelTask(task);
    return blob_id;
  }
  CHI_TASK_METHODS(GetBlobId);

  /**
   * Get \a blob_name BLOB name from \a blob_id BLOB id
   * */
  std::string GetBlobName(const DomainQuery &dom_query,
                          const TagId &tag_id,
                          const BlobId &blob_id) {
    LPointer<GetBlobNameTask> task =
        AsyncGetBlobName(dom_query, tag_id, blob_id);
    task->Wait();
    std::string blob_name = task->blob_name_.str();
    CHI_CLIENT->DelTask(task);
    return blob_name;
  }
  CHI_TASK_METHODS(GetBlobName);

  /**
   * Get \a size from \a blob_id BLOB id
   * */
  size_t GetBlobSize(const DomainQuery &dom_query,
                     const TagId &tag_id,
                     const hshm::charbuf &blob_name,
                     const BlobId &blob_id) {
    LPointer<GetBlobSizeTask> task =
        AsyncGetBlobSize(dom_query, tag_id, blob_name, blob_id);
    task->Wait();
    size_t size = task->size_;
    CHI_CLIENT->DelTask(task);
    return size;
  }
  CHI_TASK_METHODS(GetBlobSize);

  /**
   * Get \a score from \a blob_id BLOB id
   * */
  float GetBlobScore(const DomainQuery &dom_query,
                     const TagId &tag_id,
                     const BlobId &blob_id) {
    LPointer<GetBlobScoreTask> task =
        AsyncGetBlobScore(dom_query, tag_id, blob_id);
    task->Wait();
    float score = task->score_;
    CHI_CLIENT->DelTask(task);
    return score;
  }
  CHI_TASK_METHODS(GetBlobScore);

  /**
   * Get \a blob_id blob's buffers
   * */
  std::vector<BufferInfo> GetBlobBuffers(const DomainQuery &dom_query,
                                         const TagId &tag_id,
                                         const BlobId &blob_id) {
    LPointer<GetBlobBuffersTask> task =
        AsyncGetBlobBuffers(dom_query, tag_id, blob_id);
    task->Wait();
    std::vector<BufferInfo> buffers =
        hshm::to_stl_vector<BufferInfo>(task->buffers_);
    CHI_CLIENT->DelTask(task);
    return buffers;
  }
  CHI_TASK_METHODS(GetBlobBuffers)

  /**
   * Truncate a blob to a new size
   * */
  void TruncateBlob(const DomainQuery &dom_query,
                    const TagId &tag_id,
                    const BlobId &blob_id,
                    size_t new_size) {
    LPointer<TruncateBlobTask> task =
        AsyncTruncateBlob(dom_query, tag_id, blob_id, new_size);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TruncateBlob);

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   * */
  void DestroyBlob(const DomainQuery &dom_query,
                   const TagId &tag_id,
                   const BlobId &blob_id,
                   u32 blob_flags = 0) {
    LPointer<DestroyBlobTask> task =
        AsyncDestroyBlob(dom_query, tag_id, blob_id, blob_flags);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(DestroyBlob);

  /**
   * ========================================
   * STAGING Tasks
   * ========================================
   * */

  /** RegisterStager task */
  void RegisterStager(const DomainQuery &dom_query,
                      const hermes::BucketId &bkt_id,
                      const hshm::charbuf &tag_name,
                      const hshm::charbuf &params) {
    LPointer<RegisterStagerTask> task =
        AsyncRegisterStager(dom_query, bkt_id, tag_name, params);
    task->Wait();
    CHI_CLIENT->DelTask(task);
    return;
  }
  CHI_TASK_METHODS(RegisterStager);


  /** UnregisterStager task */
  void UnregisterStager(const DomainQuery &dom_query,
                        const BucketId &bkt_id) {
    LPointer<UnregisterStagerTask> task =
        AsyncUnregisterStager(dom_query, bkt_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
    return;
  }
  CHI_TASK_METHODS(UnregisterStager);


  /** StageIn task */
  void StageIn(const DomainQuery &dom_query,
               const BucketId &bkt_id,
               const hshm::charbuf &blob_name,
               float score) {
    LPointer<StageInTask> task =
        AsyncStageIn(dom_query, bkt_id, blob_name, score);
    task->Wait();
    CHI_CLIENT->DelTask(task);
    return;
  }
  CHI_TASK_METHODS(StageIn);


  /** StageOut task */
  void StageOut(const DomainQuery &dom_query,
                const BucketId &bkt_id,
                const hshm::charbuf &blob_name,
                const hipc::Pointer &data,
                size_t data_size,
                u32 task_flags) {
    LPointer<StageOutTask> task =
        AsyncStageOut(dom_query, bkt_id, blob_name, data, data_size, task_flags);
    task->Wait();
    CHI_CLIENT->DelTask(task);
    return;
  }
  CHI_TASK_METHODS(StageOut);
};

}  // namespace chi

#endif  // CHI_hermes_core_H_
