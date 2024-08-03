//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_hermes_blob_mdm_H_
#define HRUN_hermes_blob_mdm_H_

#include "hermes_blob_mdm_tasks.h"

namespace hermes::blob_mdm {

/** Create hermes_blob_mdm requests */
class Client : public ModuleClient {

 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a hermes_blob_mdm */
  HSHM_ALWAYS_INLINE
      LPointer<ConstructTask> AsyncCreate(const TaskNode &task_node,
                                          const DomainId &domain_id,
                                          const std::string &state_name) {
    id_ = PoolId::GetNull();
    QueueManagerInfo &qm = CHI_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    return CHI_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info);
  }
  void AsyncCreateComplete(ConstructTask *task) {
    if (task->IsModuleComplete()) {
      id_ = task->id_;
      queue_id_ = QueueId(id_);
      CHI_CLIENT->DelTask(task);
    }
  }
  HRUN_TASK_NODE_ROOT(AsyncCreate);
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void Create(Args&& ...args) {
    LPointer<ConstructTask> task = AsyncCreate(std::forward<Args>(args)...);
    task->Wait();
    AsyncCreateComplete(task.ptr_);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void Destroy(const DomainId &domain_id) {
    CHI_ADMIN->DestroyTaskState(domain_id, id_);
  }

  /**====================================
   * Blob Operations
   * ===================================*/

  /** Sets the BUCKET MDM */
  void AsyncSetBucketMdmConstruct(SetBucketMdmTask *task,
                                  const TaskNode &task_node,
                                  const DomainId &domain_id,
                                  const PoolId &blob_mdm,
                                  const PoolId &stager_mdm,
                                  const PoolId &op_mdm) {
    CHI_CLIENT->ConstructTask<SetBucketMdmTask>(
        task, task_node, domain_id, id_, blob_mdm, stager_mdm, op_mdm);
  }
  void SetBucketMdm(const DomainId &domain_id,
                        const PoolId &blob_mdm,
                        const PoolId &stager_mdm,
                        const PoolId &op_mdm) {
    LPointer<SetBucketMdmTask> task =
                                                          AsyncSetBucketMdm(domain_id, blob_mdm, stager_mdm, op_mdm);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(SetBucketMdm);

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  void AsyncGetOrCreateBlobIdConstruct(GetOrCreateBlobIdTask* task,
                                       const TaskNode &task_node,
                                       TagId tag_id,
                                       const hshm::charbuf &blob_name) {
    u32 hash = HashBlobName(tag_id, blob_name);
    CHI_CLIENT->ConstructTask<GetOrCreateBlobIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_name);
  }
  BlobId GetOrCreateBlobId(TagId tag_id, const hshm::charbuf &blob_name) {
    LPointer<GetOrCreateBlobIdTask> task =
                                                               AsyncGetOrCreateBlobId(tag_id, blob_name);
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
  void AsyncPutBlobConstruct(
      PutBlobTask *task,
      const TaskNode &task_node,
      TagId tag_id, const hshm::charbuf &blob_name,
      const BlobId &blob_id, size_t blob_off, size_t blob_size,
      const hipc::Pointer &blob, float score,
      u32 flags,
      Context ctx = Context(),
      u32 task_flags = TASK_FIRE_AND_FORGET | TASK_DATA_OWNER | TASK_LOW_LATENCY) {
    CHI_CLIENT->ConstructTask<PutBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id,
        blob_off, blob_size,
        blob, score, flags, ctx, task_flags);
  }
  CHI_TASK_METHODS(PutBlob);

  /** Get a blob's data */
  void AsyncGetBlobConstruct(GetBlobTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id,
                             const hshm::charbuf &blob_name,
                             const BlobId &blob_id,
                             size_t off,
                             ssize_t data_size,
                             hipc::Pointer &data,
                             Context ctx = Context(),
                             u32 flags = 0) {
    // HILOG(kDebug, "Beginning GET (task_node={})", task_node);
    CHI_CLIENT->ConstructTask<GetBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id, off, data_size, data, ctx, flags);
  }
  size_t GetBlob(const TagId &tag_id,
                     const BlobId &blob_id,
                     size_t off,
                     ssize_t data_size,
                     hipc::Pointer &data,
                     Context ctx = Context(),
                     u32 flags = 0) {
    LPointer<GetBlobTask> task =
        AsyncGetBlob(tag_id, hshm::charbuf(""),
                         blob_id, off, data_size, data, ctx, flags);
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
  void AsyncReorganizeBlobConstruct(ReorganizeBlobTask *task,
                                    const TaskNode &task_node,
                                    const TagId &tag_id,
                                    const hshm::charbuf &blob_name,
                                    const BlobId &blob_id,
                                    float score,
                                    bool user_score,
                                    const Context &ctx = Context(),
                                    u32 task_flags = TASK_LOW_LATENCY | TASK_FIRE_AND_FORGET) {
    // HILOG(kDebug, "Beginning REORGANIZE (task_node={})", task_node);
    CHI_CLIENT->ConstructTask<ReorganizeBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id, score, user_score, ctx, task_flags);
  }
  CHI_TASK_METHODS(ReorganizeBlob);

  /**
   * Tag a blob
   *
   * @param blob_id id of the blob being tagged
   * @param tag_name tag name
   * */
  void AsyncTagBlobConstruct(TagBlobTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id,
                             const BlobId &blob_id,
                             const TagId &tag) {
    CHI_CLIENT->ConstructTask<TagBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, tag);
  }
  void TagBlob(const TagId &tag_id,
                   const BlobId &blob_id,
                   const TagId &tag) {
    LPointer<TagBlobTask> task =
                                                     AsyncTagBlob(tag_id, blob_id, tag);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TagBlob);

  /**
   * Check if blob has a tag
   * */
  void AsyncBlobHasTagConstruct(BlobHasTagTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id,
                                const TagId &tag) {
    CHI_CLIENT->ConstructTask<BlobHasTagTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, tag);
  }
  bool BlobHasTag(const TagId &tag_id,
                      const BlobId &blob_id,
                      const TagId &tag) {
    LPointer<BlobHasTagTask> task =
                                                        AsyncBlobHasTag(tag_id, blob_id, tag);
    task->Wait();
    bool has_tag = task->has_tag_;
    CHI_CLIENT->DelTask(task);
    return has_tag;
  }
  CHI_TASK_METHODS(BlobHasTag);

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  void AsyncGetBlobIdConstruct(GetBlobIdTask *task,
                               const TaskNode &task_node,
                               const TagId &tag_id,
                               const hshm::charbuf &blob_name) {
    u32 hash = HashBlobName(tag_id, blob_name);
    CHI_CLIENT->ConstructTask<GetBlobIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_name);
  }
  BlobId GetBlobId(const TagId &tag_id,
                       const hshm::charbuf &blob_name) {
    LPointer<GetBlobIdTask> task =
                                                       AsyncGetBlobId(tag_id, blob_name);
    task->Wait();
    BlobId blob_id = task->blob_id_;
    CHI_CLIENT->DelTask(task);
    return blob_id;
  }
  CHI_TASK_METHODS(GetBlobId);

  /**
   * Get \a blob_name BLOB name from \a blob_id BLOB id
   * */
  void AsyncGetBlobNameConstruct(GetBlobNameTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const BlobId &blob_id) {
    CHI_CLIENT->ConstructTask<GetBlobNameTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  std::string GetBlobName(const TagId &tag_id,
                              const BlobId &blob_id) {
    LPointer<GetBlobNameTask> task =
                                                         AsyncGetBlobName(tag_id, blob_id);
    task->Wait();
    std::string blob_name = task->blob_name_->str();
    CHI_CLIENT->DelTask(task);
    return blob_name;
  }
  CHI_TASK_METHODS(GetBlobName);

  /**
   * Get \a size from \a blob_id BLOB id
   * */
  void AsyncGetBlobSizeConstruct(GetBlobSizeTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const hshm::charbuf &blob_name,
                                 const BlobId &blob_id) {
    // HILOG(kDebug, "Getting blob size {}", task_node);
    CHI_CLIENT->ConstructTask<GetBlobSizeTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id);
  }
  size_t GetBlobSize(const TagId &tag_id,
                         const hshm::charbuf &blob_name,
                         const BlobId &blob_id) {
    LPointer<GetBlobSizeTask> task =
                                                         AsyncGetBlobSize(tag_id, blob_name, blob_id);
    task->Wait();
    size_t size = task->size_;
    CHI_CLIENT->DelTask(task);
    return size;
  }
  CHI_TASK_METHODS(GetBlobSize);

  /**
   * Get \a score from \a blob_id BLOB id
   * */
  void AsyncGetBlobScoreConstruct(GetBlobScoreTask *task,
                                  const TaskNode &task_node,
                                  const TagId &tag_id,
                                  const BlobId &blob_id) {
    CHI_CLIENT->ConstructTask<GetBlobScoreTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  float GetBlobScore(const TagId &tag_id,
                         const BlobId &blob_id) {
    LPointer<GetBlobScoreTask> task =
                                                          AsyncGetBlobScore(tag_id, blob_id);
    task->Wait();
    float score = task->score_;
    CHI_CLIENT->DelTask(task);
    return score;
  }
  CHI_TASK_METHODS(GetBlobScore);

  /**
   * Get \a blob_id blob's buffers
   * */
  void AsyncGetBlobBuffersConstruct(GetBlobBuffersTask *task,
                                    const TaskNode &task_node,
                                    const TagId &tag_id,
                                    const BlobId &blob_id) {
    CHI_CLIENT->ConstructTask<GetBlobBuffersTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  std::vector<BufferInfo> GetBlobBuffers(const TagId &tag_id,
                                             const BlobId &blob_id) {
    LPointer<GetBlobBuffersTask> task = AsyncGetBlobBuffers(tag_id, blob_id);
    task->Wait();
    std::vector<BufferInfo> buffers =
        hshm::to_stl_vector<BufferInfo>(*task->buffers_);
    CHI_CLIENT->DelTask(task);
    return buffers;
  }
  CHI_TASK_METHODS(GetBlobBuffers)

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   * */
  void AsyncRenameBlobConstruct(RenameBlobTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id,
                                const hshm::charbuf &new_blob_name) {
    CHI_CLIENT->ConstructTask<RenameBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, new_blob_name);
  }
  void RenameBlob(const TagId &tag_id,
                      const BlobId &blob_id,
                      const hshm::charbuf &new_blob_name) {
    LPointer<RenameBlobTask> task =
                                                        AsyncRenameBlob(tag_id, blob_id, new_blob_name);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(RenameBlob);

  /**
   * Truncate a blob to a new size
   * */
  void AsyncTruncateBlobConstruct(TruncateBlobTask *task,
                                  const TaskNode &task_node,
                                  const TagId &tag_id,
                                  const BlobId &blob_id,
                                  size_t new_size) {
    CHI_CLIENT->ConstructTask<TruncateBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, new_size);
  }
  void TruncateBlob(const TagId &tag_id,
                        const BlobId &blob_id,
                        size_t new_size) {
    LPointer<TruncateBlobTask> task =
                                                          AsyncTruncateBlob(tag_id, blob_id, new_size);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TruncateBlob);

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   * */
  void AsyncDestroyBlobConstruct(DestroyBlobTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const BlobId &blob_id,
                                 bool update_size = true) {
    CHI_CLIENT->ConstructTask<DestroyBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_),
        id_, tag_id, blob_id, update_size);
  }
  void DestroyBlob(const TagId &tag_id,
                       const BlobId &blob_id,
                       bool update_size = true) {
    LPointer<DestroyBlobTask> task =
                                                         AsyncDestroyBlob(tag_id, blob_id, update_size);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(DestroyBlob);

  /** Initialize automatic flushing */
  void AsyncFlushDataConstruct(FlushDataTask *task,
                               const TaskNode &task_node,
                               size_t period_ms) {
    CHI_CLIENT->ConstructTask<FlushDataTask>(
        task, task_node, id_, period_ms);
  }
  CHI_TASK_METHODS(FlushData);

  /**
   * Get all blob metadata
   * */
  void AsyncPollBlobMetadataConstruct(PollBlobMetadataTask *task,
                                      const TaskNode &task_node) {
    CHI_CLIENT->ConstructTask<PollBlobMetadataTask>(
        task, task_node, id_);
  }
  std::vector<BlobInfo> PollBlobMetadata() {
    LPointer<PollBlobMetadataTask> task =
                                                              AsyncPollBlobMetadata();
    task->Wait();
    std::vector<BlobInfo> blob_mdms =
        task->DeserializeBlobMetadata();
    CHI_CLIENT->DelTask(task);
    return blob_mdms;
  }
  CHI_TASK_METHODS(PollBlobMetadata);

  /**
  * Get all target metadata
  * */
  void AsyncPollTargetMetadataConstruct(PollTargetMetadataTask *task,
                                        const TaskNode &task_node) {
    CHI_CLIENT->ConstructTask<PollTargetMetadataTask>(
        task, task_node, id_);
  }
  std::vector<TargetStats> PollTargetMetadata() {
    LPointer<PollTargetMetadataTask> task =
                                                                AsyncPollTargetMetadata();
    task->Wait();
    std::vector<TargetStats> target_mdms =
        task->DeserializeTargetMetadata();
    CHI_CLIENT->DelTask(task);
    return target_mdms;
  }
  CHI_TASK_METHODS(PollTargetMetadata);
};

}  // namespace hrun

#endif  // HRUN_hermes_blob_mdm_H_