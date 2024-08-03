//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_hermes_bucket_mdm_H_
#define HRUN_hermes_bucket_mdm_H_

#include "hermes_bucket_mdm_tasks.h"

namespace hermes::bucket_mdm {

/** Create hermes_bucket_mdm requests */
class Client : public ModuleClient {
 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a hermes_bucket_mdm */
  HSHM_ALWAYS_INLINE
  void Create(const DomainId &domain_id,
                  const std::string &state_name) {
    id_ = PoolId::GetNull();
    QueueManagerInfo &qm = CHI_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    id_ = CHI_ADMIN->CreateTaskState<ConstructTask>(
        domain_id, state_name, id_, queue_info);
    Init(id_, CHI_ADMIN->queue_id_);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void Destroy(const DomainId &domain_id) {
    CHI_ADMIN->DestroyTaskState(domain_id, id_);
  }

  /**====================================
   * Tag Operations
   * ===================================*/

  /** Sets the BLOB MDM */
  void AsyncSetBlobMdmConstruct(SetBlobMdmTask *task,
                                const TaskNode &task_node,
                                const DomainId &domain_id,
                                const PoolId &blob_mdm,
                                const PoolId &stager_mdm) {
    CHI_CLIENT->ConstructTask<SetBlobMdmTask>(
        task, task_node, domain_id, id_, blob_mdm, stager_mdm);
  }
  void SetBlobMdm(const DomainId &domain_id,
                      const PoolId &blob_mdm,
                      const PoolId &stager_mdm) {
    LPointer<SetBlobMdmTask> task =
        AsyncSetBlobMdm(domain_id, blob_mdm, stager_mdm);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(SetBlobMdm);

  /** Update statistics after blob PUT (fire & forget) */
  HSHM_ALWAYS_INLINE
  void AsyncUpdateSizeConstruct(UpdateSizeTask *task,
                                const TaskNode &task_node,
                                TagId tag_id,
                                ssize_t update,
                                int mode) {
    CHI_CLIENT->ConstructTask<UpdateSizeTask>(
        task, task_node, DomainId::GetNode(tag_id.node_id_), id_,
        tag_id, update, mode);
  }
  CHI_TASK_METHODS(UpdateSize);

  /** Append data to the bucket (fire & forget) */
  HSHM_ALWAYS_INLINE
  void AsyncAppendBlobSchemaConstruct(AppendBlobSchemaTask *task,
                                      const TaskNode &task_node,
                                      TagId tag_id,
                                      size_t data_size,
                                      size_t page_size) {
    CHI_CLIENT->ConstructTask<AppendBlobSchemaTask>(
        task, task_node, DomainId::GetNode(tag_id.node_id_), id_,
        tag_id, data_size, page_size);
  }
  CHI_TASK_METHODS(AppendBlobSchema);

  /** Append data to the bucket (fire & forget) */
  HSHM_ALWAYS_INLINE
  void AsyncAppendBlobConstruct(
      AppendBlobTask *task,
      const TaskNode &task_node,
      TagId tag_id,
      size_t data_size,
      const hipc::Pointer &data,
      size_t page_size,
      float score,
      u32 node_id,
      const Context &ctx) {
    CHI_CLIENT->ConstructTask<AppendBlobTask>(
        task, task_node, DomainId::GetLocal(), id_,
        tag_id, data_size, data, page_size, score, node_id, ctx);
  }
  HSHM_ALWAYS_INLINE
  void AppendBlob(TagId tag_id,
                      size_t data_size,
                      const hipc::Pointer &data,
                      size_t page_size,
                      float score,
                      u32 node_id,
                      const Context &ctx) {
    AsyncAppendBlob(tag_id, data_size, data, page_size, score, node_id, ctx);
  }
  CHI_TASK_METHODS(AppendBlob);

  /** Create a tag or get the ID of existing tag */
  HSHM_ALWAYS_INLINE
  void AsyncGetOrCreateTagConstruct(GetOrCreateTagTask *task,
                                    const TaskNode &task_node,
                                    const hshm::charbuf &tag_name,
                                    bool blob_owner,
                                    const std::vector<TraitId> &traits,
                                    size_t backend_size,
                                    u32 flags,
                                    const Context &ctx = Context()) {
    HILOG(kDebug, "Creating a tag {}", tag_name.str());
    CHI_CLIENT->ConstructTask<GetOrCreateTagTask>(
        task, task_node, id_,
        tag_name, blob_owner, traits, backend_size, flags, ctx);
  }
  HSHM_ALWAYS_INLINE
  TagId GetOrCreateTag(const hshm::charbuf &tag_name,
                           bool blob_owner,
                           const std::vector<TraitId> &traits,
                           size_t backend_size,
                           u32 flags,
                           const Context &ctx = Context()) {
    LPointer<GetOrCreateTagTask> task =
        AsyncGetOrCreateTag(tag_name, blob_owner, traits, backend_size, flags, ctx);
    task->Wait();
    TagId tag_id = task->tag_id_;
    CHI_CLIENT->DelTask(task);
    return tag_id;
  }
  CHI_TASK_METHODS(GetOrCreateTag);

  /** Get tag ID */
  void AsyncGetTagIdConstruct(GetTagIdTask *task,
                              const TaskNode &task_node,
                              const hshm::charbuf &tag_name) {
    u32 hash = std::hash<hshm::charbuf>{}(tag_name);
    CHI_CLIENT->ConstructTask<GetTagIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_name);
  }
  TagId GetTagId(const hshm::charbuf &tag_name) {
    LPointer<GetTagIdTask> task =
        AsyncGetTagId(tag_name);
    task->Wait();
    TagId tag_id = task->tag_id_;
    CHI_CLIENT->DelTask(task);
    return tag_id;
  }
  CHI_TASK_METHODS(GetTagId);

  /** Get tag name */
  void AsyncGetTagNameConstruct(GetTagNameTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    CHI_CLIENT->ConstructTask<GetTagNameTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  hshm::string GetTagName(const TagId &tag_id) {
    LPointer<GetTagNameTask> task =
        AsyncGetTagName(tag_id);
    task->Wait();
    hshm::string tag_name = hshm::to_charbuf<hipc::string>(*task->tag_name_.get());
    CHI_CLIENT->DelTask(task);
    return tag_name;
  }
  CHI_TASK_METHODS(GetTagName);

  /** Rename tag */
  void AsyncRenameTagConstruct(RenameTagTask *task,
                               const TaskNode &task_node,
                               const TagId &tag_id,
                               const hshm::charbuf &new_tag_name) {
    u32 hash = tag_id.hash_;
    CHI_CLIENT->ConstructTask<RenameTagTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, new_tag_name);
  }
  void RenameTag(const TagId &tag_id, const hshm::charbuf &new_tag_name) {
    LPointer<RenameTagTask> task =
        AsyncRenameTag(tag_id, new_tag_name);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(RenameTag);

  /** Destroy tag */
  void AsyncDestroyTagConstruct(DestroyTagTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    CHI_CLIENT->ConstructTask<DestroyTagTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  void DestroyTag(const TagId &tag_id) {
    LPointer<DestroyTagTask> task =
        AsyncDestroyTag(tag_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(DestroyTag);

  /** Add a blob to a tag */
  void AsyncTagAddBlobConstruct(TagAddBlobTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id) {
    u32 hash = tag_id.hash_;
    CHI_CLIENT->ConstructTask<TagAddBlobTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_id);
  }
  void TagAddBlob(const TagId &tag_id, const BlobId &blob_id) {
    LPointer<TagAddBlobTask> task =
        AsyncTagAddBlob(tag_id, blob_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TagAddBlob);

  /** Remove a blob from a tag */
  void AsyncTagRemoveBlobConstruct(TagRemoveBlobTask *task,
                                   const TaskNode &task_node,
                                   const TagId &tag_id, const BlobId &blob_id) {
    u32 hash = tag_id.hash_;
    CHI_CLIENT->ConstructTask<TagRemoveBlobTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_id);
  }
  void TagRemoveBlob(const TagId &tag_id, const BlobId &blob_id) {
    LPointer<TagRemoveBlobTask> task =
        AsyncTagRemoveBlob(tag_id, blob_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TagRemoveBlob);

  /** Clear blobs from a tag */
  void AsyncTagClearBlobsConstruct(TagClearBlobsTask *task,
                                   const TaskNode &task_node,
                                   const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    CHI_CLIENT->ConstructTask<TagClearBlobsTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  void TagClearBlobs(const TagId &tag_id) {
    LPointer<TagClearBlobsTask> task =
        AsyncTagClearBlobs(tag_id);
    task->Wait();
    CHI_CLIENT->DelTask(task);
  }
  CHI_TASK_METHODS(TagClearBlobs);

  /** Get the size of a bucket */
  void AsyncGetSizeConstruct(GetSizeTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    CHI_CLIENT->ConstructTask<GetSizeTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  size_t GetSize(const TagId &tag_id) {
    LPointer<GetSizeTask> task =
        AsyncGetSize(tag_id);
    task->Wait();
    size_t size = task->size_;
    CHI_CLIENT->DelTask(task);
    return size;
  }
  CHI_TASK_METHODS(GetSize);

  /** Get contained blob ids */
  void AsyncGetContainedBlobIdsConstruct(GetContainedBlobIdsTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    CHI_CLIENT->ConstructTask<GetContainedBlobIdsTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  std::vector<BlobId> GetContainedBlobIds(const TagId &tag_id) {
    LPointer<GetContainedBlobIdsTask> task =
        AsyncGetContainedBlobIds(tag_id);
    task->Wait();
    std::vector<BlobId> blob_ids = task->blob_ids_->vec();
    CHI_CLIENT->DelTask(task);
    return blob_ids;
  }
  CHI_TASK_METHODS(GetContainedBlobIds);

  /**
  * Get all tag metadata
  * */
  void AsyncPollTagMetadataConstruct(PollTagMetadataTask *task,
                                        const TaskNode &task_node) {
    CHI_CLIENT->ConstructTask<PollTagMetadataTask>(
        task, task_node, id_);
  }
  std::vector<TagInfo> PollTagMetadata() {
    LPointer<PollTagMetadataTask> task =
        AsyncPollTagMetadata();
    task->Wait();
    std::vector<TagInfo> target_mdms =
        task->DeserializeTagMetadata();
    CHI_CLIENT->DelTask(task);
    return target_mdms;
  }
  CHI_TASK_METHODS(PollTagMetadata);
};

}  // namespace hrun

#endif  // HRUN_hermes_bucket_mdm_H_
