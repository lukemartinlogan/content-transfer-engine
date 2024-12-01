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
                                          const DomainQuery &dom_query,
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
      CHI_CLIENT->DelTask(HSHM_DEFAULT_MEM_CTX, task);
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
  void Destroy(const DomainQuery &dom_query) {
    CHI_ADMIN->DestroyTaskState(domain_id, id_);
  }

  /**====================================
   * Blob Operations
   * ===================================*/

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
    CHI_CLIENT->DelTask(HSHM_DEFAULT_MEM_CTX, task);
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
    CHI_CLIENT->DelTask(HSHM_DEFAULT_MEM_CTX, task);
    return target_mdms;
  }
  CHI_TASK_METHODS(PollTargetMetadata);
};

}  // namespace hrun

#endif  // HRUN_hermes_blob_mdm_H_