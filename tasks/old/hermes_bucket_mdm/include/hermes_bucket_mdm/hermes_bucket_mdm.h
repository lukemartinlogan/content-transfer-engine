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
  void Create(const DomainQuery &dom_query,
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
  void Destroy(const DomainQuery &dom_query) {
    CHI_ADMIN->DestroyTaskState(domain_id, id_);
  }

  /**====================================
   * Tag Operations
   * ===================================*/

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
    CHI_CLIENT->DelTask(HSHM_DEFAULT_MEM_CTX, task);
    return target_mdms;
  }
  CHI_TASK_METHODS(PollTagMetadata);
};

}  // namespace hrun

#endif  // HRUN_hermes_bucket_mdm_H_
