//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_hermes_data_op_H_
#define HRUN_hermes_data_op_H_

#include "hermes_data_op_tasks.h"

namespace hermes::data_op {

/** Create hermes_data_op requests */
class Client : public ModuleClient {

 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Async create a task state */
  HSHM_ALWAYS_INLINE
  LPointer<ConstructTask> AsyncCreate(const TaskNode &task_node,
                                      const DomainQuery &dom_query,
                                      const std::string &state_name,
                                      PoolId &bkt_mdm_id,
                                      PoolId &blob_mdm_id) {
    id_ = PoolId::GetNull();
    QueueManagerInfo &qm = CHI_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    return CHI_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info,
        bkt_mdm_id, blob_mdm_id);
  }
  HRUN_TASK_NODE_ROOT(AsyncCreate)
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void Create(Args&& ...args) {
    LPointer<ConstructTask> task =
        AsyncCreate(std::forward<Args>(args)...);
    task->Wait();
    id_ = task->id_;
    Init(id_, CHI_ADMIN->queue_id_);
    CHI_CLIENT->DelTask(task);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void Destroy(const DomainQuery &dom_query) {
    CHI_ADMIN->DestroyTaskState(domain_id, id_);
  }

  /** Register the OpGraph to perform on data */
  HSHM_ALWAYS_INLINE
  void AsyncRegisterOpConstruct(RegisterOpTask *task,
                                const TaskNode &task_node,
                                const OpGraph &op_graph) {
    CHI_CLIENT->ConstructTask<RegisterOpTask>(
        task, task_node, chi::DomainQuery::GetGlobalBcast(), id_, op_graph);
  }
  HSHM_ALWAYS_INLINE
  void RegisterOp(const OpGraph &op_graph) {
    LPointer<RegisterOpTask> task =
        AsyncRegisterOp(op_graph);
    task.ptr_->Wait();
  }
  CHI_TASK_METHODS(RegisterOp);

  /** Register data as ready for operations to be performed */
  HSHM_ALWAYS_INLINE
  void AsyncRegisterDataConstruct(RegisterDataTask *task,
                                  const TaskNode &task_node,
                                  const BucketId &bkt_id,
                                  const std::string &blob_name,
                                  const BlobId &blob_id,
                                  size_t off,
                                  size_t size) {
    CHI_CLIENT->ConstructTask<RegisterDataTask>(
        task, task_node, id_, bkt_id,
        blob_name, blob_id, off, size);
  }
  CHI_TASK_METHODS(RegisterData);

  /** Async task to run operators */
  HSHM_ALWAYS_INLINE
  void AsyncRunOpConstruct(RunOpTask *task,
                           const TaskNode &task_node) {
    CHI_CLIENT->ConstructTask<RunOpTask>(
        task, task_node, id_);
  }
  CHI_TASK_METHODS(RunOp);
};

}  // namespace hrun

#endif  // HRUN_hermes_data_op_H_
