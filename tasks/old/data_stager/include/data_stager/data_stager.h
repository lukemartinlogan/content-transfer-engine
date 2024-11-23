//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_data_stager_H_
#define HRUN_data_stager_H_

#include "data_stager_tasks.h"

namespace hermes::data_stager {

/** Create data_stager requests */
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
                                      const PoolId &blob_mdm,
                                      const PoolId &bkt_mdm) {
    id_ = PoolId::GetNull();
    QueueManagerInfo &qm = CHI_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    return CHI_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info, blob_mdm, bkt_mdm);
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
    CHI_CLIENT->DelTask(CHI_DEFAULT_MEM_CTX, task);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void Destroy(const DomainQuery &dom_query) {
    CHI_ADMIN->DestroyTaskState(domain_id, id_);
  }

  /** Register task state */
  HSHM_ALWAYS_INLINE
  void AsyncRegisterStagerConstruct(RegisterStagerTask *task,
                                    const TaskNode &task_node,
                                    const BucketId &bkt_id,
                                    const chi::charbuf &path,
                                    const chi::charbuf &params) {
    CHI_CLIENT->ConstructTask<RegisterStagerTask>(
        task, task_node, id_, bkt_id, path, params);
  }
  HSHM_ALWAYS_INLINE
  void RegisterStager(const BucketId &bkt_id,
                          const chi::charbuf &path,
                          const chi::charbuf params) {
    LPointer<RegisterStagerTask> task =
        AsyncRegisterStager(bkt_id, path, params);
    task.ptr_->Wait();
  }
  CHI_TASK_METHODS(RegisterStager);

  /** Unregister task state */
  HSHM_ALWAYS_INLINE
  void AsyncUnregisterStagerConstruct(UnregisterStagerTask *task,
                                      const TaskNode &task_node,
                                      const BucketId &bkt_id) {
    CHI_CLIENT->ConstructTask<UnregisterStagerTask>(
        task, task_node, id_, bkt_id);
  }
  HSHM_ALWAYS_INLINE
  void UnregisterStager(const BucketId &bkt_id) {
    LPointer<UnregisterStagerTask> task =
        AsyncUnregisterStager(bkt_id);
    task.ptr_->Wait();
  }
  CHI_TASK_METHODS(UnregisterStager);

  /** Stage in data from a remote source */
  HSHM_ALWAYS_INLINE
  void AsyncStageInConstruct(StageInTask *task,
                            const TaskNode &task_node,
                            const BucketId &bkt_id,
                            const chi::charbuf &blob_name,
                            float score,
                            u32 node_id) {
    CHI_CLIENT->ConstructTask<StageInTask>(
        task, task_node, id_, bkt_id,
        blob_name, score, node_id);
  }
  HSHM_ALWAYS_INLINE
  void StageIn(const BucketId &bkt_id,
               const chi::charbuf &blob_name,
               float score,
               u32 node_id) {
    LPointer<StageInTask> task =
        AsyncStageIn(bkt_id, blob_name, score, node_id);
    task.ptr_->Wait();
  }
  CHI_TASK_METHODS(StageIn);

  /** Stage out data to a remote source */
  HSHM_ALWAYS_INLINE
  void AsyncStageOutConstruct(StageOutTask *task,
                              const TaskNode &task_node,
                              const BucketId &bkt_id,
                              const chi::charbuf &blob_name,
                              const hipc::Pointer &data,
                              size_t data_size,
                              u32 task_flags) {
    CHI_CLIENT->ConstructTask<StageOutTask>(
        task, task_node, id_, bkt_id,
        blob_name, data, data_size, task_flags);
  }
  HSHM_ALWAYS_INLINE
  void StageOut(const BucketId &bkt_id,
                    const chi::charbuf &blob_name,
                    const hipc::Pointer &data,
                    size_t data_size,
                    u32 task_flags) {
    LPointer<StageOutTask> task =
        AsyncStageOut(bkt_id, blob_name, data, data_size, task_flags);
    task.ptr_->Wait();
  }
  CHI_TASK_METHODS(StageOut);

  /** Stage out data to a remote source */
  HSHM_ALWAYS_INLINE
  void AsyncUpdateSizeConstruct(UpdateSizeTask *task,
                              const TaskNode &task_node,
                              const BucketId &bkt_id,
                              const chi::charbuf &blob_name,
                              size_t blob_off,
                              size_t data_size,
                              u32 task_flags) {
    CHI_CLIENT->ConstructTask<UpdateSizeTask>(
        task, task_node, id_, bkt_id,
        blob_name, blob_off, data_size, task_flags);
  }
  HSHM_ALWAYS_INLINE
  void UpdateSize(const BucketId &bkt_id,
                    const chi::charbuf &blob_name,
                    size_t blob_off,
                    size_t data_size,
                    u32 task_flags) {
    AsyncUpdateSize(bkt_id, blob_name, blob_off, data_size, task_flags);
  }
  CHI_TASK_METHODS(UpdateSize);


};

}  // namespace hrun

#endif  // HRUN_data_stager_H_
