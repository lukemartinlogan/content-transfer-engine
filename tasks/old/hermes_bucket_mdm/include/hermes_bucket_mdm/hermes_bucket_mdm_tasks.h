//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef HRUN_TASKS_HERMES_BUCKET_MDM_INCLUDE_HERMES_BUCKET_MDM_HERMES_BUCKET_MDM_TASKS_H_
#define HRUN_TASKS_HERMES_BUCKET_MDM_INCLUDE_HERMES_BUCKET_MDM_HERMES_BUCKET_MDM_TASKS_H_

#include "chimaera/api/chimaera_client.h"
#include "chimaera/module_registry/task_lib.h"
#include "chimaera_admin/chimaera_admin.h"
#include "chimaera/queue_manager/queue_manager_client.h"
#include "hermes/hermes_types.h"
#include "bdev/bdev.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"
#include "chimaera/api/chimaera_client.h"
#include "chimaera/chimaera_namespace.h"
#include "proc_queue/proc_queue.h"
#include "data_stager/data_stager.h"

namespace hermes::bucket_mdm {

#include "hermes_bucket_mdm_methods.h"
#include "chimaera/chimaera_namespace.h"


/**
 * A task to create hermes_bucket_mdm
 * */
using chi::Admin::CreateTaskStateTask;
struct ConstructTask : public CreateTaskStateTask {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc) : CreateTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc,
                const TaskNode &task_node,
                const DomainQuery &dom_query,
                const std::string &state_name,
                const PoolId &id,
                const std::vector<PriorityInfo> &queue_info)
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            "hermes_bucket_mdm", id, queue_info) {
  }
};

/** Update bucket size */
struct UpdateSizeTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN ssize_t update_;
  IN int mode_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  UpdateSizeTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  UpdateSizeTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc,
              const TaskNode &task_node,
              const DomainQuery &dom_query,
              const PoolId &pool_id,
              const TagId &tag_id,
              ssize_t update,
              int mode) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = tag_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kUpdateSize;
    task_flags_.SetBits(0 | TASK_FIRE_AND_FORGET);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    update_ = update;
    mode_ = mode;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, update_, mode_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {}
};

/** Phases for the append task */
class AppendBlobPhase {
 public:
  TASK_METHOD_T kGetBlobIds = 0;
  TASK_METHOD_T kWaitBlobIds = 1;
  TASK_METHOD_T kWaitPutBlobs = 2;
};

/** A struct to store the  */
struct AppendInfo {
  size_t blob_off_;
  size_t data_size_;
  chi::charbuf blob_name_;
  BlobId blob_id_;
  blob_mdm::GetOrCreateBlobIdTask *blob_id_task_;
  blob_mdm::PutBlobTask *put_task_;

  template<typename Ar>
  void serialize(Ar &ar) {
    ar(blob_off_, data_size_, blob_name_, blob_id_);
  }
};

/** A task to append data to a bucket */
struct AppendBlobSchemaTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN size_t data_size_;
  IN size_t page_size_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  AppendBlobSchemaTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  AppendBlobSchemaTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc,
                       const TaskNode &task_node,
                       const DomainQuery &dom_query,
                       const PoolId &pool_id,
                       const TagId &tag_id,
                       size_t data_size,
                       size_t page_size) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = tag_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kAppendBlobSchema;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    data_size_ = data_size;
    page_size_ = page_size;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, data_size_, page_size_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(append_info_);
  }
};

/** A task to append data to a bucket */
struct AppendBlobTask : public Task, TaskFlags<TF_LOCAL> {
  IN TagId tag_id_;
  IN size_t data_size_;
  IN hipc::Pointer data_;
  IN size_t page_size_;
  IN u32 node_id_;
  IN float score_;
  TEMP int phase_ = AppendBlobPhase::kGetBlobIds;
  TEMP AppendBlobSchemaTask *schema_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  AppendBlobTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  AppendBlobTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc,
                 const TaskNode &task_node,
                 const DomainQuery &dom_query,
                 const PoolId &pool_id,
                 const TagId &tag_id,
                 size_t data_size,
                 const hipc::Pointer &data,
                 size_t page_size,
                 float score,
                 u32 node_id,
                 const Context &ctx) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = tag_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kAppendBlob;
    task_flags_.SetBits(0 | TASK_FIRE_AND_FORGET | TASK_DATA_OWNER | TASK_UNORDERED | TASK_REMOTE_DEBUG_MARK);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    data_size_ = data_size;
    data_ = data;
    score_ = score;
    page_size_ = page_size;
    node_id_ = node_id;
  }

  /** Destructor */
  ~AppendBlobTask() {
    if (IsDataOwner()) {
      CHI_CLIENT->FreeBuffer(data_);
    }
  }
};

/** A task to collect blob metadata */
struct PollTagMetadataTask : public Task, TaskFlags<TF_SRL_SYM_START | TF_SRL_ASYM_END | TF_REPLICA> {
  OUT chi::string my_tag_mdms_;
  TEMP hipc::vector<chi::string> tag_mdms_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  PollTagMetadataTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc) : Task(alloc) {
  }

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  PollTagMetadataTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc,
                       const TaskNode &task_node,
                       const PoolId &pool_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kPollTagMetadata;
    task_flags_.SetBits(TASK_LANE_ALL);
    domain_id_ = chi::DomainQuery::GetGlobalBcast();

    // Custom params
  }

  /** Serialize tag info */
  void SerializeTagMetadata(const std::vector<TagInfo> &tag_info) {
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar << tag_info;
    (*my_tag_mdms_) = ss.str();
  }

  /** Deserialize tag info */
  void DeserializeTagMetadata(const std::string &srl, std::vector<TagInfo> &tag_mdms) {
    std::vector<TagInfo> tmp_tag_mdms;
    std::stringstream ss(srl);
    cereal::BinaryInputArchive ar(ss);
    ar >> tmp_tag_mdms;
    for (TagInfo &tag_info : tmp_tag_mdms) {
      tag_mdms.emplace_back(tag_info);
    }
  }

  /** Get combined output of all replicas */
  std::vector<TagInfo> MergeTagMetadata() {
    std::vector<TagInfo> tag_mdms;
    for (const chi::string &srl : *tag_mdms_) {
      DeserializeTagMetadata(srl.str(), tag_mdms);
    }
    return tag_mdms;
  }

  /** Deserialize final query output */
  std::vector<TagInfo> DeserializeTagMetadata() {
    std::vector<TagInfo> tag_mdms;
    DeserializeTagMetadata(my_tag_mdms_->str(), tag_mdms);
    return tag_mdms;
  }

  /** Duplicate message */
  void Dup(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, PollTagMetadataTask &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, PollTagMetadataTask &dup_task) {
    (*tag_mdms_)[replica] = (*dup_task.my_tag_mdms_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    ar(my_tag_mdms_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SaveEnd(Ar &ar) {
    ar(my_tag_mdms_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void LoadEnd(u32 replica, Ar &ar) {
    ar(my_tag_mdms_);
    DupEnd(replica, *this);
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {
    tag_mdms_->resize(count);
  }

  /** Finalize replication */
  void ReplicateEnd() {
    std::vector<TagInfo> tag_mdms = MergeTagMetadata();
    SerializeTagMetadata(tag_mdms);
  }
};

}  // namespace hermes::bucket_mdm

#endif  // HRUN_TASKS_HERMES_BUCKET_MDM_INCLUDE_HERMES_BUCKET_MDM_HERMES_BUCKET_MDM_TASKS_H_
