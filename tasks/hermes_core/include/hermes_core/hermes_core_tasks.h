//
// Created by lukemartinlogan on 8/11/23.
//

#ifndef CHI_TASKS_TASK_TEMPL_INCLUDE_hermes_core_hermes_core_TASKS_H_
#define CHI_TASKS_TASK_TEMPL_INCLUDE_hermes_core_hermes_core_TASKS_H_

#include "chimaera/chimaera_namespace.h"
#include "hermes/hermes_types.h"

namespace hermes {

#include "hermes_core_methods.h"
CHI_NAMESPACE_INIT

/** Hash tag name */
template <typename StringT>
static inline u32 HashTagName(const StringT &tag_name) {
  u32 h1 = 0;
  for (size_t i = 0; i < tag_name.size(); ++i) {
    auto shift = static_cast<u32>(i % sizeof(u32));
    auto c = static_cast<u32>((unsigned char)tag_name[i]);
    h1 = 31 * h1 + (c << shift);
  }
  return hshm::hash<u32>{}(h1);
}

/** Hash tag name or ID */
template <typename StringT>
static inline u32 HashTagNameOrId(const TagId &tag_id,
                                  const StringT &tag_name) {
  if (tag_name.size() > 0) {
    return HashTagName(tag_name);
  }
  return tag_id.hash_;
}

/** Hash blob name + TagId */
template <typename StringT>
static inline u32 HashBlobName(const TagId &tag_id, const StringT &blob_name) {
  u32 h1 = HashTagName(blob_name);
  u32 h2 = hshm::hash<TagId>{}(tag_id);
  return hshm::hash<u32>{}(h1 ^ h2);
}

/** Hash by blob name or ID */
template <typename StringT>
static inline u32 HashBlobNameOrId(const TagId &tag_id,
                                   const StringT &blob_name,
                                   const BlobId &blob_id) {
  if (blob_name.size() > 0) {
    return HashBlobName(tag_id, blob_name);
  }
  return blob_id.hash_;
}

/** Blob with ID */
class BlobWithId {};

/** Blob with name */
class BlobWithName {};

/** Blob with ID and name */
class BlobWithIdAndName {};

/** Tag with ID */
class TagWithId {};

/** Tag with name */
class TagWithName {};

/** Tag with ID and name */
class TagWithIdAndName {};

/**
 * A task to create hermes_core
 * */
struct CreateTaskParams {
  CLS_CONST char *lib_name_ = "hermes_core";

  CreateTaskParams() = default;

  CreateTaskParams(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc) {}

  template <typename Ar>
  void serialize(Ar &ar) {}
};
typedef chi::Admin::CreatePoolBaseTask<CreateTaskParams> CreateTask;

/** A task to destroy hermes_core */
typedef chi::Admin::DestroyContainerTask DestroyTask;

/**
 * ========================================
 * TAG Tasks
 * ========================================
 * */

/**
 * Create a tag
 * */
struct GetOrCreateTagTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithName {
  IN chi::ipc::string tag_name_;
  IN chi::ipc::string params_;
  IN bool blob_owner_;
  IN size_t backend_size_;
  IN bitfield32_t flags_;
  OUT TagId tag_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetOrCreateTagTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), tag_name_(alloc), params_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetOrCreateTagTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query,
      const chi::string &tag_name, bool blob_owner, size_t backend_size,
      u32 flags, const Context &ctx)
      : Task(alloc), tag_name_(alloc, tag_name), params_(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetOrCreateTag;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    blob_owner_ = blob_owner;
    backend_size_ = backend_size;
    params_ = ctx.bkt_params_;
    flags_ = bitfield32_t(flags | ctx.flags_.bits_);
  }

  /** Duplicate message */
  void CopyStart(const GetOrCreateTagTask &other, bool deep) {
    tag_name_ = other.tag_name_;
    blob_owner_ = other.blob_owner_;
    backend_size_ = other.backend_size_;
    flags_ = other.flags_;
    tag_id_ = other.tag_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_name_, params_, blob_owner_, backend_size_, flags_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(tag_id_);
  }
};

/** A task to get a tag id */
struct GetTagIdTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithName {
  IN chi::ipc::string tag_name_;
  OUT TagId tag_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetTagIdTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), tag_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetTagIdTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query,
      const chi::string &tag_name)
      : Task(alloc), tag_name_(alloc, tag_name) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetTagId;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;
  }

  /** Duplicate message */
  void CopyStart(const GetTagIdTask &other, bool deep) {
    tag_name_ = other.tag_name_;
    tag_id_ = other.tag_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_name_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(tag_id_);
  }
};

/** A task to get a tag name */
struct GetTagNameTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithId {
  IN TagId tag_id_;
  OUT chi::ipc::string tag_name_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetTagNameTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), tag_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetTagNameTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id)
      : Task(alloc), tag_name_(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetTagName;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
  }

  /** Duplicate message */
  void CopyStart(const GetTagNameTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    tag_name_ = other.tag_name_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(tag_name_);
  }
};

/** A task to destroy a tag */
struct DestroyTagTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithId {
  IN TagId tag_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit DestroyTagTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit DestroyTagTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kDestroyTag;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
  }

  /** Duplicate message */
  void CopyStart(const DestroyTagTask &other, bool deep) {
    tag_id_ = other.tag_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to add a blob to the tag */
struct TagAddBlobTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithId {
  IN TagId tag_id_;
  IN BlobId blob_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit TagAddBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TagAddBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, TagId tag_id,
      const BlobId &blob_id)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTagAddBlob;
    task_flags_.SetBits(TASK_FIRE_AND_FORGET);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
  }

  /** Duplicate message */
  void CopyStart(const TagAddBlobTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to remove a blob from a tag */
struct TagRemoveBlobTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithId {
  IN TagId tag_id_;
  IN BlobId blob_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit TagRemoveBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TagRemoveBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, TagId tag_id,
      const BlobId &blob_id)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTagRemoveBlob;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
  }

  /** Duplicate message */
  void CopyStart(const TagRemoveBlobTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to destroy all blobs in the tag */
struct TagClearBlobsTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithId {
  IN TagId tag_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit TagClearBlobsTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TagClearBlobsTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, TagId tag_id)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTagClearBlobs;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
  }

  /** Duplicate message */
  void CopyStart(const TagClearBlobsTask &other, bool deep) {
    tag_id_ = other.tag_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to destroy all blobs in the tag */
struct TagGetSizeTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithId {
  IN TagId tag_id_;
  OUT size_t size_;

  /** SHM default constructor */
  HSHM_INLINE explicit TagGetSizeTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TagGetSizeTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, TagId tag_id)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTagGetSize;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
  }

  /** Duplicate message */
  void CopyStart(const TagGetSizeTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    size_ = other.size_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(size_);
  }
};

/** A task to destroy all blobs in the tag */
struct TagUpdateSizeTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithId {
  IN TagId tag_id_;
  IN ssize_t update_;
  IN int mode_;

  /** SHM default constructor */
  HSHM_INLINE explicit TagUpdateSizeTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TagUpdateSizeTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, TagId tag_id,
      ssize_t update, int mode)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTagUpdateSize;
    task_flags_.SetBits(TASK_FIRE_AND_FORGET);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    update_ = update;
    mode_ = mode;
  }

  /** Duplicate message */
  void CopyStart(const TagUpdateSizeTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    update_ = other.update_;
    mode_ = other.mode_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, update_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to destroy all blobs in the tag */
struct TagGetContainedBlobIdsTask : public Task,
                                    TaskFlags<TF_SRL_SYM>,
                                    TagWithId {
  IN TagId tag_id_;
  OUT hipc::vector<BlobId> blob_ids_;

  /** SHM default constructor */
  HSHM_INLINE explicit TagGetContainedBlobIdsTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_ids_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TagGetContainedBlobIdsTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id)
      : Task(alloc), blob_ids_(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTagGetContainedBlobIds;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
  }

  /** Duplicate message */
  void CopyStart(const TagGetContainedBlobIdsTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_ids_ = other.blob_ids_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_ids_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(blob_ids_);
  }
};

/** The TagFlushTask task */
struct TagFlushTask : public Task, TaskFlags<TF_SRL_SYM>, TagWithId {
  IN TagId tag_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit TagFlushTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TagFlushTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTagFlush;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
  }

  /** Duplicate message */
  void CopyStart(const TagFlushTask &other, bool deep) {}

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {}

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/**
 * ========================================
 * BLOB Tasks
 * ========================================
 * */

#define HERMES_BLOB_REPLACE BIT_OPT(u32, 0)
#define HERMES_BLOB_APPEND BIT_OPT(u32, 1)
#define HERMES_DID_STAGE_IN BIT_OPT(u32, 2)
#define HERMES_SHOULD_STAGE BIT_OPT(u32, 3)
#define HERMES_STAGE_NO_WRITE BIT_OPT(u32, 4)
#define HERMES_STAGE_NO_READ BIT_OPT(u32, 5)
#define HERMES_BLOB_DID_CREATE BIT_OPT(u32, 6)
#define HERMES_GET_BLOB_ID BIT_OPT(u32, 7)
#define HERMES_HAS_DERIVED BIT_OPT(u32, 8)
#define HERMES_USER_SCORE_STATIONARY BIT_OPT(u32, 9)

/**
 * Get \a blob_name BLOB from \a bkt_id bucket
 * */
struct GetOrCreateBlobIdTask : public Task,
                               TaskFlags<TF_SRL_SYM>,
                               BlobWithName {
  IN TagId tag_id_;
  IN chi::string blob_name_;
  OUT BlobId blob_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetOrCreateBlobIdTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetOrCreateBlobIdTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const chi::string &blob_name)
      : Task(alloc), blob_name_(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetOrCreateBlobId;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
  }

  /** Duplicate message */
  void CopyStart(const GetOrCreateBlobIdTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_name_ = other.blob_name_;
    blob_id_ = other.blob_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_name_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(blob_id_);
  }
};

/**
 * Get \a blob_name BLOB from \a bkt_id bucket
 * */
struct GetBlobIdTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithName {
  IN TagId tag_id_;
  IN chi::string blob_name_;
  OUT BlobId blob_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetBlobIdTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetBlobIdTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const chi::string &blob_name)
      : Task(alloc), blob_name_(alloc, blob_name) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetBlobId;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
  }

  /** Duplicate message */
  void CopyStart(const GetBlobIdTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_name_ = other.blob_name_;
    blob_id_ = other.blob_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_name_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(blob_id_);
  }
};

/**
 * Get \a blob_name BLOB name from \a blob_id BLOB id
 * */
struct GetBlobNameTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  OUT chi::ipc::string blob_name_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetBlobNameTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetBlobNameTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const BlobId &blob_id)
      : Task(alloc), blob_name_(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetBlobName;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
  }

  /** Duplicate message */
  void CopyStart(const GetBlobNameTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    blob_name_ = other.blob_name_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(blob_name_);
  }
};

/** Get \a score from \a blob_id BLOB id */
struct GetBlobSizeTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN TagId tag_id_;
  IN chi::string blob_name_;
  IN BlobId blob_id_;
  OUT size_t size_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetBlobSizeTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetBlobSizeTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const chi::string &blob_name, const BlobId &blob_id)
      : Task(alloc), blob_name_(alloc, blob_name) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetBlobSize;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
  }

  /** Duplicate message */
  void CopyStart(const GetBlobSizeTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_name_ = other.blob_name_;
    blob_id_ = other.blob_id_;
    size_ = other.size_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_name_, blob_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(size_);
  }
};

/** Get \a score from \a blob_id BLOB id */
struct GetBlobScoreTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  OUT float score_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetBlobScoreTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetBlobScoreTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const BlobId &blob_id)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetBlobScore;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
  }

  /** Duplicate message */
  void CopyStart(const GetBlobScoreTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    score_ = other.score_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(score_);
  }
};

/** Get \a blob_id blob's buffers */
struct GetBlobBuffersTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  OUT hipc::vector<BufferInfo> buffers_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetBlobBuffersTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), buffers_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetBlobBuffersTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const BlobId &blob_id)
      : Task(alloc), buffers_(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetBlobBuffers;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
  }

  /** Duplicate message */
  void CopyStart(const GetBlobBuffersTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    buffers_ = other.buffers_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(buffers_);
  }
};

/**
 * Check if blob has a tag
 * */
struct BlobHasTagTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN TagId tag_;
  OUT bool has_tag_;

  /** SHM default constructor */
  HSHM_INLINE explicit BlobHasTagTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit BlobHasTagTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const BlobId &blob_id, const TagId &tag)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kBlobHasTag;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    tag_ = tag;
  }

  /** Duplicate message */
  void CopyStart(const BlobHasTagTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    tag_ = other.tag_;
    has_tag_ = other.has_tag_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_id_, tag_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(has_tag_);
  }
};

/** A task to truncate a blob */
struct TruncateBlobTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN u64 size_;

  /** SHM default constructor */
  HSHM_INLINE explicit TruncateBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TruncateBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const BlobId &blob_id, u64 size)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTruncateBlob;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    size_ = size;
  }

  /** Duplicate message */
  void CopyStart(const TruncateBlobTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    size_ = other.size_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_id_, size_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to destroy a blob */
struct DestroyBlobTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  CLS_CONST u32 kKeepInTag = BIT_OPT(u32, 0);

  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN bitfield32_t flags_;

  /** SHM default constructor */
  HSHM_INLINE explicit DestroyBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit DestroyBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const BlobId &blob_id, u32 blob_flags, u32 task_flags = 0)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kDestroyBlob;
    task_flags_.SetBits(task_flags);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    flags_.SetBits(blob_flags);
  }

  /** Duplicate message */
  void CopyStart(const DestroyBlobTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    flags_ = other.flags_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_id_, flags_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to reorganize a blob's composition in the hierarchy */
struct ReorganizeBlobTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN chi::string blob_name_;
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN float score_;
  IN u32 node_id_;
  IN bool is_user_score_;

  /** SHM default constructor */
  HSHM_INLINE explicit ReorganizeBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit ReorganizeBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const chi::string &blob_name, const BlobId &blob_id, float score,
      bool is_user_score, const Context &ctx,
      u32 task_flags = TASK_FIRE_AND_FORGET)
      : Task(alloc), blob_name_(alloc, blob_name) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kReorganizeBlob;
    task_flags_.SetBits(task_flags);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    score_ = score;
    node_id_ = ctx.node_id_;
    is_user_score_ = is_user_score;
  }

  /** Duplicate message */
  void CopyStart(const ReorganizeBlobTask &other, bool deep) {
    blob_name_ = other.blob_name_;
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    score_ = other.score_;
    node_id_ = other.node_id_;
    is_user_score_ = other.is_user_score_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_name_, blob_id_, score_, node_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** The FlushBlobTask task */
struct FlushBlobTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN BlobId blob_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit FlushBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit FlushBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query,
      const BlobId &blob_id, u32 task_flags = 0)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kFlushBlob;
    task_flags_.SetBits(task_flags);
    dom_query_ = dom_query;

    // Custom
    blob_id_ = blob_id;
  }

  /** Duplicate message */
  void CopyStart(const FlushBlobTask &other, bool deep) {}

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {}

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to tag a blob */
struct TagBlobTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithId {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN TagId tag_;

  /** SHM default constructor */
  HSHM_INLINE explicit TagBlobTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit TagBlobTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc,
                                   const TaskNode &task_node,
                                   const PoolId &pool_id,
                                   const DomainQuery &dom_query,
                                   const TagId &tag_id, const BlobId &blob_id,
                                   const TagId &tag)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kTagBlob;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    tag_ = tag;
  }

  /** Duplicate message */
  void CopyStart(const TagBlobTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    tag_ = other.tag_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(blob_id_, tag_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** A task to put data in a blob */
struct PutBlobTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithIdAndName {
  IN TagId tag_id_;
  IN chi::string blob_name_;
  IN BlobId blob_id_;
  IN size_t blob_off_;
  IN size_t data_size_;
  IN hipc::Pointer data_;
  IN float score_;
  IN bitfield32_t flags_;

  /** SHM default constructor */
  HSHM_INLINE explicit PutBlobTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit PutBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const chi::string &blob_name, const BlobId &blob_id, size_t blob_off,
      size_t data_size, const hipc::Pointer &data, float score, u32 task_flags,
      u32 hermes_flags, const Context &ctx = Context())
      : Task(alloc), blob_name_(alloc, blob_name) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kPutBlob;
    task_flags_.SetBits(task_flags);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    blob_off_ = blob_off;
    data_size_ = data_size;
    data_ = data;
    score_ = score;
    flags_ = bitfield32_t(hermes_flags | ctx.flags_.bits_);
  }

  /** Destructor */
  ~PutBlobTask() {
    if (IsDataOwner()) {
      // HILOG(kInfo, "Actually freeing PUT {} of size {}", task_node_,
      // data_size_);
      CHI_CLIENT->FreeBuffer(data_);
    }
  }

  /** Duplicate message */
  void CopyStart(const PutBlobTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_name_ = other.blob_name_;
    blob_id_ = other.blob_id_;
    blob_off_ = other.blob_off_;
    data_size_ = other.data_size_;
    data_ = other.data_;
    score_ = other.score_;
    flags_ = other.flags_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, score_, flags_);
    ar.bulk(DT_WRITE, data_, data_size_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    if (flags_.Any(HERMES_GET_BLOB_ID)) {
      ar(blob_id_);
    }
  }
};

/** A task to get data from a blob */
struct GetBlobTask : public Task, TaskFlags<TF_SRL_SYM>, BlobWithIdAndName {
  IN TagId tag_id_;
  IN chi::string blob_name_;
  INOUT BlobId blob_id_;
  IN size_t blob_off_;
  IN hipc::Pointer data_;
  INOUT size_t data_size_;
  IN bitfield32_t flags_;

  /** SHM default constructor */
  HSHM_INLINE explicit GetBlobTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit GetBlobTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, const TagId &tag_id,
      const chi::string &blob_name, const BlobId &blob_id, size_t off,
      size_t data_size, hipc::Pointer &data, u32 hermes_flags,
      const Context &ctx = Context())
      : Task(alloc), blob_name_(alloc, blob_name) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kGetBlob;
    task_flags_.SetBits(TASK_COROUTINE);
    dom_query_ = dom_query;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    blob_off_ = off;
    data_size_ = data_size;
    data_ = data;
    flags_ = bitfield32_t(hermes_flags | ctx.flags_.bits_);
  }

  /** Convert data to a data structure */
  template <typename T>
  HSHM_INLINE void Get(T &obj) {
    char *data = CHI_CLIENT->GetDataPointer(data_);
    std::stringstream ss(std::string(data, data_size_));
    cereal::BinaryInputArchive ar(ss);
    ar >> obj;
  }

  /** Convert data to a data structure */
  template <typename T>
  HSHM_INLINE T Get() {
    T obj;
    return Get(obj);
  }

  /** Duplicate message */
  void CopyStart(const GetBlobTask &other, bool deep) {
    tag_id_ = other.tag_id_;
    blob_name_ = other.blob_name_;
    blob_id_ = other.blob_id_;
    blob_off_ = other.blob_off_;
    data_size_ = other.data_size_;
    data_ = other.data_;
    flags_ = other.flags_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, flags_);
    ar.bulk(DT_EXPOSE, data_, data_size_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar.bulk(DT_WRITE, data_, data_size_);
    if (flags_.Any(HERMES_GET_BLOB_ID)) {
      ar(blob_id_);
    }
    ar(data_size_);
  }
};

/** The FlushDataTask task */
struct FlushDataTask : public Task, TaskFlags<TF_SRL_SYM> {
  /** SHM default constructor */
  HSHM_INLINE explicit FlushDataTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit FlushDataTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query, int period_sec)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kFlushData;
    task_flags_.SetBits(TASK_LONG_RUNNING);
    dom_query_ = dom_query;
    SetPeriodSec(period_sec);

    // Custom
  }

  /** Duplicate message */
  void CopyStart(const FlushDataTask &other, bool deep) {}

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {}

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** Base task for various metadata queries */
template <typename MD, int METHOD>
struct PollMetadataTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN hipc::string filter_;
  IN int max_count_;
  OUT hipc::string stats_buf_;

  /** SHM default constructor */
  HSHM_INLINE explicit PollMetadataTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), filter_(alloc), stats_buf_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit PollMetadataTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query,
      const std::string &filter, int max_count)
      : Task(alloc), filter_(alloc), stats_buf_(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = METHOD;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    filter_ = filter;
    max_count_ = max_count;
  }

  /** Serialize stats buf */
  void SetStats(const std::vector<MD> &stats) {
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar(stats);
    stats_buf_ = ss.str();
  }

  /** Get stats buf */
  std::vector<MD> GetStats() {
    std::vector<MD> stats;
    std::stringstream ss(stats_buf_.str());
    cereal::BinaryInputArchive ar(ss);
    ar(stats);
    return stats;
  }

  /** Duplicate message */
  void CopyStart(const PollMetadataTask &other, bool deep) {
    filter_ = other.filter_;
    max_count_ = other.max_count_;
    stats_buf_ = other.stats_buf_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(filter_, max_count_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(stats_buf_);
  }
};

/** The PollBlobMetadataTask task */
using PollBlobMetadataTask =
    PollMetadataTask<BlobInfo, Method::kPollBlobMetadata>;

/** The PollTargetMetadataTask task */
using PollTargetMetadataTask =
    PollMetadataTask<TargetStats, Method::kPollTargetMetadata>;

/** The PollTagMetadataTask task */
using PollTagMetadataTask = PollMetadataTask<TagInfo, Method::kPollTagMetadata>;

/** The PollAccessPatternTask task */
struct PollAccessPatternTask : public Task, TaskFlags<TF_SRL_SYM> {
  INOUT hshm::min_u64 last_access_;
  OUT hipc::vector<IoStat> io_pattern_;

  /** SHM default constructor */
  HSHM_INLINE explicit PollAccessPatternTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit PollAccessPatternTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query,
      hshm::min_u64 last_access)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kPollAccessPattern;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    last_access_ = last_access;
  }

  /** Duplicate message */
  void CopyStart(const PollAccessPatternTask &other, bool deep) {
    last_access_ = other.last_access_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(last_access_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {
    ar(last_access_);
    ar(io_pattern_);
  }
};

/**
 * ========================================
 * STAGING Tasks
 * ========================================
 * */

/** The RegisterStagerTask task */
struct RegisterStagerTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN hermes::BucketId bkt_id_;
  IN chi::ipc::string tag_name_;
  IN chi::ipc::string params_;

  /** SHM default constructor */
  HSHM_INLINE explicit RegisterStagerTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), tag_name_(alloc), params_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit RegisterStagerTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query,
      const hermes::BucketId &bkt_id, const chi::string &tag_name,
      const chi::string &params)
      : Task(alloc), tag_name_(alloc, tag_name), params_(alloc, params) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kRegisterStager;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    bkt_id_ = bkt_id;
  }

  /** Duplicate message */
  void CopyStart(const RegisterStagerTask &other, bool deep) {
    bkt_id_ = other.bkt_id_;
    tag_name_ = other.tag_name_;
    params_ = other.params_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(bkt_id_, tag_name_, params_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** The UnregisterStagerTask task */
struct UnregisterStagerTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN hermes::BucketId bkt_id_;

  /** SHM default constructor */
  HSHM_INLINE explicit UnregisterStagerTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit UnregisterStagerTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query,
      const BucketId &bkt_id)
      : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kUnregisterStager;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    bkt_id_ = bkt_id;
  }

  /** Duplicate message */
  void CopyStart(const UnregisterStagerTask &other, bool deep) {
    bkt_id_ = other.bkt_id_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(bkt_id_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** The StageInTask task */
struct StageInTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN hermes::BucketId bkt_id_;
  IN chi::string blob_name_;
  IN float score_;

  /** SHM default constructor */
  HSHM_INLINE explicit StageInTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit StageInTask(const hipc::CtxAllocator<CHI_ALLOC_T> &alloc,
                                   const TaskNode &task_node,
                                   const PoolId &pool_id,
                                   const DomainQuery &dom_query,
                                   const BucketId &bkt_id,
                                   const chi::string &blob_name, float score)
      : Task(alloc), blob_name_(alloc, blob_name) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kStageIn;
    task_flags_.SetBits(0);
    dom_query_ = dom_query;

    // Custom
    bkt_id_ = bkt_id;
    score_ = score;
  }

  /** Duplicate message */
  void CopyStart(const StageInTask &other, bool deep) {
    bkt_id_ = other.bkt_id_;
    blob_name_ = other.blob_name_;
    score_ = other.score_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(bkt_id_, blob_name_, score_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

/** The StageOutTask task */
struct StageOutTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN hermes::BucketId bkt_id_;
  IN chi::string blob_name_;
  IN hipc::Pointer data_;
  IN size_t data_size_;

  /** SHM default constructor */
  HSHM_INLINE explicit StageOutTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc)
      : Task(alloc), blob_name_(alloc) {}

  /** Emplace constructor */
  HSHM_INLINE explicit StageOutTask(
      const hipc::CtxAllocator<CHI_ALLOC_T> &alloc, const TaskNode &task_node,
      const PoolId &pool_id, const DomainQuery &dom_query,
      const BucketId &bkt_id, const chi::string &blob_name,
      const hipc::Pointer &data, size_t data_size, u32 task_flags)
      : Task(alloc), blob_name_(alloc, blob_name) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrioOpt::kLowLatency;
    pool_ = pool_id;
    method_ = Method::kStageOut;
    task_flags_.SetBits(task_flags);
    dom_query_ = dom_query;

    // Custom
    data_ = data;
    data_size_ = data_size;
    bkt_id_ = bkt_id;
  }

  /** Duplicate message */
  void CopyStart(const StageOutTask &other, bool deep) {
    bkt_id_ = other.bkt_id_;
    blob_name_ = other.blob_name_;
    data_ = other.data_;
    data_size_ = other.data_size_;
  }

  /** (De)serialize message call */
  template <typename Ar>
  void SerializeStart(Ar &ar) {
    ar(bkt_id_, blob_name_);
  }

  /** (De)serialize message return */
  template <typename Ar>
  void SerializeEnd(Ar &ar) {}
};

}  // namespace hermes

#endif  // CHI_TASKS_TASK_TEMPL_INCLUDE_hermes_core_hermes_core_TASKS_H_
