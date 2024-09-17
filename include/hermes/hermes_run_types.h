//
// Created by llogan on 9/16/24.
//

#ifndef HERMES_INCLUDE_HERMES_HERMES_RUN_TYPES_H_
#define HERMES_INCLUDE_HERMES_HERMES_RUN_TYPES_H_

#include "status.h"
#include "statuses.h"
#include "bdev/bdev.h"
#include "hermes_types.h"
#include "chimaera/chimaera_namespace.h"
#include "chimaera/work_orchestrator/corwlock.h"
#include "chimaera/work_orchestrator/comutex.h"

namespace hapi = hermes;

namespace hermes {

CHI_NAMESPACE_INIT

/** Data structure used to store Blob information */
struct BlobInfo {
  TagId tag_id_;    /**< Tag the blob is on */
  BlobId blob_id_;  /**< Unique ID of the blob */
  hshm::charbuf name_;  /**< Name of the blob (without tag_id) */
  std::vector<BufferInfo> buffers_;  /**< Set of buffers */
  std::vector<TagId> tags_;  /**< Set of tags */
  size_t blob_size_;      /**< The overall size of the blob */
  size_t max_blob_size_;  /**< The amount of space current buffers support */
  float score_;  /**< The priority of this blob */
  float user_score_;  /**< The user-defined priority of this blob */
  std::atomic<u32> access_freq_;  /**< Number of times blob accessed in epoch */
  hshm::Timepoint last_access_;  /**< Last time blob accessed */
  std::atomic<size_t> mod_count_;   /**< The number of times blob modified */
  std::atomic<size_t> last_flush_;  /**< The last mod that was flushed */
  bitfield32_t flags_;  /**< Flags */
  chi::CoRwLock lock_;  /**< Lock */

  /** Serialization */
  template<typename Ar>
  void serialize(Ar &ar) {
    ar(tag_id_, blob_id_, name_, buffers_, tags_, blob_size_, max_blob_size_,
       score_, access_freq_, mod_count_, last_flush_);
  }

  /** Default constructor */
  BlobInfo() = default;

  /** Copy constructor */
  BlobInfo(const BlobInfo &other) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    name_ = other.name_;
    buffers_ = other.buffers_;
    tags_ = other.tags_;
    blob_size_ = other.blob_size_;
    max_blob_size_ = other.max_blob_size_;
    score_ = other.score_;
    user_score_ = other.user_score_;
    access_freq_ = other.access_freq_.load();
    last_access_ = other.last_access_;
    mod_count_ = other.mod_count_.load();
    last_flush_ = other.last_flush_.load();
  }

  /** Update modify stats */
  void UpdateWriteStats() {
    mod_count_.fetch_add(1);
    UpdateReadStats();
  }

  /** Update read stats */
  void UpdateReadStats() {
    last_access_.Now();
    access_freq_.fetch_add(1);
  }

  /** Get the globally unique blob name */
  static const hshm::charbuf GetBlobNameWithBucket(
      const TagId &tag_id,
      const hshm::charbuf &blob_name) {
    hshm::charbuf new_name(sizeof(TagId) + blob_name.size());
    chi::LocalSerialize srl(new_name);
    srl << tag_id;
    srl << blob_name;
    return new_name;
  }

  /** Return the unique blob name for blob_id_map */
  hshm::charbuf GetBlobNameWithBucket() {
    return GetBlobNameWithBucket(tag_id_, name_);
  }
};

/** Data structure used to store Bucket information */
struct TagInfo {
  TagId tag_id_;
  hshm::charbuf name_;
  std::list<BlobId> blobs_;
  std::list<Task *> traits_;
  size_t internal_size_;
  size_t page_size_;
  bitfield32_t flags_;
  bool owner_;
  chi::CoRwLock lock_;

  /** Serialization */
  template<typename Ar>
  void serialize(Ar &ar) {
    ar(tag_id_, name_, internal_size_, page_size_, owner_, flags_);
  }

  /** Get std::string of name */
  std::vector<char> GetName() {
    std::vector<char> data(name_.size());
    memcpy(data.data(), name_.data(), name_.size());
    return data;
  }
};

}  // namespace hermes

#endif  // HERMES_INCLUDE_HERMES_HERMES_RUN_TYPES_H_
