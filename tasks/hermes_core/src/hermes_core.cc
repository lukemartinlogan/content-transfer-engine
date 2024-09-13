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

#include "chimaera_admin/chimaera_admin.h"
#include "chimaera/api/chimaera_runtime.h"
#include "chimaera/work_orchestrator/work_orchestrator.h"
#include "bdev/bdev.h"
#include "hermes_core/hermes_core.h"
#include "hermes/hermes.h"
#include "hermes/dpe/dpe_factory.h"

namespace hermes {

#define HERMES_LANES 32

/** Type name simplification for the various map types */
typedef std::unordered_map<hshm::charbuf, TagId> TAG_ID_MAP_T;
typedef std::unordered_map<TagId, TagInfo> TAG_MAP_T;
typedef std::unordered_map<hshm::charbuf, BlobId> BLOB_ID_MAP_T;
typedef std::unordered_map<BlobId, BlobInfo> BLOB_MAP_T;
typedef hipc::mpsc_queue<IoStat> IO_PATTERN_LOG_T;

struct HermesLane {
  TAG_ID_MAP_T tag_id_map_;
  TAG_MAP_T tag_map_;
  BLOB_ID_MAP_T blob_id_map_;
  BLOB_MAP_T blob_map_;
};

class Server : public Module {
 public:
  Client client_;
  std::vector<HermesLane> tls_;
  std::atomic<u64> id_alloc_;
  std::vector<TargetInfo> targets_;
  std::unordered_map<TargetId, TargetInfo*> target_map_;
  TargetInfo *fallback_target_;

 private:
  /** Get the globally unique blob name */
  const hshm::charbuf GetBlobNameWithBucket(
      TagId tag_id, const hshm::charbuf &blob_name) {
    hshm::charbuf new_name(sizeof(TagId) + blob_name.size());
    chi::LocalSerialize srl(new_name);
    srl << tag_id;
    srl << blob_name;
    return new_name;
  }

 public:
  Server() = default;

  /** Construct hermes_core */
  void Create(CreateTask *task, RunContext &rctx) {
    // Create a set of lanes for holding tasks
    HERMES_CONF->ServerInit();
    client_.Init(id_);
    CreateLaneGroup(0, HERMES_LANES, QUEUE_LOW_LATENCY);
    tls_.resize(HERMES_LANES);
    // Create block devices
    for (int i = 0; i < 3; ++i) {
      for (DeviceInfo &dev : HERMES_SERVER_CONF.devices_) {
        std::string dev_type;
        if (dev.mount_dir_.empty()) {
          dev_type = "ram";
          dev.mount_point_ =
              hshm::Formatter::format("{}/{}", dev.mount_dir_, dev.dev_name_);
        } else {
          dev_type = "fs";
        }
        targets_.emplace_back();
        TargetInfo &target = targets_.back();
        target.client_.Create(
            DomainQuery::GetDirectHash(
                chi::SubDomainId::kGlobalContainers, CHI_CLIENT->node_id_ + i),
            DomainQuery::GetDirectHash(
                chi::SubDomainId::kGlobalContainers, CHI_CLIENT->node_id_ + i),
            hshm::Formatter::format(
                "hermes_{}_{}/{}",
                dev_type, dev.dev_name_, CHI_CLIENT->node_id_),
            hshm::Formatter::format("{}://{}", dev_type, dev.mount_point_),
            dev.capacity_);
        target.id_ = target.client_.id_;
        target.poll_stats_ = target.client_.AsyncPollStats(
            chi::DomainQuery::GetDirectHash(
                chi::SubDomainId::kGlobalContainers, 0), 25);
        target.stats_ = &target.poll_stats_->stats_;
        target_map_[target.id_] = &target;
      }
    }
    fallback_target_ = &targets_.back();
    task->SetModuleComplete();
  }
  void MonitorCreate(MonitorModeId mode, CreateTask *task, RunContext &rctx) {
  }

  /** Route a task to a lane */
  Lane* Route(const Task *task) override {
    // Route tasks to lanes based on their properties
    // E.g., a strongly consistent filesystem could map tasks to a lane
    // by the hash of an absolute filename path.
    return GetLaneByHash(0, 0);
  }

  /** Destroy hermes_core */
  void Destroy(DestroyTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestroy(MonitorModeId mode, DestroyTask *task, RunContext &rctx) {
  }

  /**
   * ========================================
   * TAG Methods
   * ========================================
   * */

  /** Get or create a tag */
  void GetOrCreateTag(GetOrCreateTagTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    // Check if the tag exists
    TAG_ID_MAP_T &tag_id_map =
        tls.tag_id_map_;
    hshm::string tag_name = hshm::to_charbuf(task->tag_name_);
    // hshm::charbuf tag_name = data_stager::Client::GetTagNameFromUrl(url);
    bool did_create = false;
    if (tag_name.size() > 0) {
      did_create = tag_id_map.find(tag_name) == tag_id_map.end();
    }

    // Emplace bucket if it does not already exist
    TagId tag_id;
    if (did_create) {
      TAG_MAP_T &tag_map = tls.tag_map_;
      tag_id.unique_ = id_alloc_.fetch_add(1);
      tag_id.hash_ = HashBucketName(tag_name);
      tag_id.node_id_ = CHI_CLIENT->node_id_;
      HILOG(kDebug, "Creating tag for the first time: {} {}", tag_name.str(), tag_id)
      tag_id_map.emplace(tag_name, tag_id);
      tag_map.emplace(tag_id, TagInfo());
      TagInfo &tag_info = tag_map[tag_id];
      tag_info.name_ = tag_name;
      tag_info.tag_id_ = tag_id;
      tag_info.owner_ = task->blob_owner_;
      tag_info.internal_size_ = task->backend_size_;
    } else {
      if (tag_name.size()) {
        HILOG(kDebug, "Found existing tag: {}", tag_name.str())
        tag_id = tag_id_map[tag_name];
      } else {
        HILOG(kDebug, "Found existing tag: {}", task->tag_id_)
        tag_id = task->tag_id_;
      }
    }

    task->tag_id_ = tag_id;
    // task->did_create_ = did_create;
    task->SetModuleComplete();
  }
  void MonitorGetOrCreateTag(MonitorModeId mode, GetOrCreateTagTask *task, RunContext &rctx) {
  }

  /** Get an existing tag ID */
  void GetTagId(GetTagIdTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    TAG_ID_MAP_T &tag_id_map = tls.tag_id_map_;
    hshm::charbuf tag_name = hshm::to_charbuf(task->tag_name_);
    auto it = tag_id_map.find(tag_name);
    if (it == tag_id_map.end()) {
      task->tag_id_ = TagId::GetNull();
      task->SetModuleComplete();
      return;
    }
    task->tag_id_ = it->second;
    task->SetModuleComplete();
  }
  void MonitorGetTagId(MonitorModeId mode, GetTagIdTask *task, RunContext &rctx) {
  }

  /** Get the name of a tag */
  void GetTagName(GetTagNameTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    TAG_MAP_T &tag_map = tls.tag_map_;
    auto it = tag_map.find(task->tag_id_);
    if (it == tag_map.end()) {
      task->SetModuleComplete();
      return;
    }
    task->tag_name_ = it->second.name_;
    task->SetModuleComplete();
  }
  void MonitorGetTagName(MonitorModeId mode, GetTagNameTask *task, RunContext &rctx) {
  }

  /** Destroy a tag (TODO) */
  void DestroyTag(DestroyTagTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    task->SetModuleComplete();
  }
  void MonitorDestroyTag(MonitorModeId mode, DestroyTagTask *task, RunContext &rctx) {
  }

  /** Add a blob to the tag */
  void TagAddBlob(TagAddBlobTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    TAG_MAP_T &tag_map = tls.tag_map_;
    auto it = tag_map.find(task->tag_id_);
    if (it == tag_map.end()) {
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    tag.blobs_.emplace_back(task->blob_id_);
    task->SetModuleComplete();
  }
  void MonitorTagAddBlob(MonitorModeId mode, TagAddBlobTask *task, RunContext &rctx) {
  }

  /** Remove a blob from the tag */
  void TagRemoveBlob(TagRemoveBlobTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    TAG_MAP_T &tag_map = tls.tag_map_;
    auto it = tag_map.find(task->tag_id_);
    if (it == tag_map.end()) {
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    auto blob_it = std::find(tag.blobs_.begin(), tag.blobs_.end(), task->blob_id_);
    tag.blobs_.erase(blob_it);
    task->SetModuleComplete();
  }
  void MonitorTagRemoveBlob(MonitorModeId mode, TagRemoveBlobTask *task, RunContext &rctx) {
  }

  /** Clear blobs from the tag */
  void TagClearBlobs(TagClearBlobsTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    TAG_MAP_T &tag_map = tls.tag_map_;
    auto it = tag_map.find(task->tag_id_);
    if (it == tag_map.end()) {
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    if (tag.owner_) {
      for (BlobId &blob_id : tag.blobs_) {
        // TODO(llogan)
        // client_.AsyncDestroyBlob(task->tag_id_, blob_id);
      }
    }
    tag.blobs_.clear();
    tag.internal_size_ = 0;
    task->SetModuleComplete();
  }
  void MonitorTagClearBlobs(MonitorModeId mode, TagClearBlobsTask *task, RunContext &rctx) {
  }

  /** Get the size of a tag */
  void TagGetSize(TagGetSizeTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    TAG_MAP_T &tag_map = tls.tag_map_;
    auto it = tag_map.find(task->tag_id_);
    if (it == tag_map.end()) {
      task->size_ = 0;
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    task->size_ = tag.internal_size_;
    task->SetModuleComplete();
  }
  void MonitorTagGetSize(MonitorModeId mode, TagGetSizeTask *task, RunContext &rctx) {
  }

  /** Update the size of a tag */
  void TagUpdateSize(TagUpdateSizeTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    TAG_MAP_T &tag_map = tls.tag_map_;
    TagInfo &tag_info = tag_map[task->tag_id_];
    ssize_t internal_size = (ssize_t) tag_info.internal_size_;
    if (task->mode_ == UpdateSizeMode::kAdd) {
      internal_size += task->update_;
    } else {
      internal_size = std::max(task->update_, internal_size);
    }
    HILOG(kDebug, "Updating size of tag {} from {} to {} with update {} (mode={})",
          task->tag_id_, tag_info.internal_size_, internal_size, task->update_, task->mode_)
    tag_info.internal_size_ = (size_t) internal_size;
    task->SetModuleComplete();
  }
  void MonitorTagUpdateSize(MonitorModeId mode, TagUpdateSizeTask *task, RunContext &rctx) {
  }

  /** Get the set of blobs in the tag */
  void TagGetContainedBlobIds(TagGetContainedBlobIdsTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    TAG_MAP_T &tag_map = tls.tag_map_;
    auto it = tag_map.find(task->tag_id_);
    if (it == tag_map.end()) {
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    hipc::vector<BlobId> &blobs = task->blob_ids_;
    blobs.reserve(tag.blobs_.size());
    for (BlobId &blob_id : tag.blobs_) {
      blobs.emplace_back(blob_id);
    }
    task->SetModuleComplete();
  }
  void MonitorTagGetContainedBlobIds(MonitorModeId mode, TagGetContainedBlobIdsTask *task, RunContext &rctx) {
  }

  /**
   * ========================================
   * BLOB Methods
   * ========================================
   * */

  /** Get or create a blob ID */
  BlobId GetOrCreateBlobId(HermesLane &tls, TagId &tag_id, u32 name_hash,
                           const hshm::charbuf &blob_name,
                           bitfield32_t &flags) {
    hshm::charbuf blob_name_unique = GetBlobNameWithBucket(tag_id, blob_name);
    BLOB_ID_MAP_T &blob_id_map = tls.blob_id_map_;
    auto it = blob_id_map.find(blob_name_unique);
    if (it == blob_id_map.end()) {
      BlobId blob_id = BlobId(CHI_CLIENT->node_id_, name_hash,
                              id_alloc_.fetch_add(1));
      blob_id_map.emplace(blob_name_unique, blob_id);
      flags.SetBits(HERMES_BLOB_DID_CREATE);
      BLOB_MAP_T &blob_map = tls.blob_map_;
      blob_map.emplace(blob_id, BlobInfo());
      BlobInfo &blob_info = blob_map[blob_id];
      blob_info.name_ = blob_name;
      blob_info.blob_id_ = blob_id;
      blob_info.tag_id_ = tag_id;
      blob_info.blob_size_ = 0;
      blob_info.max_blob_size_ = 0;
      blob_info.score_ = 1;
      blob_info.mod_count_ = 0;
      blob_info.access_freq_ = 0;
      blob_info.last_flush_ = 0;
      return blob_id;
    }
    return it->second;
  }
  void GetOrCreateBlobId(GetOrCreateBlobIdTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    hshm::charbuf blob_name = hshm::to_charbuf(task->blob_name_);
    bitfield32_t flags;
    task->blob_id_ = GetOrCreateBlobId(
        tls, task->tag_id_,
        HashBlobName(task->tag_id_, blob_name),
        blob_name, flags);
    task->SetModuleComplete();
  }
  void MonitorGetOrCreateBlobId(MonitorModeId mode,
                                GetOrCreateBlobIdTask *task,
                                RunContext &rctx) {
  }

  /** Get the blob ID */
  void GetBlobId(GetBlobIdTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    hshm::charbuf blob_name = hshm::to_charbuf(task->blob_name_);
    hshm::charbuf blob_name_unique = GetBlobNameWithBucket(task->tag_id_, blob_name);
    BLOB_ID_MAP_T &blob_id_map = tls.blob_id_map_;
    auto it = blob_id_map.find(blob_name_unique);
    if (it == blob_id_map.end()) {
      task->blob_id_ = BlobId::GetNull();
      task->SetModuleComplete();
      HILOG(kDebug, "Failed to find blob {} in {}", blob_name.str(), task->tag_id_);
      return;
    }
    task->blob_id_ = it->second;
    task->SetModuleComplete();
  }
  void MonitorGetBlobId(MonitorModeId mode,
                        GetBlobIdTask *task,
                        RunContext &rctx) {
  }

  /** Get blob name */
  void GetBlobName(GetBlobNameTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    BLOB_MAP_T &blob_map = tls.blob_map_;
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    task->blob_name_ = blob.name_;
    task->SetModuleComplete();
  }
  void MonitorGetBlobName(MonitorModeId mode,
                          GetBlobNameTask *task,
                          RunContext &rctx) {
  }

  /** Get the blob size */
  void GetBlobSize(GetBlobSizeTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    if (task->blob_id_.IsNull()) {
      bitfield32_t flags;
      task->blob_id_ = GetOrCreateBlobId(
          tls, task->tag_id_,
          HashBlobName(task->tag_id_, task->blob_name_),
          hshm::to_charbuf(task->blob_name_), flags);
    }
    BLOB_MAP_T &blob_map = tls.blob_map_;
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->size_ = 0;
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    task->size_ = blob.blob_size_;
    task->SetModuleComplete();
  }
  void MonitorGetBlobSize(MonitorModeId mode, GetBlobSizeTask *task, RunContext &rctx) {
  }

  /** Get the score of a blob */
  void GetBlobScore(GetBlobScoreTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    BLOB_MAP_T &blob_map = tls.blob_map_;
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    task->score_ = blob.score_;
    task->SetModuleComplete();
  }
  void MonitorGetBlobScore(MonitorModeId mode, GetBlobScoreTask *task, RunContext &rctx) {
  }

  /** Get blob buffers */
  void GetBlobBuffers(GetBlobBuffersTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    BLOB_MAP_T &blob_map = tls.blob_map_;
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    task->buffers_ = blob.buffers_;
    task->SetModuleComplete();
  }
  void MonitorGetBlobBuffers(MonitorModeId mode, GetBlobBuffersTask *task, RunContext &rctx) {
  }

  /** Put a blob (TODO) */
  void PutBlob(PutBlobTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    // Get blob ID
    hshm::charbuf blob_name = hshm::to_charbuf(task->blob_name_);
    if (task->blob_id_.IsNull()) {
      task->blob_id_ = GetOrCreateBlobId(
          tls, task->tag_id_,
          HashBlobName(task->tag_id_, blob_name),
          blob_name, task->flags_);
    }
    // Get blob struct
    BLOB_MAP_T &blob_map = tls.blob_map_;
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob_info = it->second;

    // Determine amount of additional buffering space needed
    ssize_t bkt_size_diff = 0;
    size_t needed_space = task->blob_off_ + task->data_size_;
    size_t size_diff = 0;
    if (needed_space > blob_info.max_blob_size_) {
      size_diff = needed_space - blob_info.max_blob_size_;
    }
    size_t min_blob_size = task->blob_off_ + task->data_size_;
    if (min_blob_size > blob_info.blob_size_) {
      blob_info.blob_size_ = task->blob_off_ + task->data_size_;
    }
    bkt_size_diff += (ssize_t)size_diff;
    HILOG(kDebug, "The size diff is {} bytes (bkt diff {})", size_diff, bkt_size_diff)

    // Use DPE
    std::vector<TargetInfo> targets = targets_;
    std::vector<PlacementSchema> schema_vec;
    if (size_diff > 0) {
      Context ctx;
      auto *dpe = DpeFactory::Get(ctx.dpe_);
      ctx.blob_score_ = task->score_;
      dpe->Placement({size_diff}, targets, ctx, schema_vec);
    }

    // Allocate blob buffers
    for (PlacementSchema &schema : schema_vec) {
      schema.plcmnts_.emplace_back(0, fallback_target_->id_);
      for (size_t sub_idx = 0; sub_idx < schema.plcmnts_.size(); ++sub_idx) {
        // Allocate chi::blocks
        SubPlacement &placement = schema.plcmnts_[sub_idx];
        TargetInfo &bdev = *target_map_[placement.tid_];
        if (placement.size_ == 0) {
          continue;
        }
        std::vector<chi::Block> blocks = bdev.client_.Allocate(
                chi::DomainQuery::GetDirectHash(
                    chi::SubDomainId::kGlobalContainers, bdev.id_.node_id_),
                placement.size_);
        // Convert to BufferInfo
        size_t t_alloc = 0;
        for (chi::Block &block : blocks) {
          blob_info.buffers_.emplace_back(placement.tid_, block);
          t_alloc += block.size_;
        }
//        HILOG(kInfo, "(node {}) Placing {}/{} bytes in target {} of bw {}",
//              CHI_CLIENT->node_id_,
//              alloc_task->alloc_size_, task->data_size_,
//              placement.tid_, bdev.bandwidth_)
        // Spill to next tier
        if (t_alloc < placement.size_) {
          SubPlacement &next_placement = schema.plcmnts_[sub_idx + 1];
          size_t diff = placement.size_ - t_alloc;
          next_placement.size_ += diff;
        }
        bdev.stats_->free_ -= t_alloc;
      }
    }

    // Place blob in buffers
    std::vector<LPointer<chi::bdev::WriteTask>> write_tasks;
    write_tasks.reserve(blob_info.buffers_.size());
    size_t blob_off = task->blob_off_, buf_off = 0;
    size_t buf_left = 0, buf_right = 0;
    size_t blob_right = task->blob_off_ + task->data_size_;
    HILOG(kDebug, "Number of buffers {}", blob_info.buffers_.size());
    bool found_left = false;
    for (BufferInfo &buf : blob_info.buffers_) {
      buf_right = buf_left + buf.size_;
      if (blob_off >= blob_right) {
        break;
      }
      if (buf_left <= blob_off && blob_off < buf_right) {
        found_left = true;
      }
      if (found_left) {
        size_t rel_off = blob_off - buf_left;
        size_t tgt_off = buf.off_ + rel_off;
        size_t buf_size = buf.size_ - rel_off;
        if (buf_right > blob_right) {
          buf_size = blob_right - (buf_left + rel_off);
        }
        HILOG(kDebug, "Writing {} bytes at off {} from target {}", buf_size, tgt_off, buf.tid_)
        TargetInfo &target = *target_map_[buf.tid_];
        LPointer<chi::bdev::WriteTask> write_task =
            target.client_.AsyncWrite(
                chi::DomainQuery::GetDirectHash(
                    chi::SubDomainId::kGlobalContainers, 0),
                task->data_ + buf_off,
                tgt_off, buf_size);
        write_tasks.emplace_back(write_task);
        buf_off += buf_size;
        blob_off = buf_right;
      }
      buf_left += buf.size_;
    }
    blob_info.max_blob_size_ = blob_off;

    // Wait for the placements to complete
    for (LPointer<chi::bdev::WriteTask> &write_task : write_tasks) {
      task->Wait(write_task);
      CHI_CLIENT->DelTask(write_task);
    }

    // Update information
    client_.AsyncTagUpdateSize(
        chi::DomainQuery::GetDirectHash(
            chi::SubDomainId::kGlobalContainers, 0),
        task->tag_id_,
        bkt_size_diff,
        UpdateSizeMode::kAdd);
//    if (task->flags_.Any(HERMES_SHOULD_STAGE)) {
//      client_.AsyncTagUpdateSize(
//          chi::DomainQuery::GetDirectHash(
//              chi::SubDomainId::kGlobalContainers, 0),
//          task->tag_id_,
//          blob_info.name_,
//          task->blob_off_,
//          task->data_size_, 0);
//    } else {
//      client_.AsyncTagUpdateSize(
//          chi::DomainQuery::GetDirectHash(
//              chi::SubDomainId::kGlobalContainers, 0),
//          task->tag_id_,
//          bkt_size_diff,
//          UpdateSizeMode::kAdd);
//    }
    if (task->flags_.Any(HERMES_BLOB_DID_CREATE)) {
      client_.AsyncTagAddBlob(
          chi::DomainQuery::GetDirectHash(
              chi::SubDomainId::kGlobalContainers, 0),
          task->tag_id_,
          task->blob_id_);
    }
//    if (task->flags_.Any(HERMES_HAS_DERIVED)) {
//      op_mdm_.AsyncRegisterData(task->task_node_ + 1,
//                                task->tag_id_,
//                                task->blob_name_->str(),
//                                task->blob_id_,
//                                task->blob_off_,
//                                task->data_size_);
//    }

    // Free data
    HILOG(kDebug, "Completing PUT for {}", blob_name.str());
    blob_info.UpdateWriteStats();
    task->SetModuleComplete();
  }
  void MonitorPutBlob(MonitorModeId mode, PutBlobTask *task, RunContext &rctx) {
  }

  /** Get a blob (TODO) */
  void GetBlob(GetBlobTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];

    if (task->blob_id_.IsNull()) {
      hshm::charbuf blob_name = hshm::to_charbuf(task->blob_name_);
      task->blob_id_ = GetOrCreateBlobId(
          tls, task->tag_id_,
          HashBlobName(task->tag_id_, blob_name),
          blob_name, task->flags_);
    }
    BLOB_MAP_T &blob_map = tls.blob_map_;
    BlobInfo &blob_info = blob_map[task->blob_id_];

    // Stage Blob
//    if (task->flags_.Any(HERMES_SHOULD_STAGE) && blob_info.last_flush_ == 0) {
//      // TODO(llogan): Don't hardcore score = 1
//      blob_info.last_flush_ = 1;
//      LPointer<data_stager::StageInTask> stage_task =
//          stager_mdm_.AsyncStageIn(task->task_node_ + 1,
//                                   task->tag_id_,
//                                   blob_info.name_,
//                                   1, 0);
//      stage_task->Wait<TASK_YIELD_CO>(task);
//      CHI_CLIENT->DelTask(stage_task);
//    }

    // Read blob from buffers
    std::vector<chi::bdev::ReadTask*> read_tasks;
    read_tasks.reserve(blob_info.buffers_.size());
    HILOG(kDebug, "Getting blob {} of size {} starting at offset {} (total_blob_size={}, buffers={})",
          task->blob_id_, task->data_size_, task->blob_off_, blob_info.blob_size_, blob_info.buffers_.size());
    size_t blob_off = task->blob_off_;
    size_t buf_left = 0, buf_right = 0;
    size_t buf_off = 0;
    size_t blob_right = task->blob_off_ + task->data_size_;
    bool found_left = false;
    for (BufferInfo &buf : blob_info.buffers_) {
      buf_right = buf_left + buf.size_;
      if (blob_off >= blob_right) {
        break;
      }
      if (buf_left <= blob_off && blob_off < buf_right) {
        found_left = true;
      }
      if (found_left) {
        size_t rel_off = blob_off - buf_left;
        size_t tgt_off = buf.off_ + rel_off;
        size_t buf_size = buf.size_ - rel_off;
        if (buf_right > blob_right) {
          buf_size = blob_right - (buf_left + rel_off);
        }
        HILOG(kDebug, "Loading {} bytes at off {} from target {}", buf_size, tgt_off, buf.tid_)
        TargetInfo &target = *target_map_[buf.tid_];
        chi::bdev::ReadTask *read_task = target.client_.AsyncRead(
            chi::DomainQuery::GetDirectHash(
                chi::SubDomainId::kGlobalContainers, 0),
            task->data_ + buf_off,
            tgt_off, buf_size).ptr_;
        read_tasks.emplace_back(read_task);
        buf_off += buf_size;
        blob_off = buf_right;
      }
      buf_left += buf.size_;
    }
    for (chi::bdev::ReadTask *&read_task : read_tasks) {
      task->Wait(read_task);
      CHI_CLIENT->DelTask(read_task);
    }
    task->data_size_ = buf_off;
    task->SetModuleComplete();
  }
  void MonitorGetBlob(MonitorModeId mode, GetBlobTask *task, RunContext &rctx) {
  }

  /** Truncate a blob (TODO) */
  void TruncateBlob(TruncateBlobTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    task->SetModuleComplete();
  }
  void MonitorTruncateBlob(MonitorModeId mode, TruncateBlobTask *task, RunContext &rctx) {
  }

  /** Destroy blob (TODO) */
  void DestroyBlob(DestroyBlobTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    task->SetModuleComplete();
  }
  void MonitorDestroyBlob(MonitorModeId mode, DestroyBlobTask *task, RunContext &rctx) {
  }

  /** Tag a blob */
  void TagBlob(TagBlobTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    BLOB_MAP_T &blob_map = tls.blob_map_;
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    blob.tags_.push_back(task->tag_);
    task->SetModuleComplete();
  }
  void MonitorTagBlob(MonitorModeId mode, TagBlobTask *task, RunContext &rctx) {
  }

  /** Check if blob has a tag */
  void BlobHasTag(BlobHasTagTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    BLOB_MAP_T &blob_map = tls.blob_map_;
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    task->has_tag_ = std::find(blob.tags_.begin(),
                               blob.tags_.end(),
                               task->tag_) != blob.tags_.end();
    task->SetModuleComplete();
  }
  void MonitorBlobHasTag(MonitorModeId mode, BlobHasTagTask *task, RunContext &rctx) {
  }

  /** Change blob composition (TODO) */
  void ReorganizeBlob(ReorganizeBlobTask *task, RunContext &rctx) {
    HermesLane &tls = tls_[CHI_CUR_LANE->lane_id_.unique_];
    task->SetModuleComplete();
  }
  void MonitorReorganizeBlob(MonitorModeId mode, ReorganizeBlobTask *task, RunContext &rctx) {
  }
//  void FlushData(FlushDataTask *task, RunContext &rctx) {
//    task->SetModuleComplete();
//  }
//  void MonitorFlushData(MonitorModeId mode, FlushDataTask *task, RunContext &rctx) {
//  }
//  void PollBlobMetadata(PollBlobMetadataTask *task, RunContext &rctx) {
//    task->SetModuleComplete();
//  }
//  void MonitorPollBlobMetadata(MonitorModeId mode, PollBlobMetadataTask *task, RunContext &rctx) {
//  }
//  void PollTargetMetadata(PollTargetMetadataTask *task, RunContext &rctx) {
//    task->SetModuleComplete();
//  }
//  void MonitorPollTargetMetadata(MonitorModeId mode, PollTargetMetadataTask *task, RunContext &rctx) {
//  }

 public:
#include "hermes_core/hermes_core_lib_exec.h"
};

}  // namespace hermes

CHI_TASK_CC(hermes::Server, "hermes_core");
