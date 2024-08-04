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
#include "hermes_core/hermes_core.h"

namespace hermes {

class Server : public Module {
 public:
  Server() = default;

  /** Construct hermes_core */
  void Create(CreateTask *task, RunContext &rctx) {
    // Create a set of lanes for holding tasks
    CreateLaneGroup(0, 1, QUEUE_LOW_LATENCY);
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

  void GetOrCreateTag(GetOrCreateTagTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetOrCreateTag(MonitorModeId mode, GetOrCreateTagTask *task, RunContext &rctx) {
  }
  void GetTagId(GetTagIdTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetTagId(MonitorModeId mode, GetTagIdTask *task, RunContext &rctx) {
  }
  void GetTagName(GetTagNameTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetTagName(MonitorModeId mode, GetTagNameTask *task, RunContext &rctx) {
  }
  void DestroyTag(DestroyTagTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestroyTag(MonitorModeId mode, DestroyTagTask *task, RunContext &rctx) {
  }
  void TagAddBlob(TagAddBlobTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorTagAddBlob(MonitorModeId mode, TagAddBlobTask *task, RunContext &rctx) {
  }
  void TagRemoveBlob(TagRemoveBlobTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorTagRemoveBlob(MonitorModeId mode, TagRemoveBlobTask *task, RunContext &rctx) {
  }
  void TagClearBlobs(TagClearBlobsTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorTagClearBlobs(MonitorModeId mode, TagClearBlobsTask *task, RunContext &rctx) {
  }
  void TagGetSize(TagGetSizeTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorTagGetSize(MonitorModeId mode, TagGetSizeTask *task, RunContext &rctx) {
  }
  void TagUpdateSize(TagUpdateSizeTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorTagUpdateSize(MonitorModeId mode, TagUpdateSizeTask *task, RunContext &rctx) {
  }
  void TagGetContainedBlobIds(TagGetContainedBlobIdsTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorTagGetContainedBlobIds(MonitorModeId mode, TagGetContainedBlobIdsTask *task, RunContext &rctx) {
  }

  /**
   * ========================================
   * BLOB Methods
   * ========================================
   * */

  void GetOrCreateBlob(GetOrCreateBlobTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetOrCreateBlob(MonitorModeId mode, GetOrCreateBlobTask *task, RunContext &rctx) {
  }
  void GetBlobId(GetBlobIdTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetBlobId(MonitorModeId mode, GetBlobIdTask *task, RunContext &rctx) {
  }
  void GetBlobName(GetBlobNameTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetBlobName(MonitorModeId mode, GetBlobNameTask *task, RunContext &rctx) {
  }
  void GetBlobSize(GetBlobSizeTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetBlobSize(MonitorModeId mode, GetBlobSizeTask *task, RunContext &rctx) {
  }
  void GetBlobScore(GetBlobScoreTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetBlobScore(MonitorModeId mode, GetBlobScoreTask *task, RunContext &rctx) {
  }
  void GetBlobBuffers(GetBlobBuffersTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetBlobBuffers(MonitorModeId mode, GetBlobBuffersTask *task, RunContext &rctx) {
  }
  void PutBlob(PutBlobTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorPutBlob(MonitorModeId mode, PutBlobTask *task, RunContext &rctx) {
  }
  void GetBlob(GetBlobTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorGetBlob(MonitorModeId mode, GetBlobTask *task, RunContext &rctx) {
  }
  void TruncateBlob(TruncateBlobTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorTruncateBlob(MonitorModeId mode, TruncateBlobTask *task, RunContext &rctx) {
  }
  void DestroyBlob(DestroyBlobTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestroyBlob(MonitorModeId mode, DestroyBlobTask *task, RunContext &rctx) {
  }
  void TagBlob(TagBlobTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorTagBlob(MonitorModeId mode, TagBlobTask *task, RunContext &rctx) {
  }
  void BlobHasTag(BlobHasTagTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorBlobHasTag(MonitorModeId mode, BlobHasTagTask *task, RunContext &rctx) {
  }
  void ReorganizeBlob(ReorganizeBlobTask *task, RunContext &rctx) {
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
