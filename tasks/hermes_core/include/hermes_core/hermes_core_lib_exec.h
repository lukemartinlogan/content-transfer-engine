#ifndef CHI_HERMES_CORE_LIB_EXEC_H_
#define CHI_HERMES_CORE_LIB_EXEC_H_

/** Execute a task */
void Run(u32 method, Task *task, RunContext &rctx) override {
  switch (method) {
    case Method::kCreate: {
      Create(reinterpret_cast<CreateTask *>(task), rctx);
      break;
    }
    case Method::kDestroy: {
      Destroy(reinterpret_cast<DestroyTask *>(task), rctx);
      break;
    }
    case Method::kGetOrCreateTag: {
      GetOrCreateTag(reinterpret_cast<GetOrCreateTagTask *>(task), rctx);
      break;
    }
    case Method::kGetTagId: {
      GetTagId(reinterpret_cast<GetTagIdTask *>(task), rctx);
      break;
    }
    case Method::kGetTagName: {
      GetTagName(reinterpret_cast<GetTagNameTask *>(task), rctx);
      break;
    }
    case Method::kDestroyTag: {
      DestroyTag(reinterpret_cast<DestroyTagTask *>(task), rctx);
      break;
    }
    case Method::kTagAddBlob: {
      TagAddBlob(reinterpret_cast<TagAddBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagRemoveBlob: {
      TagRemoveBlob(reinterpret_cast<TagRemoveBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagClearBlobs: {
      TagClearBlobs(reinterpret_cast<TagClearBlobsTask *>(task), rctx);
      break;
    }
    case Method::kTagGetSize: {
      TagGetSize(reinterpret_cast<TagGetSizeTask *>(task), rctx);
      break;
    }
    case Method::kTagUpdateSize: {
      TagUpdateSize(reinterpret_cast<TagUpdateSizeTask *>(task), rctx);
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      TagGetContainedBlobIds(reinterpret_cast<TagGetContainedBlobIdsTask *>(task), rctx);
      break;
    }
    case Method::kGetOrCreateBlob: {
      GetOrCreateBlob(reinterpret_cast<GetOrCreateBlobTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobId: {
      GetBlobId(reinterpret_cast<GetBlobIdTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobName: {
      GetBlobName(reinterpret_cast<GetBlobNameTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobSize: {
      GetBlobSize(reinterpret_cast<GetBlobSizeTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobScore: {
      GetBlobScore(reinterpret_cast<GetBlobScoreTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobBuffers: {
      GetBlobBuffers(reinterpret_cast<GetBlobBuffersTask *>(task), rctx);
      break;
    }
    case Method::kPutBlob: {
      PutBlob(reinterpret_cast<PutBlobTask *>(task), rctx);
      break;
    }
    case Method::kGetBlob: {
      GetBlob(reinterpret_cast<GetBlobTask *>(task), rctx);
      break;
    }
    case Method::kTruncateBlob: {
      TruncateBlob(reinterpret_cast<TruncateBlobTask *>(task), rctx);
      break;
    }
    case Method::kDestroyBlob: {
      DestroyBlob(reinterpret_cast<DestroyBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagBlob: {
      TagBlob(reinterpret_cast<TagBlobTask *>(task), rctx);
      break;
    }
    case Method::kBlobHasTag: {
      BlobHasTag(reinterpret_cast<BlobHasTagTask *>(task), rctx);
      break;
    }
    case Method::kReorganizeBlob: {
      ReorganizeBlob(reinterpret_cast<ReorganizeBlobTask *>(task), rctx);
      break;
    }
  }
}
/** Execute a task */
void Monitor(MonitorModeId mode, MethodId method, Task *task, RunContext &rctx) override {
  switch (method) {
    case Method::kCreate: {
      MonitorCreate(mode, reinterpret_cast<CreateTask *>(task), rctx);
      break;
    }
    case Method::kDestroy: {
      MonitorDestroy(mode, reinterpret_cast<DestroyTask *>(task), rctx);
      break;
    }
    case Method::kGetOrCreateTag: {
      MonitorGetOrCreateTag(mode, reinterpret_cast<GetOrCreateTagTask *>(task), rctx);
      break;
    }
    case Method::kGetTagId: {
      MonitorGetTagId(mode, reinterpret_cast<GetTagIdTask *>(task), rctx);
      break;
    }
    case Method::kGetTagName: {
      MonitorGetTagName(mode, reinterpret_cast<GetTagNameTask *>(task), rctx);
      break;
    }
    case Method::kDestroyTag: {
      MonitorDestroyTag(mode, reinterpret_cast<DestroyTagTask *>(task), rctx);
      break;
    }
    case Method::kTagAddBlob: {
      MonitorTagAddBlob(mode, reinterpret_cast<TagAddBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagRemoveBlob: {
      MonitorTagRemoveBlob(mode, reinterpret_cast<TagRemoveBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagClearBlobs: {
      MonitorTagClearBlobs(mode, reinterpret_cast<TagClearBlobsTask *>(task), rctx);
      break;
    }
    case Method::kTagGetSize: {
      MonitorTagGetSize(mode, reinterpret_cast<TagGetSizeTask *>(task), rctx);
      break;
    }
    case Method::kTagUpdateSize: {
      MonitorTagUpdateSize(mode, reinterpret_cast<TagUpdateSizeTask *>(task), rctx);
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      MonitorTagGetContainedBlobIds(mode, reinterpret_cast<TagGetContainedBlobIdsTask *>(task), rctx);
      break;
    }
    case Method::kGetOrCreateBlob: {
      MonitorGetOrCreateBlob(mode, reinterpret_cast<GetOrCreateBlobTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobId: {
      MonitorGetBlobId(mode, reinterpret_cast<GetBlobIdTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobName: {
      MonitorGetBlobName(mode, reinterpret_cast<GetBlobNameTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobSize: {
      MonitorGetBlobSize(mode, reinterpret_cast<GetBlobSizeTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobScore: {
      MonitorGetBlobScore(mode, reinterpret_cast<GetBlobScoreTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobBuffers: {
      MonitorGetBlobBuffers(mode, reinterpret_cast<GetBlobBuffersTask *>(task), rctx);
      break;
    }
    case Method::kPutBlob: {
      MonitorPutBlob(mode, reinterpret_cast<PutBlobTask *>(task), rctx);
      break;
    }
    case Method::kGetBlob: {
      MonitorGetBlob(mode, reinterpret_cast<GetBlobTask *>(task), rctx);
      break;
    }
    case Method::kTruncateBlob: {
      MonitorTruncateBlob(mode, reinterpret_cast<TruncateBlobTask *>(task), rctx);
      break;
    }
    case Method::kDestroyBlob: {
      MonitorDestroyBlob(mode, reinterpret_cast<DestroyBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagBlob: {
      MonitorTagBlob(mode, reinterpret_cast<TagBlobTask *>(task), rctx);
      break;
    }
    case Method::kBlobHasTag: {
      MonitorBlobHasTag(mode, reinterpret_cast<BlobHasTagTask *>(task), rctx);
      break;
    }
    case Method::kReorganizeBlob: {
      MonitorReorganizeBlob(mode, reinterpret_cast<ReorganizeBlobTask *>(task), rctx);
      break;
    }
  }
}
/** Delete a task */
void Del(u32 method, Task *task) override {
  switch (method) {
    case Method::kCreate: {
      CHI_CLIENT->DelTask<CreateTask>(reinterpret_cast<CreateTask *>(task));
      break;
    }
    case Method::kDestroy: {
      CHI_CLIENT->DelTask<DestroyTask>(reinterpret_cast<DestroyTask *>(task));
      break;
    }
    case Method::kGetOrCreateTag: {
      CHI_CLIENT->DelTask<GetOrCreateTagTask>(reinterpret_cast<GetOrCreateTagTask *>(task));
      break;
    }
    case Method::kGetTagId: {
      CHI_CLIENT->DelTask<GetTagIdTask>(reinterpret_cast<GetTagIdTask *>(task));
      break;
    }
    case Method::kGetTagName: {
      CHI_CLIENT->DelTask<GetTagNameTask>(reinterpret_cast<GetTagNameTask *>(task));
      break;
    }
    case Method::kDestroyTag: {
      CHI_CLIENT->DelTask<DestroyTagTask>(reinterpret_cast<DestroyTagTask *>(task));
      break;
    }
    case Method::kTagAddBlob: {
      CHI_CLIENT->DelTask<TagAddBlobTask>(reinterpret_cast<TagAddBlobTask *>(task));
      break;
    }
    case Method::kTagRemoveBlob: {
      CHI_CLIENT->DelTask<TagRemoveBlobTask>(reinterpret_cast<TagRemoveBlobTask *>(task));
      break;
    }
    case Method::kTagClearBlobs: {
      CHI_CLIENT->DelTask<TagClearBlobsTask>(reinterpret_cast<TagClearBlobsTask *>(task));
      break;
    }
    case Method::kTagGetSize: {
      CHI_CLIENT->DelTask<TagGetSizeTask>(reinterpret_cast<TagGetSizeTask *>(task));
      break;
    }
    case Method::kTagUpdateSize: {
      CHI_CLIENT->DelTask<TagUpdateSizeTask>(reinterpret_cast<TagUpdateSizeTask *>(task));
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      CHI_CLIENT->DelTask<TagGetContainedBlobIdsTask>(reinterpret_cast<TagGetContainedBlobIdsTask *>(task));
      break;
    }
    case Method::kGetOrCreateBlob: {
      CHI_CLIENT->DelTask<GetOrCreateBlobTask>(reinterpret_cast<GetOrCreateBlobTask *>(task));
      break;
    }
    case Method::kGetBlobId: {
      CHI_CLIENT->DelTask<GetBlobIdTask>(reinterpret_cast<GetBlobIdTask *>(task));
      break;
    }
    case Method::kGetBlobName: {
      CHI_CLIENT->DelTask<GetBlobNameTask>(reinterpret_cast<GetBlobNameTask *>(task));
      break;
    }
    case Method::kGetBlobSize: {
      CHI_CLIENT->DelTask<GetBlobSizeTask>(reinterpret_cast<GetBlobSizeTask *>(task));
      break;
    }
    case Method::kGetBlobScore: {
      CHI_CLIENT->DelTask<GetBlobScoreTask>(reinterpret_cast<GetBlobScoreTask *>(task));
      break;
    }
    case Method::kGetBlobBuffers: {
      CHI_CLIENT->DelTask<GetBlobBuffersTask>(reinterpret_cast<GetBlobBuffersTask *>(task));
      break;
    }
    case Method::kPutBlob: {
      CHI_CLIENT->DelTask<PutBlobTask>(reinterpret_cast<PutBlobTask *>(task));
      break;
    }
    case Method::kGetBlob: {
      CHI_CLIENT->DelTask<GetBlobTask>(reinterpret_cast<GetBlobTask *>(task));
      break;
    }
    case Method::kTruncateBlob: {
      CHI_CLIENT->DelTask<TruncateBlobTask>(reinterpret_cast<TruncateBlobTask *>(task));
      break;
    }
    case Method::kDestroyBlob: {
      CHI_CLIENT->DelTask<DestroyBlobTask>(reinterpret_cast<DestroyBlobTask *>(task));
      break;
    }
    case Method::kTagBlob: {
      CHI_CLIENT->DelTask<TagBlobTask>(reinterpret_cast<TagBlobTask *>(task));
      break;
    }
    case Method::kBlobHasTag: {
      CHI_CLIENT->DelTask<BlobHasTagTask>(reinterpret_cast<BlobHasTagTask *>(task));
      break;
    }
    case Method::kReorganizeBlob: {
      CHI_CLIENT->DelTask<ReorganizeBlobTask>(reinterpret_cast<ReorganizeBlobTask *>(task));
      break;
    }
  }
}
/** Duplicate a task */
void CopyStart(u32 method, const Task *orig_task, Task *dup_task, bool deep) override {
  switch (method) {
    case Method::kCreate: {
      chi::CALL_COPY_START(
        reinterpret_cast<const CreateTask*>(orig_task), 
        reinterpret_cast<CreateTask*>(dup_task), deep);
      break;
    }
    case Method::kDestroy: {
      chi::CALL_COPY_START(
        reinterpret_cast<const DestroyTask*>(orig_task), 
        reinterpret_cast<DestroyTask*>(dup_task), deep);
      break;
    }
    case Method::kGetOrCreateTag: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetOrCreateTagTask*>(orig_task), 
        reinterpret_cast<GetOrCreateTagTask*>(dup_task), deep);
      break;
    }
    case Method::kGetTagId: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetTagIdTask*>(orig_task), 
        reinterpret_cast<GetTagIdTask*>(dup_task), deep);
      break;
    }
    case Method::kGetTagName: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetTagNameTask*>(orig_task), 
        reinterpret_cast<GetTagNameTask*>(dup_task), deep);
      break;
    }
    case Method::kDestroyTag: {
      chi::CALL_COPY_START(
        reinterpret_cast<const DestroyTagTask*>(orig_task), 
        reinterpret_cast<DestroyTagTask*>(dup_task), deep);
      break;
    }
    case Method::kTagAddBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const TagAddBlobTask*>(orig_task), 
        reinterpret_cast<TagAddBlobTask*>(dup_task), deep);
      break;
    }
    case Method::kTagRemoveBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const TagRemoveBlobTask*>(orig_task), 
        reinterpret_cast<TagRemoveBlobTask*>(dup_task), deep);
      break;
    }
    case Method::kTagClearBlobs: {
      chi::CALL_COPY_START(
        reinterpret_cast<const TagClearBlobsTask*>(orig_task), 
        reinterpret_cast<TagClearBlobsTask*>(dup_task), deep);
      break;
    }
    case Method::kTagGetSize: {
      chi::CALL_COPY_START(
        reinterpret_cast<const TagGetSizeTask*>(orig_task), 
        reinterpret_cast<TagGetSizeTask*>(dup_task), deep);
      break;
    }
    case Method::kTagUpdateSize: {
      chi::CALL_COPY_START(
        reinterpret_cast<const TagUpdateSizeTask*>(orig_task), 
        reinterpret_cast<TagUpdateSizeTask*>(dup_task), deep);
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      chi::CALL_COPY_START(
        reinterpret_cast<const TagGetContainedBlobIdsTask*>(orig_task), 
        reinterpret_cast<TagGetContainedBlobIdsTask*>(dup_task), deep);
      break;
    }
    case Method::kGetOrCreateBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetOrCreateBlobTask*>(orig_task), 
        reinterpret_cast<GetOrCreateBlobTask*>(dup_task), deep);
      break;
    }
    case Method::kGetBlobId: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetBlobIdTask*>(orig_task), 
        reinterpret_cast<GetBlobIdTask*>(dup_task), deep);
      break;
    }
    case Method::kGetBlobName: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetBlobNameTask*>(orig_task), 
        reinterpret_cast<GetBlobNameTask*>(dup_task), deep);
      break;
    }
    case Method::kGetBlobSize: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetBlobSizeTask*>(orig_task), 
        reinterpret_cast<GetBlobSizeTask*>(dup_task), deep);
      break;
    }
    case Method::kGetBlobScore: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetBlobScoreTask*>(orig_task), 
        reinterpret_cast<GetBlobScoreTask*>(dup_task), deep);
      break;
    }
    case Method::kGetBlobBuffers: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetBlobBuffersTask*>(orig_task), 
        reinterpret_cast<GetBlobBuffersTask*>(dup_task), deep);
      break;
    }
    case Method::kPutBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const PutBlobTask*>(orig_task), 
        reinterpret_cast<PutBlobTask*>(dup_task), deep);
      break;
    }
    case Method::kGetBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const GetBlobTask*>(orig_task), 
        reinterpret_cast<GetBlobTask*>(dup_task), deep);
      break;
    }
    case Method::kTruncateBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const TruncateBlobTask*>(orig_task), 
        reinterpret_cast<TruncateBlobTask*>(dup_task), deep);
      break;
    }
    case Method::kDestroyBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const DestroyBlobTask*>(orig_task), 
        reinterpret_cast<DestroyBlobTask*>(dup_task), deep);
      break;
    }
    case Method::kTagBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const TagBlobTask*>(orig_task), 
        reinterpret_cast<TagBlobTask*>(dup_task), deep);
      break;
    }
    case Method::kBlobHasTag: {
      chi::CALL_COPY_START(
        reinterpret_cast<const BlobHasTagTask*>(orig_task), 
        reinterpret_cast<BlobHasTagTask*>(dup_task), deep);
      break;
    }
    case Method::kReorganizeBlob: {
      chi::CALL_COPY_START(
        reinterpret_cast<const ReorganizeBlobTask*>(orig_task), 
        reinterpret_cast<ReorganizeBlobTask*>(dup_task), deep);
      break;
    }
  }
}
/** Duplicate a task */
void NewCopyStart(u32 method, const Task *orig_task, LPointer<Task> &dup_task, bool deep) override {
  switch (method) {
    case Method::kCreate: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const CreateTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kDestroy: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const DestroyTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetOrCreateTag: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetOrCreateTagTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetTagId: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetTagIdTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetTagName: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetTagNameTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kDestroyTag: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const DestroyTagTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kTagAddBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const TagAddBlobTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kTagRemoveBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const TagRemoveBlobTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kTagClearBlobs: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const TagClearBlobsTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kTagGetSize: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const TagGetSizeTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kTagUpdateSize: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const TagUpdateSizeTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const TagGetContainedBlobIdsTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetOrCreateBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetOrCreateBlobTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetBlobId: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetBlobIdTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetBlobName: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetBlobNameTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetBlobSize: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetBlobSizeTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetBlobScore: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetBlobScoreTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetBlobBuffers: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetBlobBuffersTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kPutBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const PutBlobTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kGetBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const GetBlobTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kTruncateBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const TruncateBlobTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kDestroyBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const DestroyBlobTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kTagBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const TagBlobTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kBlobHasTag: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const BlobHasTagTask*>(orig_task), dup_task, deep);
      break;
    }
    case Method::kReorganizeBlob: {
      chi::CALL_NEW_COPY_START(reinterpret_cast<const ReorganizeBlobTask*>(orig_task), dup_task, deep);
      break;
    }
  }
}
/** Serialize a task when initially pushing into remote */
void SaveStart(u32 method, BinaryOutputArchive<true> &ar, Task *task) override {
  switch (method) {
    case Method::kCreate: {
      ar << *reinterpret_cast<CreateTask*>(task);
      break;
    }
    case Method::kDestroy: {
      ar << *reinterpret_cast<DestroyTask*>(task);
      break;
    }
    case Method::kGetOrCreateTag: {
      ar << *reinterpret_cast<GetOrCreateTagTask*>(task);
      break;
    }
    case Method::kGetTagId: {
      ar << *reinterpret_cast<GetTagIdTask*>(task);
      break;
    }
    case Method::kGetTagName: {
      ar << *reinterpret_cast<GetTagNameTask*>(task);
      break;
    }
    case Method::kDestroyTag: {
      ar << *reinterpret_cast<DestroyTagTask*>(task);
      break;
    }
    case Method::kTagAddBlob: {
      ar << *reinterpret_cast<TagAddBlobTask*>(task);
      break;
    }
    case Method::kTagRemoveBlob: {
      ar << *reinterpret_cast<TagRemoveBlobTask*>(task);
      break;
    }
    case Method::kTagClearBlobs: {
      ar << *reinterpret_cast<TagClearBlobsTask*>(task);
      break;
    }
    case Method::kTagGetSize: {
      ar << *reinterpret_cast<TagGetSizeTask*>(task);
      break;
    }
    case Method::kTagUpdateSize: {
      ar << *reinterpret_cast<TagUpdateSizeTask*>(task);
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      ar << *reinterpret_cast<TagGetContainedBlobIdsTask*>(task);
      break;
    }
    case Method::kGetOrCreateBlob: {
      ar << *reinterpret_cast<GetOrCreateBlobTask*>(task);
      break;
    }
    case Method::kGetBlobId: {
      ar << *reinterpret_cast<GetBlobIdTask*>(task);
      break;
    }
    case Method::kGetBlobName: {
      ar << *reinterpret_cast<GetBlobNameTask*>(task);
      break;
    }
    case Method::kGetBlobSize: {
      ar << *reinterpret_cast<GetBlobSizeTask*>(task);
      break;
    }
    case Method::kGetBlobScore: {
      ar << *reinterpret_cast<GetBlobScoreTask*>(task);
      break;
    }
    case Method::kGetBlobBuffers: {
      ar << *reinterpret_cast<GetBlobBuffersTask*>(task);
      break;
    }
    case Method::kPutBlob: {
      ar << *reinterpret_cast<PutBlobTask*>(task);
      break;
    }
    case Method::kGetBlob: {
      ar << *reinterpret_cast<GetBlobTask*>(task);
      break;
    }
    case Method::kTruncateBlob: {
      ar << *reinterpret_cast<TruncateBlobTask*>(task);
      break;
    }
    case Method::kDestroyBlob: {
      ar << *reinterpret_cast<DestroyBlobTask*>(task);
      break;
    }
    case Method::kTagBlob: {
      ar << *reinterpret_cast<TagBlobTask*>(task);
      break;
    }
    case Method::kBlobHasTag: {
      ar << *reinterpret_cast<BlobHasTagTask*>(task);
      break;
    }
    case Method::kReorganizeBlob: {
      ar << *reinterpret_cast<ReorganizeBlobTask*>(task);
      break;
    }
  }
}
/** Deserialize a task when popping from remote queue */
TaskPointer LoadStart(u32 method, BinaryInputArchive<true> &ar) override {
  TaskPointer task_ptr;
  switch (method) {
    case Method::kCreate: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<CreateTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<CreateTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestroy: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<DestroyTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestroyTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetOrCreateTag: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetOrCreateTagTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetOrCreateTagTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetTagId: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetTagIdTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetTagIdTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetTagName: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetTagNameTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetTagNameTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestroyTag: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<DestroyTagTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestroyTagTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagAddBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<TagAddBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagAddBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagRemoveBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<TagRemoveBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagRemoveBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagClearBlobs: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<TagClearBlobsTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagClearBlobsTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagGetSize: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<TagGetSizeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagGetSizeTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagUpdateSize: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<TagUpdateSizeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagUpdateSizeTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<TagGetContainedBlobIdsTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagGetContainedBlobIdsTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetOrCreateBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetOrCreateBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetOrCreateBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobId: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetBlobIdTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobIdTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobName: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetBlobNameTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobNameTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobSize: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetBlobSizeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobSizeTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobScore: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetBlobScoreTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobScoreTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobBuffers: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetBlobBuffersTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobBuffersTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kPutBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<PutBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<PutBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<GetBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTruncateBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<TruncateBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TruncateBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestroyBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<DestroyBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestroyBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<TagBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kBlobHasTag: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<BlobHasTagTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<BlobHasTagTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kReorganizeBlob: {
      task_ptr.ptr_ = CHI_CLIENT->NewEmptyTask<ReorganizeBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<ReorganizeBlobTask*>(task_ptr.ptr_);
      break;
    }
  }
  return task_ptr;
}
/** Serialize a task when returning from remote queue */
void SaveEnd(u32 method, BinaryOutputArchive<false> &ar, Task *task) override {
  switch (method) {
    case Method::kCreate: {
      ar << *reinterpret_cast<CreateTask*>(task);
      break;
    }
    case Method::kDestroy: {
      ar << *reinterpret_cast<DestroyTask*>(task);
      break;
    }
    case Method::kGetOrCreateTag: {
      ar << *reinterpret_cast<GetOrCreateTagTask*>(task);
      break;
    }
    case Method::kGetTagId: {
      ar << *reinterpret_cast<GetTagIdTask*>(task);
      break;
    }
    case Method::kGetTagName: {
      ar << *reinterpret_cast<GetTagNameTask*>(task);
      break;
    }
    case Method::kDestroyTag: {
      ar << *reinterpret_cast<DestroyTagTask*>(task);
      break;
    }
    case Method::kTagAddBlob: {
      ar << *reinterpret_cast<TagAddBlobTask*>(task);
      break;
    }
    case Method::kTagRemoveBlob: {
      ar << *reinterpret_cast<TagRemoveBlobTask*>(task);
      break;
    }
    case Method::kTagClearBlobs: {
      ar << *reinterpret_cast<TagClearBlobsTask*>(task);
      break;
    }
    case Method::kTagGetSize: {
      ar << *reinterpret_cast<TagGetSizeTask*>(task);
      break;
    }
    case Method::kTagUpdateSize: {
      ar << *reinterpret_cast<TagUpdateSizeTask*>(task);
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      ar << *reinterpret_cast<TagGetContainedBlobIdsTask*>(task);
      break;
    }
    case Method::kGetOrCreateBlob: {
      ar << *reinterpret_cast<GetOrCreateBlobTask*>(task);
      break;
    }
    case Method::kGetBlobId: {
      ar << *reinterpret_cast<GetBlobIdTask*>(task);
      break;
    }
    case Method::kGetBlobName: {
      ar << *reinterpret_cast<GetBlobNameTask*>(task);
      break;
    }
    case Method::kGetBlobSize: {
      ar << *reinterpret_cast<GetBlobSizeTask*>(task);
      break;
    }
    case Method::kGetBlobScore: {
      ar << *reinterpret_cast<GetBlobScoreTask*>(task);
      break;
    }
    case Method::kGetBlobBuffers: {
      ar << *reinterpret_cast<GetBlobBuffersTask*>(task);
      break;
    }
    case Method::kPutBlob: {
      ar << *reinterpret_cast<PutBlobTask*>(task);
      break;
    }
    case Method::kGetBlob: {
      ar << *reinterpret_cast<GetBlobTask*>(task);
      break;
    }
    case Method::kTruncateBlob: {
      ar << *reinterpret_cast<TruncateBlobTask*>(task);
      break;
    }
    case Method::kDestroyBlob: {
      ar << *reinterpret_cast<DestroyBlobTask*>(task);
      break;
    }
    case Method::kTagBlob: {
      ar << *reinterpret_cast<TagBlobTask*>(task);
      break;
    }
    case Method::kBlobHasTag: {
      ar << *reinterpret_cast<BlobHasTagTask*>(task);
      break;
    }
    case Method::kReorganizeBlob: {
      ar << *reinterpret_cast<ReorganizeBlobTask*>(task);
      break;
    }
  }
}
/** Deserialize a task when popping from remote queue */
void LoadEnd(u32 method, BinaryInputArchive<false> &ar, Task *task) override {
  switch (method) {
    case Method::kCreate: {
      ar >> *reinterpret_cast<CreateTask*>(task);
      break;
    }
    case Method::kDestroy: {
      ar >> *reinterpret_cast<DestroyTask*>(task);
      break;
    }
    case Method::kGetOrCreateTag: {
      ar >> *reinterpret_cast<GetOrCreateTagTask*>(task);
      break;
    }
    case Method::kGetTagId: {
      ar >> *reinterpret_cast<GetTagIdTask*>(task);
      break;
    }
    case Method::kGetTagName: {
      ar >> *reinterpret_cast<GetTagNameTask*>(task);
      break;
    }
    case Method::kDestroyTag: {
      ar >> *reinterpret_cast<DestroyTagTask*>(task);
      break;
    }
    case Method::kTagAddBlob: {
      ar >> *reinterpret_cast<TagAddBlobTask*>(task);
      break;
    }
    case Method::kTagRemoveBlob: {
      ar >> *reinterpret_cast<TagRemoveBlobTask*>(task);
      break;
    }
    case Method::kTagClearBlobs: {
      ar >> *reinterpret_cast<TagClearBlobsTask*>(task);
      break;
    }
    case Method::kTagGetSize: {
      ar >> *reinterpret_cast<TagGetSizeTask*>(task);
      break;
    }
    case Method::kTagUpdateSize: {
      ar >> *reinterpret_cast<TagUpdateSizeTask*>(task);
      break;
    }
    case Method::kTagGetContainedBlobIds: {
      ar >> *reinterpret_cast<TagGetContainedBlobIdsTask*>(task);
      break;
    }
    case Method::kGetOrCreateBlob: {
      ar >> *reinterpret_cast<GetOrCreateBlobTask*>(task);
      break;
    }
    case Method::kGetBlobId: {
      ar >> *reinterpret_cast<GetBlobIdTask*>(task);
      break;
    }
    case Method::kGetBlobName: {
      ar >> *reinterpret_cast<GetBlobNameTask*>(task);
      break;
    }
    case Method::kGetBlobSize: {
      ar >> *reinterpret_cast<GetBlobSizeTask*>(task);
      break;
    }
    case Method::kGetBlobScore: {
      ar >> *reinterpret_cast<GetBlobScoreTask*>(task);
      break;
    }
    case Method::kGetBlobBuffers: {
      ar >> *reinterpret_cast<GetBlobBuffersTask*>(task);
      break;
    }
    case Method::kPutBlob: {
      ar >> *reinterpret_cast<PutBlobTask*>(task);
      break;
    }
    case Method::kGetBlob: {
      ar >> *reinterpret_cast<GetBlobTask*>(task);
      break;
    }
    case Method::kTruncateBlob: {
      ar >> *reinterpret_cast<TruncateBlobTask*>(task);
      break;
    }
    case Method::kDestroyBlob: {
      ar >> *reinterpret_cast<DestroyBlobTask*>(task);
      break;
    }
    case Method::kTagBlob: {
      ar >> *reinterpret_cast<TagBlobTask*>(task);
      break;
    }
    case Method::kBlobHasTag: {
      ar >> *reinterpret_cast<BlobHasTagTask*>(task);
      break;
    }
    case Method::kReorganizeBlob: {
      ar >> *reinterpret_cast<ReorganizeBlobTask*>(task);
      break;
    }
  }
}

#endif  // CHI_HERMES_CORE_METHODS_H_