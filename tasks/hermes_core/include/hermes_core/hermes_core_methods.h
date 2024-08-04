#ifndef CHI_HERMES_CORE_METHODS_H_
#define CHI_HERMES_CORE_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kGetOrCreateTag = 10;
  TASK_METHOD_T kGetTagId = 11;
  TASK_METHOD_T kGetTagName = 12;
  TASK_METHOD_T kDestroyTag = 14;
  TASK_METHOD_T kTagAddBlob = 15;
  TASK_METHOD_T kTagRemoveBlob = 16;
  TASK_METHOD_T kTagClearBlobs = 17;
  TASK_METHOD_T kTagGetSize = 18;
  TASK_METHOD_T kTagUpdateSize = 19;
  TASK_METHOD_T kTagGetContainedBlobIds = 20;
  TASK_METHOD_T kGetOrCreateBlobId = 30;
  TASK_METHOD_T kGetBlobId = 31;
  TASK_METHOD_T kGetBlobName = 32;
  TASK_METHOD_T kGetBlobSize = 34;
  TASK_METHOD_T kGetBlobScore = 35;
  TASK_METHOD_T kGetBlobBuffers = 36;
  TASK_METHOD_T kPutBlob = 37;
  TASK_METHOD_T kGetBlob = 38;
  TASK_METHOD_T kTruncateBlob = 39;
  TASK_METHOD_T kDestroyBlob = 40;
  TASK_METHOD_T kTagBlob = 41;
  TASK_METHOD_T kBlobHasTag = 42;
  TASK_METHOD_T kReorganizeBlob = 43;
  TASK_METHOD_T kCount = 44;
};

#endif  // CHI_HERMES_CORE_METHODS_H_