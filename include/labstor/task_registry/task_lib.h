//
// Created by lukemartinlogan on 6/23/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_TASK_TASK_H_
#define LABSTOR_INCLUDE_LABSTOR_TASK_TASK_H_

#include <dlfcn.h>
#include "labstor/labstor_types.h"
#include "labstor/queue_manager/queue_factory.h"
#include "labstor/network/serialize.h"
#include "task.h"

namespace labstor {

typedef LPointer<Task> TaskPointer;

/**
 * Represents a custom operation to perform.
 * Tasks are independent of Hermes.
 * */
class TaskLib {
 public:
  TaskStateId id_;    /**< The unique name of a task state */
  QueueId queue_id_;  /**< The queue id of a task state */
  std::string name_; /**< The unique semantic name of a task state */

  /** Default constructor */
  TaskLib() : id_(TaskStateId::GetNull()) {}

  /** Emplace Constructor */
  void Init(const TaskStateId &id, const std::string &name) {
    id_ = id;
    queue_id_ = QueueId(id);
    name_ = name;
  }

  /** Virtual destructor */
  virtual ~TaskLib() = default;

  /** Run a method of the task */
  virtual void Run(u32 method, Task *task, RunContext &rctx) = 0;

  /** Delete a task */
  virtual void Del(u32 method, Task *task) = 0;

  /** Duplicate a task */
  virtual void Dup(u32 method, Task *orig_task, std::vector<LPointer<Task>> &dups) = 0;

  /** Register end of duplicate */
  virtual void DupEnd(u32 method, u32 replica, Task *orig_task, Task *dup_task) = 0;

  /** Allow task to store replicas of completion */
  virtual void ReplicateStart(u32 method, u32 count, Task *task) = 0;

  /** Can be used to summarize the completions */
  virtual void ReplicateEnd(u32 method, Task *task) = 0;

  /** Serialize a task when initially pushing into remote */
  virtual std::vector<DataTransfer> SaveStart(u32 method, BinaryOutputArchive<true> &ar, Task *task) = 0;

  /** Deserialize a task when popping from remote queue */
  virtual TaskPointer LoadStart(u32 method, BinaryInputArchive<true> &ar) = 0;

  /** Serialize a task when returning from remote queue */
  virtual std::vector<DataTransfer> SaveEnd(u32 method, BinaryOutputArchive<false> &ar, Task *task) = 0;

  /** Deserialize a task when returning from remote queue */
  virtual void LoadEnd(u32 replica, u32 method, BinaryInputArchive<false> &ar, Task *task) = 0;

  /** Deserialize a task when returning from remote queue */
  virtual u32 GetGroup(u32 method, Task *task, hshm::charbuf &buf) = 0;
};

/** Represents a TaskLib in action */
typedef TaskLib TaskState;

/** Represents the TaskLib client-side */
class TaskLibClient {
 public:
  TaskStateId id_;
  QueueId queue_id_;

 public:
  /** Init from existing ID */
  void Init(const TaskStateId &id) {
    id_ = id;
    queue_id_ = QueueId(id_);
  }
};

extern "C" {
/** The two methods provided by all tasks */
typedef TaskState* (*create_state_t)(Task *task, const char *state_name);
/** Get the name of a task */
typedef const char* (*get_task_lib_name_t)(void);
}  // extern c

/** Used internally by task source file */
#define LABSTOR_TASK_CC(TRAIT_CLASS, TASK_NAME) \
    extern "C" {                              \
        void* create_state(labstor::Admin::CreateTaskStateTask *task, const char *state_name) { \
          labstor::TaskState *exec = reinterpret_cast<labstor::TaskState*>( \
            new TYPE_UNWRAP(TRAIT_CLASS)()); \
          exec->Init(task->id_, state_name); \
          RunContext rctx(0); \
          exec->Run(labstor::TaskMethod::kConstruct, task, rctx); \
          return exec; \
        } \
        const char* get_task_lib_name(void) { return TASK_NAME; } \
        bool is_labstor_task_ = true; \
    }
}   // namespace labstor

#endif  // LABSTOR_INCLUDE_LABSTOR_TASK_TASK_H_
