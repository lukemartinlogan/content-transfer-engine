//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_hermes_core_H_
#define HRUN_hermes_core_H_

#include "hermes_core_tasks.h"

namespace hermes::mdm {

/** Create requests */
class Client : public ModuleClient {
 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a hermes_core */
  HSHM_ALWAYS_INLINE
  void Create(const DomainQuery &dom_query,
                  const std::string &state_name) {
    id_ = PoolId::GetNull();
    std::vector<PriorityInfo> queue_info;
    id_ = CHI_ADMIN->CreateTaskState<ConstructTask>(
        domain_id, state_name, id_, queue_info);
    Init(id_, CHI_ADMIN->queue_id_);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void Destroy(const DomainQuery &dom_query) {
    CHI_ADMIN->DestroyTaskState(domain_id, id_);
  }
};

}  // namespace hrun

#endif  // HRUN_hermes_core_H_
