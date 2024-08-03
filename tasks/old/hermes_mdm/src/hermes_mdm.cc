//
// Created by lukemartinlogan on 6/29/23.
//

#include "chimaera_admin/chimaera_admin.h"
#include "chimaera/api/chimaera_runtime.h"
#include "hermes/config_server.h"
#include "hermes_core/hermes_core.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"
#include "hermes_bucket_mdm/hermes_bucket_mdm.h"
#include "hermes/dpe/dpe_factory.h"
#include "bdev/bdev.h"

namespace hermes::mdm {

class Server : public Module {
 public:
  /**====================================
   * Configuration
   * ===================================*/
  u32 node_id_;
  blob_mdm::Client blob_mdm_;
  bucket_mdm::Client bkt_mdm_;

 public:
  Server() = default;

  /** Construct hermes MDM */
  void Construct(ConstructTask *task, RunContext &rctx) {
    HILOG(kDebug, "ConstructTaskPhase::kLoadConfig")
    std::string config_path = task->server_config_path_->str();
    HERMES_CONF->LoadServerConfig(config_path);
    node_id_ = CHI_CLIENT->node_id_;
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destory hermes MDM */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

 public:
#include "hermes_core/hermes_core_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hermes::mdm::Server, "hermes_core");
