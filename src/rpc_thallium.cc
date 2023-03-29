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

#include <string>

#include "hermes.h"
#include "metadata_manager.h"
#include "rpc_thallium.h"
#include "hermes_shm/util/singleton.h"
#include "prefetcher.h"
#include <fstream>

namespace tl = thallium;

namespace hermes {

/** start Thallium RPC server */
void ThalliumRpc::InitServer() {
  LOG(INFO) << "Initializing RPC server" << std::endl;
  InitRpcContext();
  std::string addr = GetMyRpcAddress();
  LOG(INFO) << "Attempting to start server on: " << addr << std::endl;
  try {
    server_engine_ = std::make_unique<tl::engine>(
        addr, THALLIUM_SERVER_MODE, true, config_->rpc_.num_threads_);
  } catch (std::exception &e) {
    LOG(FATAL) << "RPC init failed for host: " << addr
               << std::endl << e.what() << std::endl;
  }
  std::string rpc_server_name = server_engine_->self();
  LOG(INFO) << hshm::Formatter::format(
                   "Serving {} (i.e., {}) with {} RPC threads as node id {}",
                   rpc_server_name, addr,
                   config_->rpc_.num_threads_,
                   node_id_) << std::endl;
  DefineRpcs();
}

/** initialize RPC clients */
void ThalliumRpc::InitClient() {
  InitRpcContext();
  std::string protocol = GetProtocol();
  client_engine_ = std::make_unique<tl::engine>(protocol,
                              THALLIUM_CLIENT_MODE,
                              true, 1);
  LOG(INFO) << hshm::Formatter::format(
                   "This client is on node {} (i.e., {})",
                   node_id_, GetHostNameFromNodeId(node_id_)) << std::endl;
}

/** run daemon */
void ThalliumRpc::RunDaemon() {
  server_engine_->enable_remote_shutdown();
  auto prefinalize_callback = [this]() {
    LOG(INFO) << "Beginning finalization on node: " <<
        this->node_id_ << std::endl;
    this->Finalize();
    LOG(INFO) << "Finished finalization callback on node: " <<
        this->node_id_ << std::endl;
  };

  // TODO(llogan): and config param to do this
  std::ofstream daemon_started_fs;
  daemon_started_fs.open("/tmp/hermes_daemon_log.txt");
  daemon_started_fs << HERMES_SYSTEM_INFO->pid_;
  daemon_started_fs.close();
  LOG(INFO) << "Running the daemon on node " << node_id_ << std::endl;

  server_engine_->push_prefinalize_callback(prefinalize_callback);
  server_engine_->wait_for_finalize();
  LOG(INFO) << "Daemon has stopped on node: " <<
      this->node_id_ << std::endl;
}

/** stop daemon (from client) */
void ThalliumRpc::StopDaemon() {
  try {
    for (i32 node_id = 1; node_id < (int)hosts_.size() + 1; ++node_id) {
      LOG(INFO) << "Sending stop signal to: " << node_id << std::endl;
      std::string server_name = GetServerName(node_id);
      tl::endpoint server = client_engine_->lookup(server_name.c_str());
      client_engine_->shutdown_remote_engine(server);
    }
  } catch (std::exception &e) {
    LOG(FATAL) << e.what() << std::endl;
  }
}

/** get server name */
std::string ThalliumRpc::GetServerName(i32 node_id) {
  std::string ip_address = GetIpAddressFromNodeId(node_id);
  return config_->rpc_.protocol_ + "://" +
         std::string(ip_address) +
         ":" + std::to_string(config_->rpc_.port_);
}

/** finalize RPC context */
void ThalliumRpc::Finalize() {
  switch (mode_) {
    case HermesType::kServer: {
      LOG(INFO) << "Stopping (server mode)" << std::endl;
      this->kill_requested_.store(true);
      HERMES->prefetch_.Finalize();
      try {
        // NOTE(llogan): Don't use finalize with unique_ptr. finalize() is
        // called in the destructor of the tl::enigne, and will segfault if
        // called twice.
        // server_engine_->finalize();
        server_engine_.release();
        client_engine_.release();
      } catch (std::exception &e) {
        LOG(INFO) << "Ignoring margo finalization error: "
                  << e.what() << std::endl;
      }
      break;
    }
    case HermesType::kClient: {
      LOG(INFO) << "Stopping (client mode)" << std::endl;
      client_engine_.release();
      break;
    }
    default: {
      throw std::logic_error("Invalid Hermes initialization type");
    }
  }
}

}  // namespace hermes
