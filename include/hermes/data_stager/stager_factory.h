//
// Created by lukemartinlogan on 9/30/23.
//

#ifndef HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_
#define HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_

#include "abstract_stager.h"
#include "binary_stager.h"

namespace hermes {

class StagerFactory {
 public:
  static std::unique_ptr<AbstractStager> Get(const std::string &path,
                                             const std::string &params) {
    std::string protocol;
    chi::LocalDeserialize srl(params);
    srl >> protocol;

    std::unique_ptr<AbstractStager> stager;
    if (protocol == "file") {
      stager = std::make_unique<BinaryFileStager>();
    } else if (protocol == "parquet") {
    } else if (protocol == "hdf5") {
    } else {
      throw std::runtime_error("Unknown stager type");
    }
    stager->path_ = path;
    stager->params_ = params;
    return stager;
  }
};

}  // namespace hermes

#endif  // HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_
